import asyncio
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import cast

import asyncpg
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from seqlog import log_to_seq
from seqlog.structured_logging import StructuredLogger


SERVICE_NAME = "service-python"

seq_server_url = os.getenv(
    "SEQ_SERVER_URL",
    "http://localhost:5341",
)

port = int(
    os.getenv("PYTHON_SERVICE_REST_PORT")
    or os.getenv("PORT")
    or "3002"
)

kafka_brokers = [
    broker.strip()
    for broker in os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    ).split(",")
    if broker.strip()
]

kafka_topic = os.getenv(
    "KAFKA_TOPIC_ITEMS_PYTHON_SERVICE",
    "items.events.python",
)

kafka_state_topic = os.getenv(
    "KAFKA_TOPIC_STATE_PYTHON_SERVICE",
    "service-python.state",
)

state_interval_seconds = int(
    os.getenv(
        "PYTHON_SERVICE_STATE_INTERVAL_SECONDS"
    )
    or "30"
)

STATE_PROBE_TIMEOUT_SECONDS = 3

STATE_SINK_TIMEOUT_SECONDS = 5

postgres_host = os.getenv(
    "POSTGRES_HOST",
    "localhost",
)

postgres_port = int(
    os.getenv("POSTGRES_PORT")
    or "5432"
)

postgres_user = os.getenv(
    "POSTGRES_USER",
    "postgres",
)

postgres_password = os.getenv(
    "POSTGRES_PASSWORD",
    "postgres",
)

postgres_db = os.getenv(
    "POSTGRES_DB",
    "devinfra",
)

postgres_schema = os.getenv(
    "POSTGRES_SCHEMA",
    "python",
)

# A schema name cannot be a bound parameter, so it is
# interpolated and must be validated.
if not re.fullmatch(
    r"[a-z_][a-z0-9_]*",
    postgres_schema,
):
    raise ValueError(
        f"Invalid POSTGRES_SCHEMA: {postgres_schema}"
    )

items_table = f"{postgres_schema}.items"

service_state_table = f"{postgres_schema}.service_state"


def configure_logger() -> StructuredLogger:
    logging.setLoggerClass(StructuredLogger)

    logger_instance = logging.getLogger(SERVICE_NAME)
    logger_instance = cast(StructuredLogger, logger_instance)

    logger_instance.setLevel(logging.INFO)

    # Remove handlers in case logger is configured more than once.
    logger_instance.handlers.clear()

    # Prevent propagation to the root logger.
    logger_instance.propagate = False

    # Console logging
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    console_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s"
        )
    )

    logger_instance.addHandler(console_handler)

    # Seq logging
    try:
        seq_handler = log_to_seq(
            server_url=seq_server_url,
            level=logging.INFO,
            batch_size=1,
            auto_flush_timeout=1,
            override_root_logger=False,
            support_extra_properties=True,
            use_clef=True,
        )

        logger_instance.addHandler(seq_handler)

    except Exception:
        logger_instance.exception(
            "Seq logging setup failed",
            extra={
                "service": SERVICE_NAME,
                "seqServerUrl": seq_server_url,
            },
        )

    return logger_instance


logger: StructuredLogger = configure_logger()


logger.info(
    f"Kafka topic initialized {kafka_topic}",
    service=SERVICE_NAME,
    topic=kafka_topic,
    kafka_topic_items_python_service=os.getenv(
        "KAFKA_TOPIC_ITEMS_PYTHON_SERVICE"
    ),
    kafka_topic_items=os.getenv(
        "KAFKA_TOPIC_ITEMS"
    ),
    action="initialized",
)


class AppState:
    def __init__(
        self,
        app_logger: StructuredLogger,
    ) -> None:
        self.logger = app_logger

        self.producer: AIOKafkaProducer | None = None

        self.producer_lock = asyncio.Lock()

        self.pool: asyncpg.Pool | None = None

        self.pool_lock = asyncio.Lock()

        self.state_task: asyncio.Task | None = None

    async def get_producer(
        self,
    ) -> AIOKafkaProducer:
        if self.producer is not None:
            return self.producer

        async with self.producer_lock:
            if self.producer is None:
                producer = AIOKafkaProducer(
                    bootstrap_servers=kafka_brokers,
                    client_id=SERVICE_NAME,
                )

                await producer.start()

                self.producer = producer

        return self.producer

    async def get_pool(
        self,
    ) -> asyncpg.Pool:
        if self.pool is not None:
            return self.pool

        async with self.pool_lock:
            if self.pool is None:
                self.pool = await asyncpg.create_pool(
                    host=postgres_host,
                    port=postgres_port,
                    user=postgres_user,
                    password=postgres_password,
                    database=postgres_db,
                )

        return self.pool

    async def init_db(
        self,
    ) -> None:
        pool = await self.get_pool()

        async with pool.acquire() as connection:
            await connection.execute(
                f"CREATE SCHEMA IF NOT EXISTS {postgres_schema}"
            )

            await connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {items_table} (
                    id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    name text NOT NULL
                )
                """
            )

            await connection.execute(
                f"""
                INSERT INTO {items_table} (name)
                SELECT unnest(ARRAY['alpha', 'beta', 'gamma'])
                WHERE NOT EXISTS (SELECT 1 FROM {items_table})
                """
            )

            await connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {service_state_table} (
                    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    published_at timestamptz NOT NULL,
                    origin_service text NOT NULL,
                    health_state text NOT NULL,
                    checks jsonb NOT NULL
                )
                """
            )

            await connection.execute(
                f"""
                CREATE INDEX IF NOT EXISTS service_state_published_at_idx
                ON {service_state_table} (published_at DESC)
                """
            )

        self.logger.info(
            "Postgres schema ready",
            service=SERVICE_NAME,
            schema=postgres_schema,
            database=postgres_db,
            action="db_initialized",
        )

    async def shutdown(
        self,
    ) -> None:
        if self.state_task is not None:
            self.state_task.cancel()

            try:
                await self.state_task

            except asyncio.CancelledError:
                pass

            self.state_task = None

        if self.producer is not None:
            await self.producer.stop()

            self.producer = None

        if self.pool is not None:
            await self.pool.close()

            self.pool = None


state = AppState(logger)


async def publish_item_event(
    action: str,
    item: dict,
) -> None:
    payload = {
        "action": action,
        "item": item,
        "occurredAt": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    try:
        producer = await state.get_producer()

        await producer.send_and_wait(
            kafka_topic,
            key=str(
                item["id"]
            ).encode("utf-8"),
            value=json.dumps(
                payload
            ).encode("utf-8"),
        )

        state.logger.info(
            "Published Kafka event",
            service=SERVICE_NAME,
            action=action,
            itemId=item["id"],
            topic=kafka_topic,
        )

    except Exception:
        state.logger.exception(
            "Failed to publish Kafka event",
            service=SERVICE_NAME,
            action=action,
            itemId=item["id"],
            topic=kafka_topic,
        )


async def probe_postgres() -> str:
    try:
        pool = await asyncio.wait_for(
            state.get_pool(),
            timeout=STATE_PROBE_TIMEOUT_SECONDS,
        )

        await asyncio.wait_for(
            pool.execute("SELECT 1"),
            timeout=STATE_PROBE_TIMEOUT_SECONDS,
        )

        return "Healthy"

    except Exception:
        state.logger.exception(
            "Postgres health probe failed",
            service=SERVICE_NAME,
        )

        return "Unhealthy"


# A real metadata round-trip: the cached producer reports a stopped broker as reachable.
async def probe_kafka() -> str:
    try:
        producer = await asyncio.wait_for(
            state.get_producer(),
            timeout=STATE_PROBE_TIMEOUT_SECONDS,
        )

        await asyncio.wait_for(
            producer.client.fetch_all_metadata(),
            timeout=STATE_PROBE_TIMEOUT_SECONDS,
        )

        return "Healthy"

    except Exception:
        state.logger.exception(
            "Kafka health probe failed",
            service=SERVICE_NAME,
        )

        return "Unhealthy"


async def write_state_to_kafka(
    service_state: dict,
) -> None:
    try:
        producer = await asyncio.wait_for(
            state.get_producer(),
            timeout=STATE_SINK_TIMEOUT_SECONDS,
        )

        await asyncio.wait_for(
            producer.send_and_wait(
                kafka_state_topic,
                key=b"ServiceState",
                value=json.dumps(
                    service_state
                ).encode("utf-8"),
            ),
            timeout=STATE_SINK_TIMEOUT_SECONDS,
        )

    except Exception:
        state.logger.exception(
            "Failed to publish service state to Kafka",
            service=SERVICE_NAME,
            topic=kafka_state_topic,
        )


async def write_state_to_seq(
    service_state: dict,
) -> None:
    try:
        state.logger.info(
            "Service state",
            service=SERVICE_NAME,
            publishedAt=service_state["publishedAt"],
            originService=service_state["originService"],
            healthState=service_state["healthState"],
            checks=service_state["checks"],
            topic=kafka_state_topic,
            action="service_state",
        )

    except Exception:
        print("Failed to log service state")


async def write_state_to_postgres(
    service_state: dict,
    published_at: datetime,
) -> None:
    try:
        pool = await asyncio.wait_for(
            state.get_pool(),
            timeout=STATE_SINK_TIMEOUT_SECONDS,
        )

        await asyncio.wait_for(
            pool.execute(
                f"""
                INSERT INTO {service_state_table}
                    (published_at, origin_service, health_state, checks)
                VALUES ($1, $2, $3, $4)
                """,
                published_at,
                service_state["originService"],
                service_state["healthState"],
                json.dumps(
                    service_state["checks"]
                ),
            ),
            timeout=STATE_SINK_TIMEOUT_SECONDS,
        )

    except Exception:
        state.logger.exception(
            "Failed to store service state in Postgres",
            service=SERVICE_NAME,
            table=service_state_table,
        )


# The three sinks run concurrently and each owns its own error handling, so a dead
# Kafka can neither fail nor delay the Seq and Postgres writes.
async def publish_service_state() -> None:
    postgres_check, kafka_check = await asyncio.gather(
        probe_postgres(),
        probe_kafka(),
    )

    checks = {
        "postgres": postgres_check,
        "kafka": kafka_check,
    }

    unhealthy = [
        result
        for result in checks.values()
        if result != "Healthy"
    ]

    if not unhealthy:
        health_state = "Healthy"

    elif len(unhealthy) == len(checks):
        health_state = "Unhealthy"

    else:
        health_state = "Degraded"

    published_at = datetime.now(
        timezone.utc
    )

    service_state = {
        "publishedAt": published_at.isoformat(),
        "originService": SERVICE_NAME,
        "healthState": health_state,
        "checks": checks,
    }

    await asyncio.gather(
        write_state_to_kafka(service_state),
        write_state_to_seq(service_state),
        write_state_to_postgres(
            service_state,
            published_at,
        ),
        return_exceptions=True,
    )


async def run_service_state_loop() -> None:
    while True:
        await asyncio.sleep(
            state_interval_seconds
        )

        try:
            await publish_service_state()

        except asyncio.CancelledError:
            raise

        except Exception:
            state.logger.exception(
                "Service state tick failed",
                service=SERVICE_NAME,
            )


@asynccontextmanager
async def lifespan(
    _app: FastAPI,
):
    await state.init_db()

    state.logger.info(
        "Service started",
        service=SERVICE_NAME,
        port=port,
        kafkaTopic=kafka_topic,
        kafkaBrokers=kafka_brokers,
        seqServerUrl=seq_server_url,
        postgresSchema=postgres_schema,
        action="service_started",
    )

    state.state_task = asyncio.create_task(
        run_service_state_loop()
    )

    state.logger.info(
        "Service state publisher started",
        service=SERVICE_NAME,
        intervalSeconds=state_interval_seconds,
        topic=kafka_state_topic,
    )

    try:
        yield

    finally:
        try:
            await state.shutdown()

            state.logger.info(
                "Service stopped",
                service=SERVICE_NAME,
                action="service_stopped",
            )

        except Exception:
            state.logger.exception(
                "Failed to disconnect Kafka producer",
                service=SERVICE_NAME,
            )


app = FastAPI(
    lifespan=lifespan,
)


async def get_validated_name(
    request: Request,
) -> str:
    try:
        payload = await request.json()

    except Exception as error:
        raise HTTPException(
            status_code=400,
            detail="name is required",
        ) from error

    name = (
        payload.get("name")
        if isinstance(payload, dict)
        else None
    )

    if (
        not isinstance(name, str)
        or not name.strip()
    ):
        raise HTTPException(
            status_code=400,
            detail="name is required",
        )

    return name.strip()


@app.middleware("http")
async def log_requests(
    request: Request,
    call_next,
):
    started_at = datetime.now(
        timezone.utc
    )

    try:
        response = await call_next(
            request
        )

        duration_ms = (
            datetime.now(timezone.utc)
            - started_at
        ).total_seconds() * 1000

        state.logger.info(
            "HTTP request completed",
            service=SERVICE_NAME,
            method=request.method,
            path=request.url.path,
            statusCode=response.status_code,
            durationMs=round(
                duration_ms,
                2,
            ),
            client=(
                request.client.host
                if request.client
                else None
            ),
            userAgent=request.headers.get(
                "user-agent"
            ),
        )

        return response

    except Exception:
        duration_ms = (
            datetime.now(timezone.utc)
            - started_at
        ).total_seconds() * 1000

        state.logger.exception(
            "HTTP request failed",
            service=SERVICE_NAME,
            method=request.method,
            path=request.url.path,
            durationMs=round(
                duration_ms,
                2,
            ),
        )

        raise


@app.exception_handler(
    HTTPException
)
async def http_exception_handler(
    _request: Request,
    exc: HTTPException,
):
    if exc.detail == "Item not found":
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "message": "Item not found"
            },
        )

    if exc.detail == "name is required":
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "message": "name is required"
            },
        )

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "message": exc.detail
        },
    )


@app.get("/health")
async def health():
    state.logger.info(
        "Health check requested",
        service=SERVICE_NAME,
        path="/health",
        action="health_check",
    )

    return {
        "status": "ok"
    }


@app.get("/api/items")
async def list_items():
    pool = await state.get_pool()

    rows = await pool.fetch(
        f"SELECT id, name FROM {items_table} ORDER BY id"
    )

    items = [
        dict(row)
        for row in rows
    ]

    state.logger.info(
        "Listing items",
        service=SERVICE_NAME,
        path="/api/items",
        action="list_items",
        itemCount=len(items),
    )

    for item in items:
        await publish_item_event(
            "fetched",
            item,
        )

    return items


@app.get(
    "/api/items/{item_id}"
)
async def get_item(
    item_id: int,
):
    pool = await state.get_pool()

    row = await pool.fetchrow(
        f"SELECT id, name FROM {items_table} WHERE id = $1",
        item_id,
    )

    if row is None:
        state.logger.warning(
            "Item lookup failed",
            service=SERVICE_NAME,
            path="/api/items/{item_id}",
            action="get_item",
            itemId=item_id,
        )

        raise HTTPException(
            status_code=404,
            detail="Item not found",
        )

    state.logger.info(
        "Item retrieved",
        service=SERVICE_NAME,
        path="/api/items/{item_id}",
        action="get_item",
        itemId=item_id,
    )

    return dict(row)


@app.post(
    "/api/items",
    status_code=201,
)
async def create_item(
    request: Request,
):
    name = await get_validated_name(
        request
    )

    pool = await state.get_pool()

    row = await pool.fetchrow(
        f"INSERT INTO {items_table} (name) VALUES ($1) RETURNING id, name",
        name,
    )

    item = dict(row)

    state.logger.info(
        "Item created",
        service=SERVICE_NAME,
        path="/api/items",
        action="create_item",
        itemId=item["id"],
        itemName=item["name"],
    )

    await publish_item_event(
        "created",
        item,
    )

    return item


@app.put(
    "/api/items/{item_id}"
)
async def update_item(
    item_id: int,
    request: Request,
):
    pool = await state.get_pool()

    existing = await pool.fetchrow(
        f"SELECT id FROM {items_table} WHERE id = $1",
        item_id,
    )

    if existing is None:
        state.logger.warning(
            "Item update failed",
            service=SERVICE_NAME,
            path="/api/items/{item_id}",
            action="update_item",
            itemId=item_id,
        )

        raise HTTPException(
            status_code=404,
            detail="Item not found",
        )

    name = await get_validated_name(
        request
    )

    row = await pool.fetchrow(
        f"UPDATE {items_table} SET name = $1 WHERE id = $2 RETURNING id, name",
        name,
        item_id,
    )

    item = dict(row)

    state.logger.info(
        "Item updated",
        service=SERVICE_NAME,
        path="/api/items/{item_id}",
        action="update_item",
        itemId=item_id,
        itemName=item["name"],
    )

    await publish_item_event(
        "updated",
        item,
    )

    return item


@app.delete(
    "/api/items/{item_id}",
    status_code=204,
)
async def delete_item(
    item_id: int,
):
    pool = await state.get_pool()

    row = await pool.fetchrow(
        f"DELETE FROM {items_table} WHERE id = $1 RETURNING id, name",
        item_id,
    )

    if row is None:
        state.logger.warning(
            "Item delete failed",
            service=SERVICE_NAME,
            path="/api/items/{item_id}",
            action="delete_item",
            itemId=item_id,
        )

        raise HTTPException(
            status_code=404,
            detail="Item not found",
        )

    deleted_item = dict(row)

    state.logger.info(
        "Item deleted",
        service=SERVICE_NAME,
        path="/api/items/{item_id}",
        action="delete_item",
        itemId=item_id,
        itemName=deleted_item["name"],
    )

    await publish_item_event(
        "deleted",
        deleted_item,
    )

    return Response(
        status_code=204
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,

        # IMPORTANT:
        # Prevent Uvicorn from replacing/closing
        # the Seq logging handler.
        log_config=None,
    )
