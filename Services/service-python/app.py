import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import cast

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

        self.items = [
            {
                "id": 1,
                "name": "alpha",
            },
            {
                "id": 2,
                "name": "beta",
            },
            {
                "id": 3,
                "name": "gamma",
            },
        ]

        self.next_id = 4

        self.producer: AIOKafkaProducer | None = None

        self.producer_lock = asyncio.Lock()

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

    async def shutdown(
        self,
    ) -> None:
        if self.producer is not None:
            await self.producer.stop()

            self.producer = None


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


@asynccontextmanager
async def lifespan(
    _app: FastAPI,
):
    state.logger.info(
        "Service started",
        service=SERVICE_NAME,
        port=port,
        kafkaTopic=kafka_topic,
        kafkaBrokers=kafka_brokers,
        seqServerUrl=seq_server_url,
        action="service_started",
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
    state.logger.info(
        "Listing items",
        service=SERVICE_NAME,
        path="/api/items",
        action="list_items",
        itemCount=len(state.items),
    )

    for item in state.items:
        await publish_item_event(
            "fetched",
            item,
        )

    return state.items


@app.get(
    "/api/items/{item_id}"
)
async def get_item(
    item_id: int,
):
    item = next(
        (
            entry
            for entry in state.items
            if entry["id"] == item_id
        ),
        None,
    )

    if item is None:
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

    return item


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

    item = {
        "id": state.next_id,
        "name": name,
    }

    state.next_id += 1

    state.items.append(
        item
    )

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
    item = next(
        (
            entry
            for entry in state.items
            if entry["id"] == item_id
        ),
        None,
    )

    if item is None:
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

    item["name"] = name

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
    item_index = next(
        (
            index
            for index, entry
            in enumerate(state.items)
            if entry["id"] == item_id
        ),
        None,
    )

    if item_index is None:
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

    deleted_item = state.items.pop(
        item_index
    )

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