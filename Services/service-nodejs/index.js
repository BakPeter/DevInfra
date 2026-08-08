const express = require('express');
const { Kafka } = require('kafkajs');
const winston = require('winston');
const { SeqTransport } = require('@datalust/winston-seq');

const seqServerUrl = process.env.SEQ_SERVER_URL || 'http://localhost:5341';

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(winston.format.errors({ stack: true }), winston.format.json()),
  defaultMeta: { service: 'service-nodejs' },
  transports: [
    new winston.transports.Console({
      format: winston.format.simple(),
    }),
    new SeqTransport({
      serverUrl: seqServerUrl,
      //   apiKey: seqApiKey,
      onError: (error) => {
        console.error('Seq logging failed', error);
      },
      handleExceptions: true,
      handleRejections: true,
    }),
  ],
});

const app = express();
const port = Number(process.env.NODEJS_SERVICE_REST_PORT || process.env.PORT) || 3000;
const kafkaBrokers = (process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);
const kafkaTopic = process.env.KAFKA_TOPIC_ITEMS_NODEJS_SERVICE || 'items.events';
const kafkaConsumerTopics = process.env.NODEJS_SERVICE_KAFKA_CONSUMER_TOPICS.split(',')
  .map((topic) => topic.trim())
  .filter(Boolean);
logger.info('Kafka Topic {topic}', {
  topic: kafkaTopic,
  KAFKA_TOPIC_ITEMS_NODEJS_SERVICE: process.env.KAFKA_TOPIC_ITEMS_NODEJS_SERVICE,
  KAFKA_TOPIC_ITEMS: process.env.KAFKA_TOPIC_ITEMS,
  NODEJS_SERVICE_KAFKA_CONSUMER_TOPICS: kafkaConsumerTopics,
  action: 'initialized',
});
// const seqApiKey = process.env.SEQ_API_KEY;

const kafka = new Kafka({
  clientId: 'service-nodejs',
  brokers: kafkaBrokers,
});
const producer = kafka.producer();
const consumer = kafka.consumer({
  groupId: process.env.KAFKA_CONSUMER_GROUP_ID || 'service-nodejs-consumer',
});
let producerConnectPromise;
let consumerConnectPromise;

function getProducer() {
  if (!producerConnectPromise) {
    producerConnectPromise = producer.connect().then(() => producer);
  }

  return producerConnectPromise;
}

async function startConsumer() {
  if (kafkaConsumerTopics.length === 0) {
    logger.info('Kafka consumer disabled because no topics were configured');
    return null;
  }

  if (!consumerConnectPromise) {
    consumerConnectPromise = (async () => {
      await consumer.connect();

      for (const topic of kafkaConsumerTopics) {
        await consumer.subscribe({ topic, fromBeginning: false });
      }

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          logger.info('Consumed Kafka event from {topic}', {
            topic,
            partition,
            key: message.key?.toString(),
            value: message.value?.toString(),
            headers: Object.fromEntries(
              Object.entries(message.headers || {}).map(([header, value]) => [
                header,
                value?.toString(),
              ]),
            ),
          });
        },
      });

      logger.info('Kafka consumer subscribed to topics', {
        topics: kafkaConsumerTopics,
        groupId: process.env.KAFKA_CONSUMER_GROUP_ID || 'service-nodejs-consumer',
      });

      return consumer;
    })().catch((error) => {
      consumerConnectPromise = undefined;
      logger.error('Failed to start Kafka consumer', {
        topics: kafkaConsumerTopics,
        groupId: process.env.KAFKA_CONSUMER_GROUP_ID || 'service-nodejs-consumer',
        error,
      });
      throw error;
    });
  }

  return consumerConnectPromise;
}

async function publishItemEvent(action, item) {
  try {
    const connectedProducer = await getProducer();
    await connectedProducer.send({
      topic: kafkaTopic,
      messages: [
        {
          key: String(item.id),
          value: JSON.stringify({
            action,
            item,
            occurredAt: new Date().toISOString(),
          }),
        },
      ],
    });

    logger.info('Published Kafka event {action} for item {itemId}', {
      action,
      itemId: item.id,
      topic: kafkaTopic,
    });
  } catch (error) {
    logger.error('Failed to publish Kafka event {action} for item {itemId}', {
      action,
      itemId: item.id,
      topic: kafkaTopic,
      error,
    });
  }
}

app.use(express.json());
app.use((request, response, next) => {
  const startedAt = Date.now();

  response.on('finish', () => {
    logger.info('HTTP {method} {path} responded {statusCode} in {durationMs} ms', {
      method: request.method,
      path: request.originalUrl,
      statusCode: response.statusCode,
      durationMs: Date.now() - startedAt,
      ip: request.ip,
      userAgent: request.get('user-agent'),
    });
  });

  next();
});

let nextId = 3;
const items = [
  { id: 1, name: 'alpha' },
  { id: 2, name: 'beta' },
  { id: 3, name: 'gamma' },
];

app.get('/health', (_request, response) => {
  response.json({ status: 'ok' });
});

app.get('/api/items', async (_request, response) => {
  for (const item of items) {
    await publishItemEvent('fetched', item);
  }

  response.json(items);
});

app.get('/api/items/:id', (request, response) => {
  const itemId = Number(request.params.id);
  const item = items.find((entry) => entry.id === itemId);

  if (!item) {
    response.status(404).json({ message: 'Item not found' });
    return;
  }

  response.json(item);
});

app.post('/api/items', async (request, response) => {
  const { name } = request.body;

  if (typeof name !== 'string' || name.trim() === '') {
    response.status(400).json({ message: 'name is required' });
    return;
  }

  const item = {
    id: nextId,
    name: name.trim(),
  };

  nextId += 1;
  items.push(item);

  await publishItemEvent('created', item);

  response.status(201).json(item);
});

app.put('/api/items/:id', async (request, response) => {
  const itemId = Number(request.params.id);
  const item = items.find((entry) => entry.id === itemId);
  const { name } = request.body;

  if (!item) {
    response.status(404).json({ message: 'Item not found' });
    return;
  }

  if (typeof name !== 'string' || name.trim() === '') {
    response.status(400).json({ message: 'name is required' });
    return;
  }

  item.name = name.trim();
  await publishItemEvent('updated', item);
  response.json(item);
});

app.delete('/api/items/:id', async (request, response) => {
  const itemId = Number(request.params.id);
  const itemIndex = items.findIndex((entry) => entry.id === itemId);

  if (itemIndex === -1) {
    response.status(404).json({ message: 'Item not found' });
    return;
  }

  const [deletedItem] = items.splice(itemIndex, 1);
  await publishItemEvent('deleted', deletedItem);

  response.status(204).send();
});

if (require.main === module) {
  app.listen(port, () => {
    logger.info('REST API listening on port {port}', { port });
  });

  startConsumer().catch(() => {
    process.exitCode = 1;
  });
}

async function shutdown() {
  try {
    if (consumerConnectPromise) {
      await consumer.disconnect();
    }
  } catch (error) {
    logger.error('Failed to disconnect Kafka consumer', { error });
  }

  try {
    if (producerConnectPromise) {
      await producer.disconnect();
    }
  } catch (error) {
    logger.error('Failed to disconnect Kafka producer', { error });
  }
}

process.on('SIGINT', () => {
  shutdown().finally(() => process.exit(0));
});

process.on('SIGTERM', () => {
  shutdown().finally(() => process.exit(0));
});

module.exports = app;
