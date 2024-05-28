// order-service/index.js

const express = require('express');
const bodyParser = require('body-parser');
const { Kafka } = require('kafkajs');

const app = express();
app.use(bodyParser.json());

const kafka = new Kafka({
  clientId: 'order-service',
  brokers: ['kafka:9092'],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'order-group' });

// In-memory data structures to track message states
const messageStates = {
  'user-topic': { queued: 0, inProcess: 0, failed: 0, success: 0 },
  'order-topic': { queued: 0, inProcess: 0, failed: 0, success: 0 },
  'product-topic': { queued: 0, inProcess: 0, failed: 0, success: 0 },
};

const messageRetries = {};

const incrementCounter = (topic, state) => {
  messageStates[topic][state]++;
};

const getCounter = (topic, state) => {
  return messageStates[topic][state];
};

app.post('/users', async (req, res) => {
  const { username, email } = req.body;
  await producer.send({
    topic: 'user-topic',
    messages: [{ value: JSON.stringify({ username, email }) }],
  });
  incrementCounter('user-topic', 'queued');
  res.send('User data sent to Kafka');
});

app.post('/orders', async (req, res) => {
  const { product, quantity } = req.body;
  await producer.send({
    topic: 'order-topic',
    messages: [{ value: JSON.stringify({ product, quantity }) }],
  });
  incrementCounter('order-topic', 'queued');
  res.send('Order data sent to Kafka');
});

app.post('/products', async (req, res) => {
  const { name, price } = req.body;
  await producer.send({
    topic: 'product-topic',
    messages: [{ value: JSON.stringify({ name, price }) }],
  });
  incrementCounter('product-topic', 'queued');
  res.send('Product data sent to Kafka');
});

app.get('/metrics', (req, res) => {
  res.json(messageStates);
});

const processMessage = async (topic, message) => {
  const messageKey = `${topic}:${message.offset}`;
  const retries = messageRetries[messageKey] || 0;

  try {
    incrementCounter(topic, 'inProcess');
    messageStates[topic].queued--;
    console.log(`Consumed from ${topic}:`, message.value.toString());
    incrementCounter(topic, 'success');
    messageStates[topic].inProcess--;
  } catch (error) {
    if (retries < 3) {
      messageRetries[messageKey] = retries + 1;
      console.error(`Error processing message from ${topic}:`, error);
    } else {
      incrementCounter(topic, 'failed');
      messageStates[topic].inProcess--;
      console.error(`Message from ${topic} failed after ${retries} retries:`, error);
    }
  }
};

const run = async () => {
  await producer.connect();
  await consumer.connect();

  const topics = ['user-topic', 'order-topic', 'product-topic'];
  await Promise.all(topics.map(topic => consumer.subscribe({ topic, fromBeginning: true })));

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await processMessage(topic, message);
    },
  });

  app.listen(3001, () => {
    console.log('Order service running on port 3001');
  });
};

run().catch(console.error);
