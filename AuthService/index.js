const express = require('express');
const kafka = require('kafka-node');
const { v4: uuidv4 } = require('uuid');

const app = express();
const port = 3000;

app.use(express.json());

const client = new kafka.KafkaClient({ kafkaHost: 'kafka:9092' });
const producer = new kafka.Producer(client);

producer.on('ready', () => {
  console.log('Kafka Producer is connected and ready.');
});

producer.on('error', (error) => {
  console.error('Error in Kafka Producer:', error);
});

app.post('/send-otp', (req, res) => {
  const { email } = req.body;
  const message = JSON.stringify({
    email,
    requestId: uuidv4(),
    serviceIdentifier: 'AuthService'
  });

  const payloads = [
    { topic: 'send_otp', messages: message }
  ];

  producer.send(payloads, (error, data) => {
    if (error) {
      return res.status(500).send({ message: 'Failed to send OTP request' });
    }
    res.send({ message: 'OTP request sent successfully' });
  });
});

app.listen(port, () => {
  console.log(`AuthService listening at http://localhost:${port}`);
});
