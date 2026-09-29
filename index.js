const express = require('express');
const crypto = require('node:crypto');
const amqp = require('amqplib');

const app = express();
const port = Number(process.env.PORT) || 3003;
const rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const payments = new Map();

app.use(express.json());

async function publishPayment(payment) {
  try {
    const connection = await amqp.connect(rabbitmqUrl);
    const channel = await connection.createChannel();
    await channel.assertExchange('events', 'topic', { durable: true });
    channel.publish('events', 'payment.success', Buffer.from(JSON.stringify(payment)), { persistent: true });
    setTimeout(() => connection.close(), 100);
  } catch (error) {
    console.warn(`RabbitMQ unavailable: ${error.message}`);
  }
}

app.get('/health', (request, response) => {
  response.json({ status: 'ok', service: 'payment-service' });
});

app.get('/payments', (request, response) => {
  response.json(Array.from(payments.values()));
});

app.post('/payments', (request, response) => {
  const { orderId, amount, method = 'card' } = request.body;
  if (!orderId || typeof amount !== 'number' || amount <= 0) {
    return response.status(400).json({ error: 'orderId and a positive numeric amount are required' });
  }

  const payment = {
    id: crypto.randomUUID(),
    orderId: String(orderId),
    amount,
    method: String(method),
    status: 'success',
    createdAt: new Date().toISOString(),
  };
  payments.set(payment.id, payment);
  publishPayment(payment);
  return response.status(201).json(payment);
});

app.listen(port, () => {
  console.log(`Payment service listening on port ${port}`);
});
