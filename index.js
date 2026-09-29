const express = require('express');
const crypto = require('node:crypto');
const amqp = require('amqplib');

const app = express();
const port = Number(process.env.PORT) || 3003;
const rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const payments = new Map();
let consumerConnection;

app.use(express.json());

async function consumeOrders() {
  try {
    consumerConnection = await amqp.connect(rabbitmqUrl);
    const channel = await consumerConnection.createChannel();
    await channel.assertExchange('events', 'topic', { durable: true });
    await channel.assertQueue('payment_queue', { durable: true });
    await channel.bindQueue('payment_queue', 'events', 'order.placed');
    await channel.consume('payment_queue', async (message) => {
      if (!message) return;
      const order = JSON.parse(message.content.toString());
      const payment = {
        id: crypto.randomUUID(),
        orderId: order.id,
        amount: Number(order.total) || 0,
        method: 'order-event',
        status: 'success',
        createdAt: new Date().toISOString(),
      };
      payments.set(payment.id, payment);
      await publishPayment(payment);
      channel.ack(message);
    });
    console.log('Payment service consuming order.placed events');
  } catch (error) {
    console.warn(`RabbitMQ unavailable: ${error.message}`);
    setTimeout(consumeOrders, 3000);
  }
}

async function publishPayment(payment) {
  try {
    const connection = await amqp.connect(rabbitmqUrl);
    const channel = await connection.createConfirmChannel();
    await channel.assertExchange('events', 'topic', { durable: true });
    channel.publish('events', 'payment.success', Buffer.from(JSON.stringify(payment)), { persistent: true });
    await channel.waitForConfirms();
    await connection.close();
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

app.post('/payments', async (request, response) => {
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
  await publishPayment(payment);
  return response.status(201).json(payment);
});

app.listen(port, () => {
  console.log(`Payment service listening on port ${port}`);
  consumeOrders();
});

async function shutdown() {
  if (consumerConnection) await consumerConnection.close();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
