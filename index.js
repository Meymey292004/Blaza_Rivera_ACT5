const express = require('express');
const crypto = require('node:crypto');
const amqp = require('amqplib');

const app = express();
const port = Number(process.env.PORT) || 3002;
const rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const orders = new Map();

app.use(express.json());

async function publishOrder(order) {
  try {
    const connection = await amqp.connect(rabbitmqUrl);
    const channel = await connection.createConfirmChannel();
    await channel.assertExchange('events', 'topic', { durable: true });
    channel.publish('events', 'order.placed', Buffer.from(JSON.stringify(order)), { persistent: true });
    await channel.waitForConfirms();
    await connection.close();
  } catch (error) {
    console.warn(`RabbitMQ unavailable: ${error.message}`);
  }
}

app.get('/health', (request, response) => {
  response.json({ status: 'ok', service: 'order-service' });
});

app.get('/orders', (request, response) => {
  response.json(Array.from(orders.values()));
});

app.get('/orders/:id', (request, response) => {
  const order = orders.get(request.params.id);
  if (!order) return response.status(404).json({ error: 'Order not found' });
  return response.json(order);
});

app.post('/orders', async (request, response) => {
  const { customerId, items } = request.body;
  if (!customerId || !Array.isArray(items) || items.length === 0) {
    return response.status(400).json({ error: 'customerId and a non-empty items array are required' });
  }

  const order = {
    id: crypto.randomUUID(),
    customerId: String(customerId),
    items,
    status: 'placed',
    createdAt: new Date().toISOString(),
  };
  orders.set(order.id, order);
  await publishOrder(order);
  return response.status(201).json(order);
});

app.patch('/orders/:id/status', (request, response) => {
  const order = orders.get(request.params.id);
  if (!order) return response.status(404).json({ error: 'Order not found' });
  if (!request.body.status) return response.status(400).json({ error: 'status is required' });
  order.status = String(request.body.status);
  return response.json(order);
});

app.listen(port, () => {
  console.log(`Order service listening on port ${port}`);
});
