const express = require('express');
const crypto = require('node:crypto');
const amqp = require('amqplib');

const app = express();
const port = Number(process.env.PORT) || 3001;
const rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
let consumerConnection;

app.use(express.json());

const inventory = new Map();

async function consumeOrders() {
	try {
		consumerConnection = await amqp.connect(rabbitmqUrl);
		const channel = await consumerConnection.createChannel();
		await channel.assertExchange('events', 'topic', { durable: true });
		await channel.assertQueue('inventory_queue', { durable: true });
		await channel.bindQueue('inventory_queue', 'events', 'order.placed');
		await channel.consume('inventory_queue', (message) => {
			if (!message) return;
			const order = JSON.parse(message.content.toString());
			for (const item of order.items || []) {
				const product = Array.from(inventory.values()).find((entry) => entry.name === item.sku || entry.id === item.sku);
				if (product) product.quantity = Math.max(0, product.quantity - Number(item.quantity || 0));
			}
			channel.ack(message);
		});
		console.log('Inventory service consuming order.placed events');
	} catch (error) {
		console.warn(`RabbitMQ unavailable: ${error.message}`);
		setTimeout(consumeOrders, 3000);
	}
}

function toProduct(product) {
	return {
		id: product.id,
		name: product.name,
		quantity: product.quantity,
		price: product.price,
	};
}

app.get('/health', (request, response) => {
	response.json({ status: 'ok', service: 'inventory-service' });
});

app.get('/inventory', (request, response) => {
	response.json(Array.from(inventory.values()).map(toProduct));
});

app.get('/inventory/:id', (request, response) => {
	const product = inventory.get(request.params.id);

	if (!product) {
		return response.status(404).json({ error: 'Product not found' });
	}

	return response.json(toProduct(product));
});

app.post('/inventory', (request, response) => {
	const { name, quantity, price } = request.body;

	if (!name || !Number.isInteger(quantity) || quantity < 0 || typeof price !== 'number' || price < 0) {
		return response.status(400).json({
			error: 'name, quantity (non-negative integer), and price (non-negative number) are required',
		});
	}

	const product = {
		id: crypto.randomUUID(),
		name: String(name).trim(),
		quantity,
		price,
	};

	inventory.set(product.id, product);
	return response.status(201).json(toProduct(product));
});

app.patch('/inventory/:id', (request, response) => {
	const product = inventory.get(request.params.id);

	if (!product) {
		return response.status(404).json({ error: 'Product not found' });
	}

	const { name, quantity, price } = request.body;

	if (name !== undefined) product.name = String(name).trim();
	if (quantity !== undefined && Number.isInteger(quantity) && quantity >= 0) product.quantity = quantity;
	if (price !== undefined && typeof price === 'number' && price >= 0) product.price = price;

	return response.json(toProduct(product));
});

app.delete('/inventory/:id', (request, response) => {
	if (!inventory.delete(request.params.id)) {
		return response.status(404).json({ error: 'Product not found' });
	}

	return response.status(204).send();
});

app.listen(port, () => {
	console.log(`Inventory service listening on port ${port}`);
	consumeOrders();
});

async function shutdown() {
	if (consumerConnection) await consumerConnection.close();
	process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
