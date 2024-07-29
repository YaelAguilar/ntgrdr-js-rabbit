require('dotenv').config();
const amqp = require('amqplib');
const { db } = require('./config');
const io = require('socket.io-client');

async function connectRabbitMQ() {
    try {
        const connection = await amqp.connect(process.env.CLOUDAMQP_URL);
        const channel = await connection.createChannel();

        const queueNames = ['flujoAgua', 'nivelFertilizante', 'ph'];

        // Conectar al servidor WebSocket
        const socket = io.connect('https://wss.soursop.lat', {
            secure: true,
            reconnection: true,
            rejectUnauthorized: false,
            extraHeaders: {
                Authorization: `Bearer ${process.env.JWT_SECRET_KEY}`
            }
        });

        for (const queue of queueNames) {
            await channel.assertQueue(queue, { durable: true });
            channel.consume(queue, async (msg) => {
                if (msg !== null) {
                    const content = JSON.parse(msg.content.toString());
                    console.log(`Received message from ${queue}: ${JSON.stringify(content)}`);

                    if (queue === 'flujoAgua') {
                        // Guardar datos en la tabla consumo_agua
                        const { flow_rate_lpm, total_liters } = content;
                        if (flow_rate_lpm !== undefined && total_liters !== undefined) {
                            const query = 'INSERT INTO consumo_agua (sensor_id, cantidad, litros_por_minuto) VALUES (4, ?, ?)';
                            db.query(query, [total_liters, flow_rate_lpm], (err, results) => {
                                if (err) {
                                    console.error('Error inserting data into consumo_agua:', err);
                                } else {
                                    console.log('Data inserted into consumo_agua:', results);
                                    // Enviar mensaje al servidor WebSocket
                                    socket.emit('flujoAgua', content);
                                }
                            });
                        } else {
                            console.error('Invalid data received for flujoAgua:', content);
                        }
                    } else if (queue === 'ph') {
                        // Guardar datos en la tabla estado_planta
                        const { humidity, temperature, conductivity } = content;
                        if (humidity !== undefined && temperature !== undefined && conductivity !== undefined) {
                            const query = 'INSERT INTO estado_planta (sensor_id, humedad, temperatura, conductividad) VALUES (1, ?, ?, ?)';
                            db.query(query, [humidity, temperature, conductivity], (err, results) => {
                                if (err) {
                                    console.error('Error inserting data into estado_planta:', err);
                                } else {
                                    console.log('Data inserted into estado_planta:', results);
                                    // Enviar mensaje al servidor WebSocket
                                    socket.emit('ph', content);
                                }
                            });
                        } else {
                            console.error('Invalid data received for ph:', content);
                        }
                    } else if (queue === 'nivelFertilizante') {
                        let cantidad;
                        if (content === 'hay fertilizante') {
                            cantidad = 20;
                            const insertQuery = 'INSERT INTO consumo_fertilizante (sensor_id, cantidad) VALUES (3, ?)';
                            db.query(insertQuery, [cantidad], (err, results) => {
                                if (err) {
                                    console.error('Error inserting data into consumo_fertilizante:', err);
                                } else {
                                    console.log('Data inserted into consumo_fertilizante:', results);
                                    // Enviar mensaje al servidor WebSocket
                                    socket.emit('nivelFertilizante', content);
                                }
                            });
                        } else if (content === 'no hay fertilizante') {
                            const query = 'SELECT litros_por_minuto FROM consumo_agua ORDER BY consumo_id DESC LIMIT 1';
                            db.query(query, (err, results) => {
                                if (err) {
                                    console.error('Error fetching data from consumo_agua:', err);
                                } else {
                                    const lastLitrosPorMinuto = results[0]?.litros_por_minuto || 0;
                                    cantidad = lastLitrosPorMinuto / 6;
                                    const insertQuery = 'INSERT INTO consumo_fertilizante (sensor_id, cantidad) VALUES (3, ?)';
                                    db.query(insertQuery, [cantidad], (err, results) => {
                                        if (err) {
                                            console.error('Error inserting data into consumo_fertilizante:', err);
                                        } else {
                                            console.log('Data inserted into consumo_fertilizante:', results);
                                            // Enviar mensaje al servidor WebSocket
                                            socket.emit('nivelFertilizante', content);
                                        }
                                    });
                                }
                            });
                        }
                    }

                    channel.ack(msg);
                }
            });
        }

        console.log('Waiting for messages. To exit press CTRL+C');
    } catch (error) {
        console.error('Error in RabbitMQ connection:', error);
        setTimeout(connectRabbitMQ, 5000);
    }
}

connectRabbitMQ();
