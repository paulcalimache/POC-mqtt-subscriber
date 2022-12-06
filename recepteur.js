const mqtt = require('mqtt');
const MongoClient = require('mongodb').MongoClient;

// Connect to the MQTT broker
const client = mqtt.connect('mqtt://broker.emqx.io');

client.on('connect', () => {
  // Once connected, subscribe to the cesi topic
  client.subscribe('cesi', (err) => {
    if (!err) {
      console.log('Successfully subscribed to the cesi topic!');
    }
  });
});

// Set up a callback to handle incoming messages
client.on('message', (topic, message) => {
  // Convert the message to a string
  const messageStr = message.toString();

  // Connect to the MongoDB instance
  MongoClient.connect('mongodb://localhost:27017/', (err, client) => {
    if (err) {
      console.error(err);
      return;
    }

    const db = client.db("mqttDB");

    // Insert the message into the messages collection
    db.collection('messages').insertOne({
      topic: topic,
      message: messageStr,
    }, (err, result) => {
      if (err) {
        console.error(err);
        return;
      }

      console.log('Successfully inserted message : "' + messageStr + '" into MongoDB!');
    });
  });
});