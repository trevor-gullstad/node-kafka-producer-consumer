import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

const consumer = new Kafka.KafkaConsumer({
  'group.id': 'kafka',
  'metadata.broker.list': 'localhost:9092',
}, {});

consumer.connect();

consumer.on('ready', () => {
  console.log('consumer ready..')
  consumer.subscribe(['test']);
  consumer.consume();
});

consumer.on('data', function(msg) {
  console.log(`received message: ${JSON.stringify(msg)}`);
  console.log(`  topic: ${msg.topic}`);
  console.log(`  key: ${msg.key}`);
  console.log(`  partition: ${msg.partition}`);
  console.log(`  offset: ${msg.offset}`);
  console.log(`  size: ${msg.size}`);
  console.log(`  timestamp: ${msg.timestamp}`);
  console.log(`  opaque: ${msg.opaque}`);
  console.log(`  headers: ${JSON.stringify(msg.headers)}`);
  console.log(`    0-TYPE: ${msg.headers[0]['TYPE']}`)
  console.log(`  value: ${JSON.stringify(msg.value)}`)
  console.log(`    fromBuffer: ${eventType.fromBuffer(msg.value)}`);
});
