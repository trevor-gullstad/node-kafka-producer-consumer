import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

const producer = Kafka.Producer({ 'metadata.broker.list': 'localhost:9092'}, {});

producer.connect();

producer.on('ready', () => {
  console.log('producer ready..');
  setInterval(() => {
    produceRandomEvent();
  }, 3000);
});

producer.on('event.error', function(err) {
  console.error('Error from producer');
  console.error(err);
})

producer.setPollInterval(100);

function produceRandomEvent() {
  const category = getRandomAnimal();
  const noise = getRandomNoise(category);
  const headers = [{ TYPE: 'random' }];
  const message = { category, noise };
  const success = producer.produce('test', null, eventType.toBuffer(message), 'key', 0, {}, headers);
  if (success) {
    console.log(`produced event with message: (${JSON.stringify(message)})`);
  } else {
    console.log('error producing event..');
  }
}

function getRandomAnimal() {
  const categories = ['CAT', 'DOG'];
  return categories[Math.floor(Math.random() * categories.length)];
}

function getRandomNoise(animal) {
  if (animal === 'CAT') {
    const noises = ['meow', 'purr'];
    return noises[Math.floor(Math.random() * noises.length)];
  } else if (animal === 'DOG') {
    const noises = ['bark', 'woof'];
    return noises[Math.floor(Math.random() * noises.length)];
  } else {
    return 'silence..';
  }
}
