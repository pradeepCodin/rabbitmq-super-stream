import rabbit from "rabbitmq-stream-js-client";
import crypto from 'crypto';

const client = await rabbit.connect({
  hostname: "localhost",
  port: 5552,
  username: 'test',
  password: 'test',
  vhost: "/",
  heartbeat: 0,
});

const SUPER_STREAM = "hr_csv_data_super_stream1";
const PARTITIONS = 5;


await client.createSuperStream({ streamName: SUPER_STREAM });

await new Promise(r => setTimeout(r, 200));
// Simple hash function for partitioning
function hashEmail(inputStr, partitions) {
  const hash = crypto.createHash('sha256').update(inputStr).digest('hex');
  const hashInt = parseInt(hash.slice(0, 8), 16); // take first 8 hex chars
  const partition = hashInt % partitions;
  return `partition.${partition}`;
}

const routingKeyExtractor = (content, msgOptions) => {
  const email = msgOptions.email;
  const routingKey = hashEmail(email, PARTITIONS); // hash → routing key
  console.log('Routing key:', routingKey);
  return routingKey;
};

const publisher = await client.declareSuperStreamPublisher(
  { superStream: SUPER_STREAM },
  routingKeyExtractor
);

// Test messages
const messages = [
  { email: "patientA@example.com", event: "Admitted" },
  { email: "patientA@example.com", event: "Discharged" },
  { email: "patientB@test.com", event: "Admitted" },
];

for (const msg of messages) {
  await publisher.send(
    Buffer.from(JSON.stringify(msg)),
    { messageProperties: { messageId: msg.email }, email: msg.email }
  );
  console.log("Published:", msg);
}

await client.close();
