import rabbit from "rabbitmq-stream-js-client";

const SUPER_STREAM = "hr_csv_data_super_stream1";

async function startConsumer() {
    const client = await rabbit.connect({
        hostname: "localhost",
        port: 5552,
        username: "test",
        password: "test",
        vhost: "/",
        heartbeat: 0,
    });

    // Declare a consumer on the super stream
    await client.declareSuperStreamConsumer(
        {
            superStream: SUPER_STREAM
        },
        (message) => {
            console.log("Received message:", message.content.toString());
            // message.ack();
        }
    );


    console.log("Consumer is running. Listening for messages...");
}

startConsumer().catch(console.error);
