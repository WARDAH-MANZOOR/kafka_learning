import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "payment-producer",
  brokers: ["192.168.1.26:9092"]
});

const producer = kafka.producer();

export const producePayment = async (merchantId: number, amount: number, status: string) => {
  await producer.connect();
  await producer.send({
    topic: "payments",
    messages: [
      {
        key: merchantId.toString(),  // partition key
        value: JSON.stringify({ merchantId, amount, status })
      }
    ]
  });
  await producer.disconnect();
};