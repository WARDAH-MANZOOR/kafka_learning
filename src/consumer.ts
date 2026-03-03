import { Kafka } from "kafkajs";
import { prisma } from "./prismaClient.js";

const kafka = new Kafka({
  clientId: "payment-consumer",
  brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({ groupId: "payment-verification-group" });

export const startConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "payments", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { merchantId, amount, status } = JSON.parse(message.value!.toString());
      console.log(`Received Payment: Merchant ${merchantId}, Amount ${amount}, Status ${status}`);

      // DB me save
      await prisma.transaction.create({
        data: {
          merchant_id: merchantId,
          amount,
          status
        }
      });
    }
  });
};