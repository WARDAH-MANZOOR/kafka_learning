import express from "express";
import { producePayment } from "./producer";
import { startConsumer } from "./consumer";

const app = express();
app.use(express.json());

// Start consumer in background
startConsumer().catch(console.error);

app.post("/simulate-payment", async (req, res) => {
  const { merchantId, amount, status } = req.body;
  if (!merchantId || !amount || !status) return res.status(400).json({ error: "Missing fields" });

  await producePayment(merchantId, amount, status);
  res.json({ message: "Payment message produced to Kafka" });
});

app.listen(3000, () => console.log("Server running on http://localhost:3000"));