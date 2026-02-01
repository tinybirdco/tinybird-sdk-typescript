import { describe, it, expect } from "vitest";
import { generateConnection, generateAllConnections } from "./connection.js";
import { createKafkaConnection } from "../schema/connection.js";

describe("Connection Generator", () => {
  describe("generateConnection", () => {
    it("generates basic Kafka connection with required fields", () => {
      const conn = createKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });

      const result = generateConnection(conn);

      expect(result.name).toBe("my_kafka");
      expect(result.content).toContain("TYPE kafka");
      expect(result.content).toContain("KAFKA_BOOTSTRAP_SERVERS kafka.example.com:9092");
    });

    it("includes security protocol when provided", () => {
      const conn = createKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        securityProtocol: "SASL_SSL",
      });

      const result = generateConnection(conn);

      expect(result.content).toContain("KAFKA_SECURITY_PROTOCOL SASL_SSL");
    });

    it("includes SASL mechanism when provided", () => {
      const conn = createKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        saslMechanism: "PLAIN",
      });

      const result = generateConnection(conn);

      expect(result.content).toContain("KAFKA_SASL_MECHANISM PLAIN");
    });

    it("includes key and secret when provided", () => {
      const conn = createKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        key: '{{ tb_secret("KAFKA_KEY") }}',
        secret: '{{ tb_secret("KAFKA_SECRET") }}',
      });

      const result = generateConnection(conn);

      expect(result.content).toContain('KAFKA_KEY {{ tb_secret("KAFKA_KEY") }}');
      expect(result.content).toContain('KAFKA_SECRET {{ tb_secret("KAFKA_SECRET") }}');
    });

    it("includes SSL CA PEM when provided", () => {
      const conn = createKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        sslCaPem: '{{ tb_secret("KAFKA_CA_CERT") }}',
      });

      const result = generateConnection(conn);

      expect(result.content).toContain('KAFKA_SSL_CA_PEM {{ tb_secret("KAFKA_CA_CERT") }}');
    });

    it("generates full Kafka connection with all options", () => {
      const conn = createKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        securityProtocol: "SASL_SSL",
        saslMechanism: "SCRAM-SHA-256",
        key: '{{ tb_secret("KAFKA_KEY") }}',
        secret: '{{ tb_secret("KAFKA_SECRET") }}',
        sslCaPem: '{{ tb_secret("KAFKA_CA_CERT") }}',
      });

      const result = generateConnection(conn);

      expect(result.name).toBe("my_kafka");
      expect(result.content).toContain("TYPE kafka");
      expect(result.content).toContain("KAFKA_BOOTSTRAP_SERVERS kafka.example.com:9092");
      expect(result.content).toContain("KAFKA_SECURITY_PROTOCOL SASL_SSL");
      expect(result.content).toContain("KAFKA_SASL_MECHANISM SCRAM-SHA-256");
      expect(result.content).toContain('KAFKA_KEY {{ tb_secret("KAFKA_KEY") }}');
      expect(result.content).toContain('KAFKA_SECRET {{ tb_secret("KAFKA_SECRET") }}');
      expect(result.content).toContain('KAFKA_SSL_CA_PEM {{ tb_secret("KAFKA_CA_CERT") }}');
    });

    it("supports PLAINTEXT security protocol", () => {
      const conn = createKafkaConnection("local_kafka", {
        bootstrapServers: "localhost:9092",
        securityProtocol: "PLAINTEXT",
      });

      const result = generateConnection(conn);

      expect(result.content).toContain("KAFKA_SECURITY_PROTOCOL PLAINTEXT");
    });

    it("supports different SASL mechanisms", () => {
      const mechanisms = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"] as const;

      mechanisms.forEach((mechanism) => {
        const conn = createKafkaConnection("my_kafka", {
          bootstrapServers: "kafka.example.com:9092",
          saslMechanism: mechanism,
        });

        const result = generateConnection(conn);

        expect(result.content).toContain(`KAFKA_SASL_MECHANISM ${mechanism}`);
      });
    });
  });

  describe("generateAllConnections", () => {
    it("generates all connections", () => {
      const conn1 = createKafkaConnection("kafka1", {
        bootstrapServers: "kafka1.example.com:9092",
      });
      const conn2 = createKafkaConnection("kafka2", {
        bootstrapServers: "kafka2.example.com:9092",
      });

      const results = generateAllConnections({ kafka1: conn1, kafka2: conn2 });

      expect(results).toHaveLength(2);
      expect(results.map((r) => r.name).sort()).toEqual(["kafka1", "kafka2"]);
    });

    it("returns empty array for empty connections", () => {
      const results = generateAllConnections({});

      expect(results).toHaveLength(0);
    });
  });
});
