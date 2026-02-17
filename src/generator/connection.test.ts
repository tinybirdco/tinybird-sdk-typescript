import { describe, it, expect } from "vitest";
import { generateConnection, generateAllConnections } from "./connection.js";
import { defineKafkaConnection, defineS3Connection } from "../schema/connection.js";

describe("Connection Generator", () => {
  describe("generateConnection", () => {
    it("generates basic Kafka connection with required fields", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });

      const result = generateConnection(conn);

      expect(result.name).toBe("my_kafka");
      expect(result.content).toContain("TYPE kafka");
      expect(result.content).toContain("KAFKA_BOOTSTRAP_SERVERS kafka.example.com:9092");
    });

    it("includes security protocol when provided", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        securityProtocol: "SASL_SSL",
      });

      const result = generateConnection(conn);

      expect(result.content).toContain("KAFKA_SECURITY_PROTOCOL SASL_SSL");
    });

    it("includes SASL mechanism when provided", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        saslMechanism: "PLAIN",
      });

      const result = generateConnection(conn);

      expect(result.content).toContain("KAFKA_SASL_MECHANISM PLAIN");
    });

    it("includes key and secret when provided", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        key: '{{ tb_secret("KAFKA_KEY") }}',
        secret: '{{ tb_secret("KAFKA_SECRET") }}',
      });

      const result = generateConnection(conn);

      expect(result.content).toContain('KAFKA_KEY {{ tb_secret("KAFKA_KEY") }}');
      expect(result.content).toContain('KAFKA_SECRET {{ tb_secret("KAFKA_SECRET") }}');
    });

    it("includes schema registry URL when provided", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        schemaRegistryUrl: "https://registry-user:registry-pass@registry.example.com",
      });

      const result = generateConnection(conn);

      expect(result.content).toContain(
        "KAFKA_SCHEMA_REGISTRY_URL https://registry-user:registry-pass@registry.example.com"
      );
    });

    it("includes SSL CA PEM when provided", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        sslCaPem: '{{ tb_secret("KAFKA_CA_CERT") }}',
      });

      const result = generateConnection(conn);

      expect(result.content).toContain('KAFKA_SSL_CA_PEM {{ tb_secret("KAFKA_CA_CERT") }}');
    });

    it("generates full Kafka connection with all options", () => {
      const conn = defineKafkaConnection("my_kafka", {
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
      const conn = defineKafkaConnection("local_kafka", {
        bootstrapServers: "localhost:9092",
        securityProtocol: "PLAINTEXT",
      });

      const result = generateConnection(conn);

      expect(result.content).toContain("KAFKA_SECURITY_PROTOCOL PLAINTEXT");
    });

    it("supports different SASL mechanisms", () => {
      const mechanisms = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"] as const;

      mechanisms.forEach((mechanism) => {
        const conn = defineKafkaConnection("my_kafka", {
          bootstrapServers: "kafka.example.com:9092",
          saslMechanism: mechanism,
        });

        const result = generateConnection(conn);

        expect(result.content).toContain(`KAFKA_SASL_MECHANISM ${mechanism}`);
      });
    });

    it("generates basic S3 connection with IAM role auth", () => {
      const conn = defineS3Connection("my_s3", {
        region: "us-east-1",
        arn: "arn:aws:iam::123456789012:role/tinybird-s3-access",
      });

      const result = generateConnection(conn);

      expect(result.name).toBe("my_s3");
      expect(result.content).toContain("TYPE s3");
      expect(result.content).toContain("S3_REGION us-east-1");
      expect(result.content).toContain(
        "S3_ARN arn:aws:iam::123456789012:role/tinybird-s3-access"
      );
    });

    it("generates S3 connection with access key auth", () => {
      const conn = defineS3Connection("my_s3", {
        region: "us-east-1",
        accessKey: '{{ tb_secret("S3_ACCESS_KEY") }}',
        secret: '{{ tb_secret("S3_SECRET") }}',
      });

      const result = generateConnection(conn);

      expect(result.content).toContain("TYPE s3");
      expect(result.content).toContain('S3_ACCESS_KEY {{ tb_secret("S3_ACCESS_KEY") }}');
      expect(result.content).toContain('S3_SECRET {{ tb_secret("S3_SECRET") }}');
    });
  });

  describe("generateAllConnections", () => {
    it("generates all connections", () => {
      const conn1 = defineKafkaConnection("kafka1", {
        bootstrapServers: "kafka1.example.com:9092",
      });
      const conn2 = defineS3Connection("s3_logs", {
        region: "us-east-1",
        arn: "arn:aws:iam::123456789012:role/tinybird-s3-access",
      });

      const results = generateAllConnections({ kafka1: conn1, s3_logs: conn2 });

      expect(results).toHaveLength(2);
      expect(results.map((r) => r.name).sort()).toEqual(["kafka1", "s3_logs"]);
    });

    it("returns empty array for empty connections", () => {
      const results = generateAllConnections({});

      expect(results).toHaveLength(0);
    });
  });
});
