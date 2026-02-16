import { describe, it, expect } from "vitest";
import {
  defineKafkaConnection,
  defineS3Connection,
  isConnectionDefinition,
  isKafkaConnectionDefinition,
  isS3ConnectionDefinition,
  getConnectionType,
} from "./connection.js";

describe("Connection Schema", () => {
  describe("defineKafkaConnection", () => {
    it("creates a Kafka connection with required fields", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });

      expect(conn._name).toBe("my_kafka");
      expect(conn._type).toBe("connection");
      expect(conn._connectionType).toBe("kafka");
      expect(conn.options.bootstrapServers).toBe("kafka.example.com:9092");
    });

    it("creates a Kafka connection with all options", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        securityProtocol: "SASL_SSL",
        saslMechanism: "PLAIN",
        key: '{{ tb_secret("KAFKA_KEY") }}',
        secret: '{{ tb_secret("KAFKA_SECRET") }}',
        sslCaPem: '{{ tb_secret("KAFKA_CA_CERT") }}',
      });

      expect(conn.options.securityProtocol).toBe("SASL_SSL");
      expect(conn.options.saslMechanism).toBe("PLAIN");
      expect(conn.options.key).toBe('{{ tb_secret("KAFKA_KEY") }}');
      expect(conn.options.secret).toBe('{{ tb_secret("KAFKA_SECRET") }}');
      expect(conn.options.sslCaPem).toBe('{{ tb_secret("KAFKA_CA_CERT") }}');
    });

    it("supports different SASL mechanisms", () => {
      const scramConn = defineKafkaConnection("scram_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        saslMechanism: "SCRAM-SHA-256",
      });
      expect(scramConn.options.saslMechanism).toBe("SCRAM-SHA-256");

      const scram512Conn = defineKafkaConnection("scram512_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        saslMechanism: "SCRAM-SHA-512",
      });
      expect(scram512Conn.options.saslMechanism).toBe("SCRAM-SHA-512");

      const oauthConn = defineKafkaConnection("oauth_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        saslMechanism: "OAUTHBEARER",
      });
      expect(oauthConn.options.saslMechanism).toBe("OAUTHBEARER");
    });

    it("supports different security protocols", () => {
      const plaintext = defineKafkaConnection("plaintext_kafka", {
        bootstrapServers: "localhost:9092",
        securityProtocol: "PLAINTEXT",
      });
      expect(plaintext.options.securityProtocol).toBe("PLAINTEXT");

      const saslPlaintext = defineKafkaConnection("sasl_plaintext_kafka", {
        bootstrapServers: "localhost:9092",
        securityProtocol: "SASL_PLAINTEXT",
      });
      expect(saslPlaintext.options.securityProtocol).toBe("SASL_PLAINTEXT");
    });

    it("throws error for invalid connection name", () => {
      expect(() =>
        defineKafkaConnection("123invalid", {
          bootstrapServers: "kafka.example.com:9092",
        })
      ).toThrow("Invalid connection name");

      expect(() =>
        defineKafkaConnection("my-connection", {
          bootstrapServers: "kafka.example.com:9092",
        })
      ).toThrow("Invalid connection name");

      expect(() =>
        defineKafkaConnection("", {
          bootstrapServers: "kafka.example.com:9092",
        })
      ).toThrow("Invalid connection name");
    });

    it("allows valid naming patterns", () => {
      const conn1 = defineKafkaConnection("_private_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });
      expect(conn1._name).toBe("_private_kafka");

      const conn2 = defineKafkaConnection("kafka_v2", {
        bootstrapServers: "kafka.example.com:9092",
      });
      expect(conn2._name).toBe("kafka_v2");
    });
  });

  describe("defineS3Connection", () => {
    it("creates an S3 connection with IAM role auth", () => {
      const conn = defineS3Connection("my_s3", {
        region: "us-east-1",
        arn: "arn:aws:iam::123456789012:role/tinybird-s3-access",
      });

      expect(conn._name).toBe("my_s3");
      expect(conn._type).toBe("connection");
      expect(conn._connectionType).toBe("s3");
      expect(conn.options.region).toBe("us-east-1");
      expect(conn.options.arn).toBe("arn:aws:iam::123456789012:role/tinybird-s3-access");
    });

    it("creates an S3 connection with access key auth", () => {
      const conn = defineS3Connection("my_s3", {
        region: "us-east-1",
        accessKey: '{{ tb_secret("S3_ACCESS_KEY") }}',
        secret: '{{ tb_secret("S3_SECRET") }}',
      });

      expect(conn.options.accessKey).toBe('{{ tb_secret("S3_ACCESS_KEY") }}');
      expect(conn.options.secret).toBe('{{ tb_secret("S3_SECRET") }}');
    });

    it("throws when auth config is incomplete", () => {
      expect(() =>
        defineS3Connection("my_s3", {
          region: "us-east-1",
        })
      ).toThrow("S3 connection requires either `arn` or both `accessKey` and `secret`.");

      expect(() =>
        defineS3Connection("my_s3", {
          region: "us-east-1",
          accessKey: "key-only",
        })
      ).toThrow("S3 connection requires either `arn` or both `accessKey` and `secret`.");

      expect(() =>
        defineS3Connection("my_s3", {
          region: "us-east-1",
          secret: "secret-only",
        })
      ).toThrow("S3 connection requires either `arn` or both `accessKey` and `secret`.");
    });
  });

  describe("isConnectionDefinition", () => {
    it("returns true for valid connection", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });

      expect(isConnectionDefinition(conn)).toBe(true);
    });

    it("returns false for non-connection objects", () => {
      expect(isConnectionDefinition({})).toBe(false);
      expect(isConnectionDefinition(null)).toBe(false);
      expect(isConnectionDefinition(undefined)).toBe(false);
      expect(isConnectionDefinition("string")).toBe(false);
      expect(isConnectionDefinition(123)).toBe(false);
      expect(isConnectionDefinition({ _name: "test" })).toBe(false);
    });
  });

  describe("isKafkaConnectionDefinition", () => {
    it("returns true for Kafka connection", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });

      expect(isKafkaConnectionDefinition(conn)).toBe(true);
    });

    it("returns false for non-Kafka objects", () => {
      expect(isKafkaConnectionDefinition({})).toBe(false);
      expect(isKafkaConnectionDefinition(null)).toBe(false);
    });
  });

  describe("isS3ConnectionDefinition", () => {
    it("returns true for S3 connection", () => {
      const conn = defineS3Connection("my_s3", {
        region: "us-east-1",
        arn: "arn:aws:iam::123456789012:role/tinybird-s3-access",
      });

      expect(isS3ConnectionDefinition(conn)).toBe(true);
    });

    it("returns false for non-S3 objects", () => {
      expect(isS3ConnectionDefinition({})).toBe(false);
      expect(isS3ConnectionDefinition(null)).toBe(false);
    });
  });

  describe("getConnectionType", () => {
    it("returns the connection type", () => {
      const conn = defineKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });

      expect(getConnectionType(conn)).toBe("kafka");
    });

    it("returns the s3 connection type", () => {
      const conn = defineS3Connection("my_s3", {
        region: "us-east-1",
        arn: "arn:aws:iam::123456789012:role/tinybird-s3-access",
      });

      expect(getConnectionType(conn)).toBe("s3");
    });
  });
});
