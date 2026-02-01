import { describe, it, expect } from "vitest";
import {
  createKafkaConnection,
  isConnectionDefinition,
  isKafkaConnectionDefinition,
  getConnectionType,
} from "./connection.js";

describe("Connection Schema", () => {
  describe("createKafkaConnection", () => {
    it("creates a Kafka connection with required fields", () => {
      const conn = createKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });

      expect(conn._name).toBe("my_kafka");
      expect(conn._type).toBe("connection");
      expect(conn._connectionType).toBe("kafka");
      expect(conn.options.bootstrapServers).toBe("kafka.example.com:9092");
    });

    it("creates a Kafka connection with all options", () => {
      const conn = createKafkaConnection("my_kafka", {
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
      const scramConn = createKafkaConnection("scram_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        saslMechanism: "SCRAM-SHA-256",
      });
      expect(scramConn.options.saslMechanism).toBe("SCRAM-SHA-256");

      const scram512Conn = createKafkaConnection("scram512_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        saslMechanism: "SCRAM-SHA-512",
      });
      expect(scram512Conn.options.saslMechanism).toBe("SCRAM-SHA-512");

      const oauthConn = createKafkaConnection("oauth_kafka", {
        bootstrapServers: "kafka.example.com:9092",
        saslMechanism: "OAUTHBEARER",
      });
      expect(oauthConn.options.saslMechanism).toBe("OAUTHBEARER");
    });

    it("supports different security protocols", () => {
      const plaintext = createKafkaConnection("plaintext_kafka", {
        bootstrapServers: "localhost:9092",
        securityProtocol: "PLAINTEXT",
      });
      expect(plaintext.options.securityProtocol).toBe("PLAINTEXT");

      const saslPlaintext = createKafkaConnection("sasl_plaintext_kafka", {
        bootstrapServers: "localhost:9092",
        securityProtocol: "SASL_PLAINTEXT",
      });
      expect(saslPlaintext.options.securityProtocol).toBe("SASL_PLAINTEXT");
    });

    it("throws error for invalid connection name", () => {
      expect(() =>
        createKafkaConnection("123invalid", {
          bootstrapServers: "kafka.example.com:9092",
        })
      ).toThrow("Invalid connection name");

      expect(() =>
        createKafkaConnection("my-connection", {
          bootstrapServers: "kafka.example.com:9092",
        })
      ).toThrow("Invalid connection name");

      expect(() =>
        createKafkaConnection("", {
          bootstrapServers: "kafka.example.com:9092",
        })
      ).toThrow("Invalid connection name");
    });

    it("allows valid naming patterns", () => {
      const conn1 = createKafkaConnection("_private_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });
      expect(conn1._name).toBe("_private_kafka");

      const conn2 = createKafkaConnection("kafka_v2", {
        bootstrapServers: "kafka.example.com:9092",
      });
      expect(conn2._name).toBe("kafka_v2");
    });
  });

  describe("isConnectionDefinition", () => {
    it("returns true for valid connection", () => {
      const conn = createKafkaConnection("my_kafka", {
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
      const conn = createKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });

      expect(isKafkaConnectionDefinition(conn)).toBe(true);
    });

    it("returns false for non-Kafka objects", () => {
      expect(isKafkaConnectionDefinition({})).toBe(false);
      expect(isKafkaConnectionDefinition(null)).toBe(false);
    });
  });

  describe("getConnectionType", () => {
    it("returns the connection type", () => {
      const conn = createKafkaConnection("my_kafka", {
        bootstrapServers: "kafka.example.com:9092",
      });

      expect(getConnectionType(conn)).toBe("kafka");
    });
  });
});
