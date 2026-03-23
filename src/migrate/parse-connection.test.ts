import { describe, it, expect } from "vitest";
import { parseConnectionFile } from "./parse-connection.js";
import type { ResourceFile } from "./types.js";

function resource(name: string, content: string): ResourceFile {
  return {
    kind: "connection",
    name,
    filePath: `${name}.connection`,
    absolutePath: `/tmp/${name}.connection`,
    content,
  };
}

describe("parseConnectionFile", () => {
  it("parses basic Kafka connection", () => {
    const result = parseConnectionFile(
      resource(
        "my_kafka",
        `TYPE kafka
KAFKA_BOOTSTRAP_SERVERS localhost:9092`
      )
    );

    expect(result.connectionType).toBe("kafka");
    expect(result).toHaveProperty("bootstrapServers", "localhost:9092");
  });

  it("parses single-quoted TYPE", () => {
    const result = parseConnectionFile(
      resource(
        "my_kafka",
        `TYPE 'kafka'
KAFKA_BOOTSTRAP_SERVERS localhost:9092
KAFKA_SCHEMA_REGISTRY_URL https://registry.example.com`
      )
    );

    expect(result.connectionType).toBe("kafka");
    expect(result).toHaveProperty("schemaRegistryUrl", "https://registry.example.com");
  });

  it("parses multiline SSL CA PEM with > syntax", () => {
    const result = parseConnectionFile(
      resource(
        "my_kafka",
        `TYPE kafka
KAFKA_BOOTSTRAP_SERVERS localhost:9092
KAFKA_SECURITY_PROTOCOL SASL_SSL
KAFKA_SSL_CA_PEM >
    -----BEGIN CERTIFICATE-----
    MIIDXTCCAkWgAwIBAgIJAM
    -----END CERTIFICATE-----`
      )
    );

    expect(result).toHaveProperty(
      "sslCaPem",
      "-----BEGIN CERTIFICATE-----\nMIIDXTCCAkWgAwIBAgIJAM\n-----END CERTIFICATE-----"
    );
  });

  it("parses multiline SSL CA PEM with directives after the block", () => {
    const result = parseConnectionFile(
      resource(
        "my_kafka",
        `TYPE kafka
KAFKA_BOOTSTRAP_SERVERS localhost:9092
KAFKA_SSL_CA_PEM >
    -----BEGIN CERTIFICATE-----
    MIIDXTCCAkWgAwIBAgIJAM
    -----END CERTIFICATE-----
KAFKA_SECURITY_PROTOCOL SASL_SSL
KAFKA_KEY mykey`
      )
    );

    expect(result).toHaveProperty(
      "sslCaPem",
      "-----BEGIN CERTIFICATE-----\nMIIDXTCCAkWgAwIBAgIJAM\n-----END CERTIFICATE-----"
    );
    expect(result).toHaveProperty("securityProtocol", "SASL_SSL");
    expect(result).toHaveProperty("key", "mykey");
  });

  it("parses single-line SSL CA PEM (e.g. a secret reference)", () => {
    const result = parseConnectionFile(
      resource(
        "my_kafka",
        `TYPE kafka
KAFKA_BOOTSTRAP_SERVERS localhost:9092
KAFKA_SSL_CA_PEM {{ tb_secret('KAFKA_SSL_CA_PEM') }}`
      )
    );

    expect(result).toHaveProperty("sslCaPem", "{{ tb_secret('KAFKA_SSL_CA_PEM') }}");
  });
});
