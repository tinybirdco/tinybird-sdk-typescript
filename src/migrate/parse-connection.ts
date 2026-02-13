import type { KafkaConnectionModel, ResourceFile } from "./types.js";
import {
  MigrationParseError,
  isBlank,
  parseDirectiveLine,
  splitLines,
} from "./parser-utils.js";

export function parseConnectionFile(resource: ResourceFile): KafkaConnectionModel {
  const lines = splitLines(resource.content);
  let connectionType: string | undefined;
  let bootstrapServers: string | undefined;
  let securityProtocol:
    | "SASL_SSL"
    | "PLAINTEXT"
    | "SASL_PLAINTEXT"
    | undefined;
  let saslMechanism:
    | "PLAIN"
    | "SCRAM-SHA-256"
    | "SCRAM-SHA-512"
    | "OAUTHBEARER"
    | undefined;
  let key: string | undefined;
  let secret: string | undefined;
  let sslCaPem: string | undefined;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (isBlank(line)) {
      continue;
    }

    const { key: directive, value } = parseDirectiveLine(line);
    switch (directive) {
      case "TYPE":
        connectionType = value;
        break;
      case "KAFKA_BOOTSTRAP_SERVERS":
        bootstrapServers = value;
        break;
      case "KAFKA_SECURITY_PROTOCOL":
        if (value !== "SASL_SSL" && value !== "PLAINTEXT" && value !== "SASL_PLAINTEXT") {
          throw new MigrationParseError(
            resource.filePath,
            "connection",
            resource.name,
            `Unsupported KAFKA_SECURITY_PROTOCOL: "${value}"`
          );
        }
        securityProtocol = value;
        break;
      case "KAFKA_SASL_MECHANISM":
        if (
          value !== "PLAIN" &&
          value !== "SCRAM-SHA-256" &&
          value !== "SCRAM-SHA-512" &&
          value !== "OAUTHBEARER"
        ) {
          throw new MigrationParseError(
            resource.filePath,
            "connection",
            resource.name,
            `Unsupported KAFKA_SASL_MECHANISM: "${value}"`
          );
        }
        saslMechanism = value;
        break;
      case "KAFKA_KEY":
        key = value;
        break;
      case "KAFKA_SECRET":
        secret = value;
        break;
      case "KAFKA_SSL_CA_PEM":
        sslCaPem = value;
        break;
      default:
        throw new MigrationParseError(
          resource.filePath,
          "connection",
          resource.name,
          `Unsupported connection directive in strict mode: "${line}"`
        );
    }
  }

  if (!connectionType) {
    throw new MigrationParseError(
      resource.filePath,
      "connection",
      resource.name,
      "TYPE directive is required."
    );
  }

  if (connectionType !== "kafka") {
    throw new MigrationParseError(
      resource.filePath,
      "connection",
      resource.name,
      `Unsupported connection type in strict mode: "${connectionType}"`
    );
  }

  if (!bootstrapServers) {
    throw new MigrationParseError(
      resource.filePath,
      "connection",
      resource.name,
      "KAFKA_BOOTSTRAP_SERVERS is required for kafka connections."
    );
  }

  return {
    kind: "connection",
    name: resource.name,
    filePath: resource.filePath,
    connectionType: "kafka",
    bootstrapServers,
    securityProtocol,
    saslMechanism,
    key,
    secret,
    sslCaPem,
  };
}

