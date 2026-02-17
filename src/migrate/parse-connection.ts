import type { KafkaConnectionModel, ResourceFile, S3ConnectionModel } from "./types.js";
import {
  MigrationParseError,
  isBlank,
  parseDirectiveLine,
  parseQuotedValue,
  splitLines,
} from "./parser-utils.js";

export function parseConnectionFile(
  resource: ResourceFile
): KafkaConnectionModel | S3ConnectionModel {
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
  let schemaRegistryUrl: string | undefined;
  let sslCaPem: string | undefined;

  let region: string | undefined;
  let arn: string | undefined;
  let accessKey: string | undefined;
  let accessSecret: string | undefined;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (isBlank(line) || line.startsWith("#")) {
      continue;
    }

    const { key: directive, value } = parseDirectiveLine(line);
    switch (directive) {
      case "TYPE":
        connectionType = parseQuotedValue(value);
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
      case "KAFKA_SCHEMA_REGISTRY_URL":
        schemaRegistryUrl = value;
        break;
      case "KAFKA_SSL_CA_PEM":
        sslCaPem = value;
        break;
      case "S3_REGION":
        region = parseQuotedValue(value);
        break;
      case "S3_ARN":
        arn = parseQuotedValue(value);
        break;
      case "S3_ACCESS_KEY":
        accessKey = parseQuotedValue(value);
        break;
      case "S3_SECRET":
        accessSecret = parseQuotedValue(value);
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

  if (connectionType === "kafka") {
    if (region || arn || accessKey || accessSecret) {
      throw new MigrationParseError(
        resource.filePath,
        "connection",
        resource.name,
        "S3 directives are not valid for kafka connections."
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
      schemaRegistryUrl,
      sslCaPem,
    };
  }

  if (connectionType === "s3") {
    if (
      bootstrapServers ||
      securityProtocol ||
      saslMechanism ||
      key ||
      secret ||
      schemaRegistryUrl ||
      sslCaPem
    ) {
      throw new MigrationParseError(
        resource.filePath,
        "connection",
        resource.name,
        "Kafka directives are not valid for s3 connections."
      );
    }

    if (!region) {
      throw new MigrationParseError(
        resource.filePath,
        "connection",
        resource.name,
        "S3_REGION is required for s3 connections."
      );
    }

    if (!arn && !(accessKey && accessSecret)) {
      throw new MigrationParseError(
        resource.filePath,
        "connection",
        resource.name,
        "S3 connections require S3_ARN or both S3_ACCESS_KEY and S3_SECRET."
      );
    }

    if ((accessKey && !accessSecret) || (!accessKey && accessSecret)) {
      throw new MigrationParseError(
        resource.filePath,
        "connection",
        resource.name,
        "S3_ACCESS_KEY and S3_SECRET must be provided together."
      );
    }

    return {
      kind: "connection",
      name: resource.name,
      filePath: resource.filePath,
      connectionType: "s3",
      region,
      arn,
      accessKey,
      secret: accessSecret,
    };
  }

  throw new MigrationParseError(
    resource.filePath,
    "connection",
    resource.name,
    `Unsupported connection type in strict mode: "${connectionType}"`
  );
}
