/**
 * Connection definition for Tinybird
 * Define external connections (Kafka, etc.) as TypeScript with full type safety
 */

// Symbol for brand typing
const CONNECTION_BRAND = Symbol("tinybird.connection");

/**
 * Kafka security protocol options
 */
export type KafkaSecurityProtocol = "SASL_SSL" | "PLAINTEXT" | "SASL_PLAINTEXT";

/**
 * Kafka SASL mechanism options
 */
export type KafkaSaslMechanism = "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" | "OAUTHBEARER";

/**
 * Options for creating a Kafka connection
 */
export interface KafkaConnectionOptions {
  /** Kafka bootstrap servers (host:port) */
  bootstrapServers: string;
  /** Security protocol (default: 'SASL_SSL') */
  securityProtocol?: KafkaSecurityProtocol;
  /** SASL mechanism for authentication */
  saslMechanism?: KafkaSaslMechanism;
  /** Kafka key/username - can use {{ tb_secret(...) }} */
  key?: string;
  /** Kafka secret/password - can use {{ tb_secret(...) }} */
  secret?: string;
  /** SSL CA certificate PEM - for private CA certs */
  sslCaPem?: string;
}

/**
 * Kafka-specific connection definition
 */
export interface KafkaConnectionDefinition {
  readonly [CONNECTION_BRAND]: true;
  /** Connection name */
  readonly _name: string;
  /** Type marker for inference */
  readonly _type: "connection";
  /** Connection type */
  readonly _connectionType: "kafka";
  /** Kafka options */
  readonly options: KafkaConnectionOptions;
}

/**
 * A connection definition - union of all connection types
 */
export type ConnectionDefinition = KafkaConnectionDefinition;

/**
 * Create a Kafka connection
 *
 * @param name - The connection name (must be valid identifier)
 * @param options - Kafka connection configuration
 * @returns A connection definition that can be used in a project
 *
 * @example
 * ```ts
 * import { createKafkaConnection } from '@tinybirdco/sdk';
 *
 * export const myKafka = createKafkaConnection('my_kafka', {
 *   bootstrapServers: 'kafka.example.com:9092',
 *   securityProtocol: 'SASL_SSL',
 *   saslMechanism: 'PLAIN',
 *   key: '{{ tb_secret("KAFKA_KEY") }}',
 *   secret: '{{ tb_secret("KAFKA_SECRET") }}',
 * });
 * ```
 */
export function createKafkaConnection(
  name: string,
  options: KafkaConnectionOptions
): KafkaConnectionDefinition {
  // Validate name is a valid identifier
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(
      `Invalid connection name: "${name}". Must start with a letter or underscore and contain only alphanumeric characters and underscores.`
    );
  }

  return {
    [CONNECTION_BRAND]: true,
    _name: name,
    _type: "connection",
    _connectionType: "kafka",
    options,
  };
}

/**
 * Check if a value is a connection definition
 */
export function isConnectionDefinition(value: unknown): value is ConnectionDefinition {
  return (
    typeof value === "object" &&
    value !== null &&
    CONNECTION_BRAND in value &&
    (value as Record<symbol, unknown>)[CONNECTION_BRAND] === true
  );
}

/**
 * Check if a value is a Kafka connection definition
 */
export function isKafkaConnectionDefinition(value: unknown): value is KafkaConnectionDefinition {
  return isConnectionDefinition(value) && value._connectionType === "kafka";
}

/**
 * Get the connection type from a connection definition
 */
export function getConnectionType<T extends ConnectionDefinition>(
  connection: T
): T["_connectionType"] {
  return connection._connectionType;
}
