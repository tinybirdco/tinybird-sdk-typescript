/**
 * Connection content generator
 * Converts ConnectionDefinition to native .connection file format
 */

import type { ConnectionDefinition, KafkaConnectionDefinition } from "../schema/connection.js";

/**
 * Generated connection content
 */
export interface GeneratedConnection {
  /** Connection name */
  name: string;
  /** The generated .connection file content */
  content: string;
}

/**
 * Generate a Kafka connection content
 */
function generateKafkaConnection(connection: KafkaConnectionDefinition): string {
  const parts: string[] = [];
  const options = connection.options;

  parts.push("TYPE kafka");
  parts.push(`KAFKA_BOOTSTRAP_SERVERS ${options.bootstrapServers}`);

  if (options.securityProtocol) {
    parts.push(`KAFKA_SECURITY_PROTOCOL ${options.securityProtocol}`);
  }

  if (options.saslMechanism) {
    parts.push(`KAFKA_SASL_MECHANISM ${options.saslMechanism}`);
  }

  if (options.key) {
    parts.push(`KAFKA_KEY ${options.key}`);
  }

  if (options.secret) {
    parts.push(`KAFKA_SECRET ${options.secret}`);
  }

  if (options.sslCaPem) {
    parts.push(`KAFKA_SSL_CA_PEM ${options.sslCaPem}`);
  }

  return parts.join("\n");
}

/**
 * Generate a .connection file content from a ConnectionDefinition
 *
 * @param connection - The connection definition
 * @returns Generated connection content
 *
 * @example
 * ```ts
 * const myKafka = createKafkaConnection('my_kafka', {
 *   bootstrapServers: 'kafka.example.com:9092',
 *   securityProtocol: 'SASL_SSL',
 *   saslMechanism: 'PLAIN',
 *   key: '{{ tb_secret("KAFKA_KEY") }}',
 *   secret: '{{ tb_secret("KAFKA_SECRET") }}',
 * });
 *
 * const { content } = generateConnection(myKafka);
 * // Returns:
 * // TYPE kafka
 * // KAFKA_BOOTSTRAP_SERVERS kafka.example.com:9092
 * // KAFKA_SECURITY_PROTOCOL SASL_SSL
 * // KAFKA_SASL_MECHANISM PLAIN
 * // KAFKA_KEY {{ tb_secret("KAFKA_KEY") }}
 * // KAFKA_SECRET {{ tb_secret("KAFKA_SECRET") }}
 * ```
 */
export function generateConnection(
  connection: ConnectionDefinition
): GeneratedConnection {
  let content: string;

  if (connection._connectionType === "kafka") {
    content = generateKafkaConnection(connection as KafkaConnectionDefinition);
  } else {
    throw new Error(`Unsupported connection type: ${connection._connectionType}`);
  }

  return {
    name: connection._name,
    content,
  };
}

/**
 * Generate .connection files for all connections in a project
 *
 * @param connections - Record of connection definitions
 * @returns Array of generated connection content
 */
export function generateAllConnections(
  connections: Record<string, ConnectionDefinition>
): GeneratedConnection[] {
  return Object.values(connections).map(generateConnection);
}
