/**
 * Pipe content generator
 * Converts PipeDefinition to native .pipe file format
 */

import type {
  PipeDefinition,
  NodeDefinition,
  EndpointConfig,
} from "../schema/pipe.js";
import { getEndpointConfig } from "../schema/pipe.js";

/**
 * Generated pipe content
 */
export interface GeneratedPipe {
  /** Pipe name */
  name: string;
  /** The generated .pipe file content */
  content: string;
}

/**
 * Check if SQL contains template parameters like {{...}}
 */
function hasDynamicParameters(sql: string): boolean {
  return /\{\{[^}]+\}\}/.test(sql);
}

/**
 * Generate a NODE section for the pipe
 */
function generateNode(node: NodeDefinition): string {
  const parts: string[] = [];

  parts.push(`NODE ${node._name}`);

  if (node.description) {
    parts.push(`DESCRIPTION >`);
    parts.push(`    ${node.description}`);
  }

  parts.push(`SQL >`);

  // Check if SQL has dynamic parameters - if so, add % on its own line
  const isDynamic = hasDynamicParameters(node.sql);
  if (isDynamic) {
    parts.push(`    %`);
  }

  const sqlLines = node.sql.trim().split("\n");
  sqlLines.forEach((line) => {
    parts.push(`    ${line}`);
  });

  return parts.join("\n");
}

/**
 * Generate the TYPE endpoint section
 */
function generateEndpoint(endpoint: EndpointConfig): string {
  const parts: string[] = ["TYPE endpoint"];

  if (endpoint.cache?.enabled) {
    if (endpoint.cache.ttl !== undefined) {
      parts.push(`CACHE ${endpoint.cache.ttl}`);
    } else {
      parts.push("CACHE 60"); // Default cache TTL
    }
  }

  return parts.join("\n");
}

/**
 * Generate a .pipe file content from a PipeDefinition
 *
 * @param pipe - The pipe definition
 * @returns Generated pipe content
 *
 * @example
 * ```ts
 * const topEvents = definePipe('top_events', {
 *   description: 'Get top events by count',
 *   params: {
 *     start_date: p.dateTime(),
 *     limit: p.int32().optional(10),
 *   },
 *   nodes: [
 *     node({
 *       name: 'endpoint',
 *       sql: `
 *         SELECT event_type, count() as count
 *         FROM events
 *         WHERE timestamp >= {{DateTime(start_date)}}
 *         ORDER BY count DESC
 *         LIMIT {{Int32(limit, 10)}}
 *       `,
 *     }),
 *   ],
 *   output: {
 *     event_type: t.string(),
 *     count: t.uint64(),
 *   },
 *   endpoint: true,
 * });
 *
 * const { content } = generatePipe(topEvents);
 * // Returns:
 * // DESCRIPTION >
 * //     Get top events by count
 * //
 * // NODE endpoint
 * // SQL >
 * //     SELECT event_type, count() as count
 * //     FROM events
 * //     WHERE timestamp >= {{DateTime(start_date)}}
 * //     ORDER BY count DESC
 * //     LIMIT {{Int32(limit, 10)}}
 * //
 * // TYPE endpoint
 * ```
 */
export function generatePipe(pipe: PipeDefinition): GeneratedPipe {
  const parts: string[] = [];

  // Add description if present
  if (pipe.options.description) {
    parts.push(`DESCRIPTION >\n    ${pipe.options.description}`);
    parts.push("");
  }

  // Add all nodes
  pipe.options.nodes.forEach((node, index) => {
    parts.push(generateNode(node));
    // Add empty line between nodes
    if (index < pipe.options.nodes.length - 1) {
      parts.push("");
    }
  });

  // Add endpoint configuration if this is an endpoint
  const endpointConfig = getEndpointConfig(pipe);
  if (endpointConfig) {
    parts.push("");
    parts.push(generateEndpoint(endpointConfig));
  }

  return {
    name: pipe._name,
    content: parts.join("\n"),
  };
}

/**
 * Generate .pipe files for all pipes in a project
 *
 * @param pipes - Record of pipe definitions
 * @returns Array of generated pipe content
 */
export function generateAllPipes(
  pipes: Record<string, PipeDefinition>
): GeneratedPipe[] {
  return Object.values(pipes).map(generatePipe);
}
