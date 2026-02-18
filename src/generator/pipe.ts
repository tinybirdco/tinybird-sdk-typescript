/**
 * Pipe content generator
 * Converts PipeDefinition to native .pipe file format
 */

import type {
  PipeDefinition,
  NodeDefinition,
  EndpointConfig,
  MaterializedConfig,
  CopyConfig,
  SinkConfig,
  PipeTokenConfig,
} from "../schema/pipe.js";
import {
  getEndpointConfig,
  getMaterializedConfig,
  getCopyConfig,
  getSinkConfig,
} from "../schema/pipe.js";
import type { AnyParamValidator } from "../schema/params.js";
import { getParamDefault } from "../schema/params.js";

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
 * Check if SQL contains Jinja template syntax like {{...}} or {%...%}
 */
function hasDynamicParameters(sql: string): boolean {
  return /\{\{[^}]+\}\}/.test(sql) || /\{%[^%]+%\}/.test(sql);
}

function splitTopLevelComma(input: string): string[] {
  const parts: string[] = [];
  let current = "";
  let depth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;

  for (let i = 0; i < input.length; i += 1) {
    const char = input[i];
    const prev = i > 0 ? input[i - 1] : "";

    if (char === "'" && !inDoubleQuote && prev !== "\\") {
      inSingleQuote = !inSingleQuote;
      current += char;
      continue;
    }

    if (char === '"' && !inSingleQuote && prev !== "\\") {
      inDoubleQuote = !inDoubleQuote;
      current += char;
      continue;
    }

    if (!inSingleQuote && !inDoubleQuote) {
      if (char === "(") {
        depth += 1;
        current += char;
        continue;
      }
      if (char === ")" && depth > 0) {
        depth -= 1;
        current += char;
        continue;
      }
      if (char === "," && depth === 0) {
        parts.push(current.trim());
        current = "";
        continue;
      }
    }

    current += char;
  }

  if (current.trim()) {
    parts.push(current.trim());
  }

  return parts;
}

function toTemplateDefaultLiteral(value: string | number | boolean): string {
  if (typeof value === "string") {
    return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
  }
  return String(value);
}

function applyParamDefaultsToSql(
  sql: string,
  params?: Record<string, AnyParamValidator>
): string {
  if (!params) {
    return sql;
  }

  const defaults = new Map<string, string>();
  for (const [name, validator] of Object.entries(params)) {
    const defaultValue = getParamDefault(validator);
    if (defaultValue !== undefined) {
      defaults.set(name, toTemplateDefaultLiteral(defaultValue as string | number | boolean));
    }
  }

  if (defaults.size === 0) {
    return sql;
  }

  const placeholderRegex = /\{\{\s*([^{}]+?)\s*\}\}/g;
  return sql.replace(placeholderRegex, (fullMatch, rawExpression) => {
    const expression = String(rawExpression);
    const rewritten = expression.replace(
      /([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^()]*)\)/g,
      (call, _functionName, rawArgs) => {
        const args = splitTopLevelComma(String(rawArgs));
        if (args.length !== 1) {
          return call;
        }

        const paramName = args[0]?.trim() ?? "";
        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(paramName)) {
          return call;
        }

        const defaultLiteral = defaults.get(paramName);
        if (!defaultLiteral) {
          return call;
        }

        return call.replace(/\)\s*$/, `, ${defaultLiteral})`);
      }
    );

    if (rewritten === expression) {
      return fullMatch;
    }
    return `{{ ${rewritten.trim()} }}`;
  });
}

/**
 * Generate a NODE section for the pipe
 */
function generateNode(
  node: NodeDefinition,
  params?: Record<string, AnyParamValidator>
): string {
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

  const sqlWithDefaults = applyParamDefaultsToSql(node.sql, params);
  const sqlLines = sqlWithDefaults.trim().split("\n");
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
 * Generate the TYPE MATERIALIZED section
 */
function generateMaterialized(config: MaterializedConfig): string {
  const parts: string[] = ["TYPE MATERIALIZED"];

  // The config is normalized by definePipe to always have `datasource` set.
  // Use non-null assertion since we know it's always present after normalization.
  const datasourceName = config.datasource!._name;
  parts.push(`DATASOURCE ${datasourceName}`);

  if (config.deploymentMethod === "alter") {
    parts.push("DEPLOYMENT_METHOD alter");
  }

  return parts.join("\n");
}

/**
 * Generate the TYPE COPY section
 */
function generateCopy(config: CopyConfig): string {
  const parts: string[] = ["TYPE COPY"];

  const datasourceName = config.datasource._name;
  parts.push(`TARGET_DATASOURCE ${datasourceName}`);

  if (config.copy_schedule) {
    parts.push(`COPY_SCHEDULE ${config.copy_schedule}`);
  }

  if (config.copy_mode) {
    parts.push(`COPY_MODE ${config.copy_mode}`);
  }

  return parts.join("\n");
}

/**
 * Generate the TYPE sink section
 */
function generateSink(config: SinkConfig): string {
  const parts: string[] = ["TYPE sink"];

  parts.push(`EXPORT_CONNECTION_NAME ${config.connection._name}`);

  if ("topic" in config) {
    parts.push(`EXPORT_KAFKA_TOPIC ${config.topic}`);
    parts.push(`EXPORT_SCHEDULE ${config.schedule}`);
  } else {
    parts.push(`EXPORT_BUCKET_URI ${config.bucketUri}`);
    parts.push(`EXPORT_FILE_TEMPLATE ${config.fileTemplate}`);
    parts.push(`EXPORT_SCHEDULE ${config.schedule}`);
    parts.push(`EXPORT_FORMAT ${config.format}`);
    if (config.strategy) {
      parts.push(`EXPORT_STRATEGY ${config.strategy}`);
    }
    if (config.compression) {
      parts.push(`EXPORT_COMPRESSION ${config.compression}`);
    }
  }

  return parts.join("\n");
}

/**
 * Generate TOKEN lines for a pipe
 */
function generateTokens(tokens?: readonly PipeTokenConfig[]): string[] {
  if (!tokens || tokens.length === 0) {
    return [];
  }

  return tokens.map((token) => {
    if ("token" in token) {
      // TokenReference
      return `TOKEN ${token.token._name} ${token.scope}`;
    }
    // Inline config - pipes default to READ
    return `TOKEN ${token.name} READ`;
  });
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
    parts.push(generateNode(node, pipe.options.params as Record<string, AnyParamValidator> | undefined));
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

  // Add materialized view configuration if this is a materialized view
  const materializedConfig = getMaterializedConfig(pipe);
  if (materializedConfig) {
    parts.push("");
    parts.push(generateMaterialized(materializedConfig));
  }

  // Add copy pipe configuration if this is a copy pipe
  const copyConfig = getCopyConfig(pipe);
  if (copyConfig) {
    parts.push("");
    parts.push(generateCopy(copyConfig));
  }

  // Add sink configuration if this is a sink pipe
  const sinkConfig = getSinkConfig(pipe);
  if (sinkConfig) {
    parts.push("");
    parts.push(generateSink(sinkConfig));
  }

  // Add tokens if present
  const tokenLines = generateTokens(pipe.options.tokens);
  if (tokenLines.length > 0) {
    parts.push("");
    parts.push(tokenLines.join("\n"));
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
