/**
 * Datasource content generator
 * Converts DatasourceDefinition to native .datasource file format
 */

import type { DatasourceDefinition, SchemaDefinition, ColumnDefinition } from "../schema/datasource.js";
import type { AnyTypeValidator, TypeModifiers } from "../schema/types.js";
import { getColumnType, getColumnJsonPath } from "../schema/datasource.js";
import { getEngineClause, type EngineConfig } from "../schema/engines.js";

/**
 * Generated datasource content
 */
export interface GeneratedDatasource {
  /** Datasource name */
  name: string;
  /** The generated .datasource file content */
  content: string;
}

/**
 * Get the Tinybird type string from a type validator
 * Handles the internal structure of validators
 */
function getTinybirdTypeFromValidator(validator: AnyTypeValidator): string {
  // The validator has _tinybirdType as the type string
  return validator._tinybirdType;
}

/**
 * Get modifiers from a validator
 */
function getModifiersFromValidator(validator: AnyTypeValidator): TypeModifiers {
  return validator._modifiers;
}

/**
 * Format a default value for the datasource file
 */
function formatDefaultValue(value: unknown, tinybirdType: string): string {
  if (value === null) {
    return "NULL";
  }

  if (typeof value === "string") {
    // Escape single quotes
    return `'${value.replace(/'/g, "\\'")}'`;
  }

  if (typeof value === "number" || typeof value === "bigint") {
    return String(value);
  }

  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }

  if (value instanceof Date) {
    // Format based on type
    if (tinybirdType.startsWith("Date") && !tinybirdType.includes("Time")) {
      return `'${value.toISOString().split("T")[0]}'`;
    }
    return `'${value.toISOString().replace("T", " ").slice(0, 19)}'`;
  }

  // For arrays and objects, use raw JSON (no quotes)
  if (Array.isArray(value)) {
    return JSON.stringify(value);
  }

  if (typeof value === "object" && value !== null) {
    return JSON.stringify(value);
  }

  // Fallback for other types - stringify as string literal
  return `'${String(value).replace(/'/g, "\\'")}'`;
}

/**
 * Generate a column definition line for the schema
 */
function generateColumnLine(
  columnName: string,
  column: AnyTypeValidator | ColumnDefinition
): string {
  const validator = getColumnType(column);
  const jsonPath = getColumnJsonPath(column);
  const tinybirdType = getTinybirdTypeFromValidator(validator);
  const modifiers = getModifiersFromValidator(validator);

  const parts: string[] = [`    ${columnName} ${tinybirdType}`];

  // Add JSON path if defined
  if (jsonPath) {
    parts.push(`\`json:${jsonPath}\``);
  }

  // Add default value if defined
  if (modifiers.hasDefault && modifiers.defaultValue !== undefined) {
    const defaultStr = formatDefaultValue(modifiers.defaultValue, tinybirdType);
    parts.push(`DEFAULT ${defaultStr}`);
  }

  // Add codec if defined
  if (modifiers.codec) {
    parts.push(`CODEC(${modifiers.codec})`);
  }

  return parts.join(" ");
}

/**
 * Generate the SCHEMA section
 */
function generateSchema(schema: SchemaDefinition): string {
  const lines = ["SCHEMA >"];

  const columnNames = Object.keys(schema);
  columnNames.forEach((name, index) => {
    const column = schema[name];
    const line = generateColumnLine(name, column);
    // Add comma if not the last column
    const suffix = index < columnNames.length - 1 ? "," : "";
    lines.push(line + suffix);
  });

  return lines.join("\n");
}

/**
 * Generate the engine configuration
 * Uses the helper from engines.ts if an engine is provided
 */
function generateEngineConfig(engine?: EngineConfig): string {
  if (!engine) {
    // Default to MergeTree with first column as sorting key
    return 'ENGINE "MergeTree"';
  }

  return getEngineClause(engine);
}

/**
 * Generate a .datasource file content from a DatasourceDefinition
 *
 * @param datasource - The datasource definition
 * @returns Generated datasource content
 *
 * @example
 * ```ts
 * const events = defineDatasource('events', {
 *   description: 'User events',
 *   schema: {
 *     timestamp: t.dateTime(),
 *     user_id: t.string(),
 *     event: t.string(),
 *   },
 *   engine: engine.mergeTree({
 *     sortingKey: ['user_id', 'timestamp'],
 *   }),
 * });
 *
 * const { content } = generateDatasource(events);
 * // Returns:
 * // DESCRIPTION >
 * //     User events
 * //
 * // SCHEMA >
 * //     timestamp DateTime,
 * //     user_id String,
 * //     event String
 * //
 * // ENGINE "MergeTree"
 * // ENGINE_SORTING_KEY "user_id, timestamp"
 * ```
 */
export function generateDatasource(
  datasource: DatasourceDefinition
): GeneratedDatasource {
  const parts: string[] = [];

  // Add description if present
  if (datasource.options.description) {
    parts.push(`DESCRIPTION >\n    ${datasource.options.description}`);
    parts.push("");
  }

  // Add schema
  parts.push(generateSchema(datasource._schema));
  parts.push("");

  // Add engine configuration
  parts.push(generateEngineConfig(datasource.options.engine));

  return {
    name: datasource._name,
    content: parts.join("\n"),
  };
}

/**
 * Generate .datasource files for all datasources in a project
 *
 * @param datasources - Record of datasource definitions
 * @returns Array of generated datasource content
 */
export function generateAllDatasources(
  datasources: Record<string, DatasourceDefinition>
): GeneratedDatasource[] {
  return Object.values(datasources).map(generateDatasource);
}
