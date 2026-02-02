/**
 * Utility functions for code generation
 */

/**
 * Convert a string to camelCase
 * Handles snake_case and kebab-case
 */
export function toCamelCase(str: string): string {
  // Handle reserved keywords
  const reserved = new Set([
    "break", "case", "catch", "class", "const", "continue", "debugger",
    "default", "delete", "do", "else", "enum", "export", "extends",
    "false", "finally", "for", "function", "if", "import", "in",
    "instanceof", "new", "null", "return", "super", "switch", "this",
    "throw", "true", "try", "typeof", "undefined", "var", "void",
    "while", "with", "yield", "let", "static", "implements", "interface",
    "package", "private", "protected", "public", "await", "async",
  ]);

  const result = str
    .replace(/[-_](.)/g, (_, char) => char.toUpperCase())
    .replace(/^[A-Z]/, (char) => char.toLowerCase());

  // If the result is a reserved keyword or starts with a number, prefix with underscore
  if (reserved.has(result) || /^\d/.test(result)) {
    return `_${result}`;
  }

  return result;
}

/**
 * Convert a string to PascalCase
 * Handles snake_case and kebab-case
 */
export function toPascalCase(str: string): string {
  const camel = toCamelCase(str);
  return camel.charAt(0).toUpperCase() + camel.slice(1);
}

/**
 * Escape a string for use in JavaScript/TypeScript code
 */
export function escapeString(str: string): string {
  return str
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r")
    .replace(/\t/g, "\\t");
}

/**
 * Parse a sorting key string into an array
 * Handles comma-separated values and quoted identifiers
 */
export function parseSortingKey(sortingKey?: string): string[] {
  if (!sortingKey) {
    return [];
  }

  // Split by comma, trim whitespace
  return sortingKey
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

/**
 * Generate engine code from engine info
 */
export function generateEngineCode(engine: {
  type: string;
  sorting_key?: string;
  partition_key?: string;
  primary_key?: string;
  ttl?: string;
  ver?: string;
  sign?: string;
  version?: string;
  summing_columns?: string;
}): string {
  const sortingKey = parseSortingKey(engine.sorting_key);

  // Build options object
  const options: string[] = [];

  // Sorting key is required for all MergeTree engines
  if (sortingKey.length === 1) {
    options.push(`sortingKey: "${sortingKey[0]}"`);
  } else if (sortingKey.length > 1) {
    options.push(`sortingKey: [${sortingKey.map((k) => `"${k}"`).join(", ")}]`);
  }

  // Optional fields
  if (engine.partition_key) {
    options.push(`partitionKey: "${escapeString(engine.partition_key)}"`);
  }

  if (engine.primary_key && engine.primary_key !== engine.sorting_key) {
    const primaryKey = parseSortingKey(engine.primary_key);
    if (primaryKey.length === 1) {
      options.push(`primaryKey: "${primaryKey[0]}"`);
    } else if (primaryKey.length > 1) {
      options.push(`primaryKey: [${primaryKey.map((k) => `"${k}"`).join(", ")}]`);
    }
  }

  if (engine.ttl) {
    options.push(`ttl: "${escapeString(engine.ttl)}"`);
  }

  // Engine-specific options
  if (engine.type === "ReplacingMergeTree" && engine.ver) {
    options.push(`ver: "${engine.ver}"`);
  }

  if (
    (engine.type === "CollapsingMergeTree" ||
      engine.type === "VersionedCollapsingMergeTree") &&
    engine.sign
  ) {
    options.push(`sign: "${engine.sign}"`);
  }

  if (engine.type === "VersionedCollapsingMergeTree" && engine.version) {
    options.push(`version: "${engine.version}"`);
  }

  if (engine.type === "SummingMergeTree" && engine.summing_columns) {
    const columns = parseSortingKey(engine.summing_columns);
    if (columns.length > 0) {
      options.push(`columns: [${columns.map((c) => `"${c}"`).join(", ")}]`);
    }
  }

  // Map engine type to function name
  const engineFunctionMap: Record<string, string> = {
    MergeTree: "engine.mergeTree",
    ReplacingMergeTree: "engine.replacingMergeTree",
    SummingMergeTree: "engine.summingMergeTree",
    AggregatingMergeTree: "engine.aggregatingMergeTree",
    CollapsingMergeTree: "engine.collapsingMergeTree",
    VersionedCollapsingMergeTree: "engine.versionedCollapsingMergeTree",
  };

  const engineFunc = engineFunctionMap[engine.type] ?? "engine.mergeTree";

  if (options.length === 0) {
    return `${engineFunc}({ sortingKey: [] })`;
  }

  return `${engineFunc}({\n    ${options.join(",\n    ")},\n  })`;
}

/**
 * Indent a multi-line string
 */
export function indent(str: string, spaces: number): string {
  const prefix = " ".repeat(spaces);
  return str
    .split("\n")
    .map((line) => (line.trim() ? prefix + line : line))
    .join("\n");
}

/**
 * Format SQL for inclusion in template literal
 * Preserves newlines and indentation but escapes backticks
 */
export function formatSqlForTemplate(sql: string): string {
  return sql.replace(/`/g, "\\`").replace(/\${/g, "\\${");
}
