import type { ResourceKind } from "./types.js";

export class MigrationParseError extends Error {
  constructor(
    public readonly filePath: string,
    public readonly resourceKind: ResourceKind,
    public readonly resourceName: string,
    message: string
  ) {
    super(message);
    this.name = "MigrationParseError";
  }
}

export function splitLines(content: string): string[] {
  return content.replace(/\r\n/g, "\n").split("\n");
}

export function isBlank(line: string): boolean {
  return line.trim().length === 0;
}

export function stripIndent(line: string): string {
  if (line.startsWith("    ")) {
    return line.slice(4);
  }
  return line.trimStart();
}

export interface BlockReadResult {
  lines: string[];
  nextIndex: number;
}

export function readDirectiveBlock(
  lines: string[],
  startIndex: number,
  isDirectiveLine: (line: string) => boolean
): BlockReadResult {
  const collected: string[] = [];
  let i = startIndex;

  while (i < lines.length) {
    const line = (lines[i] ?? "").trim();
    if (isDirectiveLine(line)) {
      break;
    }
    collected.push(line);
    i += 1;
  }

  let first = 0;
  while (first < collected.length && collected[first] === "") {
    first += 1;
  }

  let last = collected.length - 1;
  while (last >= first && collected[last] === "") {
    last -= 1;
  }

  const normalized = first <= last ? collected.slice(first, last + 1) : [];
  return { lines: normalized, nextIndex: i };
}

export function splitCommaSeparated(input: string): string[] {
  return input
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

export function parseQuotedValue(input: string): string {
  const trimmed = input.trim();
  if (trimmed.startsWith('"') && trimmed.endsWith('"') && trimmed.length >= 2) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

export function parseLiteralFromDatafile(
  value: string
): string | number | boolean | null | Record<string, unknown> | unknown[] {
  const trimmed = value.trim();

  if (trimmed === "NULL") {
    return null;
  }

  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    return Number(trimmed);
  }

  if (trimmed === "1") {
    return true;
  }

  if (trimmed === "0") {
    return false;
  }

  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1).replace(/\\'/g, "'");
  }

  if (
    (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"))
  ) {
    return JSON.parse(trimmed) as Record<string, unknown> | unknown[];
  }

  throw new Error(`Unsupported literal value: ${value}`);
}

export function toTsLiteral(
  value: string | number | boolean | null | Record<string, unknown> | unknown[]
): string {
  if (value === null) {
    return "null";
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (typeof value === "string") {
    return JSON.stringify(value);
  }
  return JSON.stringify(value);
}

export function parseDirectiveLine(line: string): { key: string; value: string } {
  const firstSpace = line.indexOf(" ");
  if (firstSpace === -1) {
    return { key: line.trim(), value: "" };
  }
  return {
    key: line.slice(0, firstSpace).trim(),
    value: line.slice(firstSpace + 1).trim(),
  };
}

export function splitTopLevelComma(input: string): string[] {
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
      if (char === ")") {
        depth -= 1;
        current += char;
        continue;
      }
      if (char === "," && depth === 0) {
        const trimmed = current.trim();
        if (trimmed.length > 0) {
          parts.push(trimmed);
        }
        current = "";
        continue;
      }
    }

    current += char;
  }

  const trimmed = current.trim();
  if (trimmed.length > 0) {
    parts.push(trimmed);
  }

  return parts;
}
