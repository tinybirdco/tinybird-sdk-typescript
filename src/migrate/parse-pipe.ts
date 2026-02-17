import type { PipeModel, PipeParamModel, PipeTokenModel, ResourceFile } from "./types.js";
import {
  MigrationParseError,
  isBlank,
  parseDirectiveLine,
  readDirectiveBlock,
  splitLines,
  splitTopLevelComma,
} from "./parser-utils.js";

const PIPE_DIRECTIVES = new Set([
  "DESCRIPTION",
  "NODE",
  "SQL",
  "TYPE",
  "CACHE",
  "DATASOURCE",
  "DEPLOYMENT_METHOD",
  "TARGET_DATASOURCE",
  "COPY_SCHEDULE",
  "COPY_MODE",
  "TOKEN",
]);

function isPipeDirectiveLine(line: string): boolean {
  if (!line) {
    return false;
  }
  const { key } = parseDirectiveLine(line);
  return PIPE_DIRECTIVES.has(key);
}

function nextNonBlank(lines: string[], startIndex: number): number {
  let i = startIndex;
  while (
    i < lines.length &&
    (isBlank(lines[i] ?? "") || (lines[i] ?? "").trim().startsWith("#"))
  ) {
    i += 1;
  }
  return i;
}

function inferOutputColumnsFromSql(sql: string): string[] {
  const match = sql.match(/select\s+([\s\S]+?)\s+from\s/iu);
  if (!match) {
    return ["result"];
  }

  const selectClause = match[1] ?? "";
  const expressions = splitTopLevelComma(selectClause);
  const columns: string[] = [];

  for (const expression of expressions) {
    const aliasMatch = expression.match(/\s+AS\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s*$/iu);
    if (aliasMatch?.[1]) {
      columns.push(aliasMatch[1]);
      continue;
    }

    const simpleMatch = expression.match(/(?:^|\.)`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s*$/u);
    if (simpleMatch?.[1]) {
      columns.push(simpleMatch[1]);
      continue;
    }
  }

  return Array.from(new Set(columns.length > 0 ? columns : ["result"]));
}

function mapTemplateFunctionToParamType(func: string): string | null {
  const known = new Set([
    "String",
    "UUID",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Float32",
    "Float64",
    "Boolean",
    "Bool",
    "Date",
    "DateTime",
    "DateTime64",
    "Array",
  ]);

  if (known.has(func)) {
    return func;
  }

  if (func.startsWith("DateTime64")) {
    return "DateTime64";
  }
  if (func.startsWith("DateTime")) {
    return "DateTime";
  }

  return null;
}

function parseParamDefault(rawValue: string): string | number | boolean {
  const trimmed = rawValue.trim();
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    return Number(trimmed);
  }
  if (/^(true|false)$/i.test(trimmed)) {
    return trimmed.toLowerCase() === "true";
  }
  if (
    (trimmed.startsWith("'") && trimmed.endsWith("'")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    return trimmed.slice(1, -1);
  }
  throw new Error(`Unsupported parameter default value: "${rawValue}"`);
}

function parseKeywordArgument(rawArg: string): { key: string; value: string } | null {
  const equalsIndex = rawArg.indexOf("=");
  if (equalsIndex <= 0) {
    return null;
  }

  const key = rawArg.slice(0, equalsIndex).trim();
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(key)) {
    return null;
  }

  const value = rawArg.slice(equalsIndex + 1).trim();
  if (!value) {
    return null;
  }

  return { key, value };
}

function parseRequiredFlag(rawValue: string): boolean {
  const normalized = rawValue.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") {
    return true;
  }
  if (normalized === "false" || normalized === "0") {
    return false;
  }
  throw new Error(`Unsupported required value: "${rawValue}"`);
}

function parseParamOptions(rawArgs: string[]): {
  defaultValue?: string | number | boolean;
  required?: boolean;
  description?: string;
} {
  let positionalDefault: string | number | boolean | undefined;
  let keywordDefault: string | number | boolean | undefined;
  let required: boolean | undefined;
  let description: string | undefined;

  for (const rawArg of rawArgs) {
    const trimmed = rawArg.trim();
    if (!trimmed) {
      continue;
    }

    const keyword = parseKeywordArgument(trimmed);
    if (!keyword) {
      if (positionalDefault === undefined) {
        positionalDefault = parseParamDefault(trimmed);
      }
      continue;
    }

    const keyLower = keyword.key.toLowerCase();
    if (keyLower === "default") {
      keywordDefault = parseParamDefault(keyword.value);
      continue;
    }
    if (keyLower === "required") {
      required = parseRequiredFlag(keyword.value);
      continue;
    }
    if (keyLower === "description") {
      const parsedDescription = parseParamDefault(keyword.value);
      if (typeof parsedDescription !== "string") {
        throw new Error(`Unsupported description value: "${keyword.value}"`);
      }
      description = parsedDescription;
      continue;
    }
  }

  let defaultValue = keywordDefault ?? positionalDefault;
  if (keywordDefault !== undefined && positionalDefault !== undefined) {
    if (keywordDefault !== positionalDefault) {
      throw new Error(
        `Parameter has conflicting positional and keyword defaults: "${positionalDefault}" and "${keywordDefault}".`
      );
    }
    defaultValue = positionalDefault;
  }

  return { defaultValue, required, description };
}

function inferParamsFromSql(
  sql: string,
  filePath: string,
  resourceName: string
): PipeParamModel[] {
  const regex = /\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\(([^{}]*)\)\s*\}\}/g;
  const params = new Map<string, PipeParamModel>();
  let match: RegExpExecArray | null = regex.exec(sql);

  while (match) {
    const templateFunction = match[1] ?? "";
    const argsRaw = match[2] ?? "";
    const args = splitTopLevelComma(argsRaw);
    if (args.length === 0) {
      throw new MigrationParseError(
        filePath,
        "pipe",
        resourceName,
        `Invalid template placeholder: "${match[0]}"`
      );
    }

    const paramName = args[0]?.trim();
    if (!paramName || !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(paramName)) {
      throw new MigrationParseError(
        filePath,
        "pipe",
        resourceName,
        `Unsupported parameter name in placeholder: "${match[0]}"`
      );
    }

    const mappedType = mapTemplateFunctionToParamType(templateFunction);
    if (!mappedType) {
      throw new MigrationParseError(
        filePath,
        "pipe",
        resourceName,
        `Unsupported placeholder function in strict mode: "${templateFunction}"`
      );
    }

    let defaultValue: string | number | boolean | undefined;
    let required: boolean | undefined;
    let description: string | undefined;
    if (args.length > 1) {
      try {
        const parsedOptions = parseParamOptions(args.slice(1));
        defaultValue = parsedOptions.defaultValue;
        required = parsedOptions.required;
        description = parsedOptions.description;
      } catch (error) {
        throw new MigrationParseError(
          filePath,
          "pipe",
          resourceName,
          (error as Error).message
        );
      }
    }

    const existing = params.get(paramName);
    if (existing) {
      if (existing.type !== mappedType) {
        throw new MigrationParseError(
          filePath,
          "pipe",
          resourceName,
          `Parameter "${paramName}" is used with multiple types: "${existing.type}" and "${mappedType}".`
        );
      }
      if (existing.defaultValue !== undefined && defaultValue !== undefined) {
        if (existing.defaultValue !== defaultValue) {
          throw new MigrationParseError(
            filePath,
            "pipe",
            resourceName,
            `Parameter "${paramName}" uses multiple defaults: "${existing.defaultValue}" and "${defaultValue}".`
          );
        }
      }
      if (existing.defaultValue === undefined && defaultValue !== undefined) {
        existing.defaultValue = defaultValue;
      }
      if (existing.description === undefined && description !== undefined) {
        existing.description = description;
      }
      const optionalInAnyUsage =
        existing.required === false || required === false || defaultValue !== undefined;
      existing.required = !optionalInAnyUsage;
    } else {
      const isRequired = required ?? defaultValue === undefined;
      params.set(paramName, {
        name: paramName,
        type: mappedType,
        required: isRequired,
        defaultValue,
        description,
      });
    }

    match = regex.exec(sql);
  }

  return Array.from(params.values()).sort((a, b) => a.name.localeCompare(b.name));
}

function parseToken(filePath: string, resourceName: string, value: string): PipeTokenModel {
  const trimmed = value.trim();
  const quotedMatch = trimmed.match(/^"([^"]+)"(?:\s+(READ))?$/);
  if (quotedMatch) {
    return { name: quotedMatch[1] ?? "", scope: "READ" };
  }

  const parts = trimmed.split(/\s+/).filter(Boolean);
  if (parts.length === 0) {
    throw new MigrationParseError(filePath, "pipe", resourceName, "Invalid TOKEN line.");
  }
  if (parts.length > 2) {
    throw new MigrationParseError(
      filePath,
      "pipe",
      resourceName,
      `Unsupported TOKEN syntax in strict mode: "${value}"`
    );
  }

  const rawTokenName = parts[0] ?? "";
  const tokenName =
    rawTokenName.startsWith('"') &&
    rawTokenName.endsWith('"') &&
    rawTokenName.length >= 2
      ? rawTokenName.slice(1, -1)
      : rawTokenName;
  const scope = parts[1] ?? "READ";
  if (scope !== "READ") {
    throw new MigrationParseError(
      filePath,
      "pipe",
      resourceName,
      `Unsupported pipe token scope: "${scope}"`
    );
  }

  return { name: tokenName, scope: "READ" };
}

export function parsePipeFile(resource: ResourceFile): PipeModel {
  const lines = splitLines(resource.content);
  const nodes: PipeModel["nodes"] = [];
  const tokens: PipeTokenModel[] = [];
  let description: string | undefined;
  let pipeType: PipeModel["type"] = "pipe";
  let cacheTtl: number | undefined;
  let materializedDatasource: string | undefined;
  let deploymentMethod: "alter" | undefined;
  let copyTargetDatasource: string | undefined;
  let copySchedule: string | undefined;
  let copyMode: "append" | "replace" | undefined;

  let i = 0;
  while (i < lines.length) {
    const line = (lines[i] ?? "").trim();
    if (!line || line.startsWith("#")) {
      i += 1;
      continue;
    }

    if (line === "DESCRIPTION >") {
      const block = readDirectiveBlock(lines, i + 1, isPipeDirectiveLine);
      if (description === undefined) {
        description = block.lines.join("\n");
      } else if (nodes.length > 0) {
        nodes[nodes.length - 1] = {
          ...nodes[nodes.length - 1]!,
          description: block.lines.join("\n"),
        };
      } else {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "DESCRIPTION block is not attached to a node or pipe header."
        );
      }
      i = block.nextIndex;
      continue;
    }

    if (line.startsWith("NODE ")) {
      const nodeName = line.slice("NODE ".length).trim();
      if (!nodeName) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "NODE directive requires a name."
        );
      }

      i += 1;
      i = nextNonBlank(lines, i);

      let nodeDescription: string | undefined;
      if ((lines[i] ?? "").trim() === "DESCRIPTION >") {
        const descriptionBlock = readDirectiveBlock(lines, i + 1, isPipeDirectiveLine);
        nodeDescription = descriptionBlock.lines.join("\n");
        i = descriptionBlock.nextIndex;
        i = nextNonBlank(lines, i);
      }

      if ((lines[i] ?? "").trim() !== "SQL >") {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          `Node "${nodeName}" is missing SQL > block.`
        );
      }
      const sqlBlock = readDirectiveBlock(lines, i + 1, isPipeDirectiveLine);
      if (sqlBlock.lines.length === 0) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          `Node "${nodeName}" has an empty SQL block.`
        );
      }

      const normalizedSqlLines =
        sqlBlock.lines[0] === "%" ? sqlBlock.lines.slice(1) : sqlBlock.lines;
      const sql = normalizedSqlLines.join("\n").trim();
      if (!sql) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          `Node "${nodeName}" has SQL marker '%' but no SQL body.`
        );
      }

      nodes.push({
        name: nodeName,
        description: nodeDescription,
        sql,
      });

      i = sqlBlock.nextIndex;
      continue;
    }

    const { key, value } = parseDirectiveLine(line);
    switch (key) {
      case "TYPE": {
        const normalizedType = value.toLowerCase();
        if (normalizedType === "endpoint") {
          pipeType = "endpoint";
        } else if (normalizedType === "materialized") {
          pipeType = "materialized";
        } else if (normalizedType === "copy") {
          pipeType = "copy";
        } else {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Unsupported TYPE value in strict mode: "${value}"`
          );
        }
        break;
      }
      case "CACHE": {
        const ttl = Number(value);
        if (!Number.isFinite(ttl) || ttl < 0) {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Invalid CACHE value: "${value}"`
          );
        }
        cacheTtl = ttl;
        break;
      }
      case "DATASOURCE":
        materializedDatasource = value.trim();
        break;
      case "DEPLOYMENT_METHOD":
        if (value !== "alter") {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Unsupported DEPLOYMENT_METHOD: "${value}"`
          );
        }
        deploymentMethod = "alter";
        break;
      case "TARGET_DATASOURCE":
        copyTargetDatasource = value.trim();
        break;
      case "COPY_SCHEDULE":
        copySchedule = value;
        break;
      case "COPY_MODE":
        if (value !== "append" && value !== "replace") {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Unsupported COPY_MODE: "${value}"`
          );
        }
        copyMode = value;
        break;
      case "TOKEN":
        tokens.push(parseToken(resource.filePath, resource.name, value));
        break;
      default:
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          `Unsupported pipe directive in strict mode: "${line}"`
        );
    }

    i += 1;
  }

  if (nodes.length === 0) {
    throw new MigrationParseError(
      resource.filePath,
      "pipe",
      resource.name,
      "At least one NODE is required."
    );
  }

  if (pipeType !== "endpoint" && cacheTtl !== undefined) {
    throw new MigrationParseError(
      resource.filePath,
      "pipe",
      resource.name,
      "CACHE is only supported for TYPE endpoint."
    );
  }

  if (pipeType === "materialized" && !materializedDatasource) {
    throw new MigrationParseError(
      resource.filePath,
      "pipe",
      resource.name,
      "DATASOURCE is required for TYPE MATERIALIZED."
    );
  }

  if (pipeType === "copy" && !copyTargetDatasource) {
    throw new MigrationParseError(
      resource.filePath,
      "pipe",
      resource.name,
      "TARGET_DATASOURCE is required for TYPE COPY."
    );
  }

  const params =
    pipeType === "materialized" || pipeType === "copy"
      ? []
      : inferParamsFromSql(
          nodes.map((node) => node.sql).join("\n"),
          resource.filePath,
          resource.name
        );

  const inferredOutputColumns =
    pipeType === "endpoint" ? inferOutputColumnsFromSql(nodes[nodes.length - 1]!.sql) : [];

  return {
    kind: "pipe",
    name: resource.name,
    filePath: resource.filePath,
    description,
    type: pipeType,
    nodes,
    cacheTtl,
    materializedDatasource,
    deploymentMethod,
    copyTargetDatasource,
    copySchedule,
    copyMode,
    tokens,
    params,
    inferredOutputColumns,
  };
}
