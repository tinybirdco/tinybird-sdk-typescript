import type { PipeModel, PipeParamModel, PipeTokenModel, ResourceFile } from "./types.js";
import {
  MigrationParseError,
  isBlank,
  parseDirectiveLine,
  parseQuotedValue,
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
  const lower = func.toLowerCase();
  const aliases: Record<string, string> = {
    string: "String",
    uuid: "UUID",
    int: "Int32",
    integer: "Int32",
    int8: "Int8",
    int16: "Int16",
    int32: "Int32",
    int64: "Int64",
    uint8: "UInt8",
    uint16: "UInt16",
    uint32: "UInt32",
    uint64: "UInt64",
    float32: "Float32",
    float64: "Float64",
    boolean: "Boolean",
    bool: "Boolean",
    date: "Date",
    datetime: "DateTime",
    datetime64: "DateTime64",
    array: "Array",
    column: "column",
    json: "JSON",
  };

  const mapped = aliases[lower];
  if (mapped) {
    return mapped;
  }

  if (lower.startsWith("datetime64")) {
    return "DateTime64";
  }
  if (lower.startsWith("datetime")) {
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
  let defaultValue: string | number | boolean | undefined;
  let required: boolean | undefined;
  let description: string | undefined;

  for (const rawArg of rawArgs) {
    const trimmed = rawArg.trim();
    if (!trimmed) {
      continue;
    }

    const keyword = parseKeywordArgument(trimmed);
    if (!keyword) {
      defaultValue = parseParamDefault(trimmed);
      continue;
    }

    const keyLower = keyword.key.toLowerCase();
    if (keyLower === "default") {
      defaultValue = parseParamDefault(keyword.value);
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

  return { defaultValue, required, description };
}

function extractTemplateFunctionCalls(expression: string): Array<{
  functionName: string;
  argsRaw: string;
  fullCall: string;
  start: number;
  end: number;
}> {
  const maskParenthesesInsideQuotes = (value: string): string => {
    let output = "";
    let inSingleQuote = false;
    let inDoubleQuote = false;

    for (let i = 0; i < value.length; i += 1) {
      const char = value[i] ?? "";
      const prev = i > 0 ? value[i - 1] ?? "" : "";

      if (char === "'" && !inDoubleQuote && prev !== "\\") {
        inSingleQuote = !inSingleQuote;
        output += char;
        continue;
      }
      if (char === '"' && !inSingleQuote && prev !== "\\") {
        inDoubleQuote = !inDoubleQuote;
        output += char;
        continue;
      }

      if ((inSingleQuote || inDoubleQuote) && (char === "(" || char === ")")) {
        output += " ";
        continue;
      }

      output += char;
    }

    return output;
  };

  const maskedExpression = maskParenthesesInsideQuotes(expression);
  const callRegex = /([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^()]*)\)/g;
  const calls: Array<{
    functionName: string;
    argsRaw: string;
    fullCall: string;
    start: number;
    end: number;
  }> = [];
  let match: RegExpExecArray | null = callRegex.exec(maskedExpression);
  while (match) {
    const start = match.index;
    const fullCall = expression.slice(start, start + (match[0]?.length ?? 0));
    const openParen = fullCall.indexOf("(");
    const closeParen = fullCall.lastIndexOf(")");

    calls.push({
      functionName: match[1] ?? "",
      argsRaw: openParen >= 0 && closeParen > openParen ? fullCall.slice(openParen + 1, closeParen) : "",
      fullCall,
      start,
      end: start + fullCall.length,
    });
    match = callRegex.exec(maskedExpression);
  }
  return calls;
}

function shouldParseTemplateFunctionAsParam(mappedType: string): boolean {
  return mappedType !== "Array";
}

function normalizeSqlPlaceholders(sql: string): string {
  const placeholderRegex = /\{\{\s*([^{}]+?)\s*\}\}/g;
  return sql.replace(placeholderRegex, (fullMatch, rawExpression) => {
    const expression = String(rawExpression);
    const calls = extractTemplateFunctionCalls(expression);
    if (calls.length === 0) {
      return fullMatch;
    }

    let rewritten = "";
    let cursor = 0;
    let changed = false;
    for (const call of calls) {
      rewritten += expression.slice(cursor, call.start);

      let replacement = call.fullCall;
      const normalizedFunction = String(call.functionName).toLowerCase();
      if (normalizedFunction !== "error" && normalizedFunction !== "custom_error") {
        const mappedType = mapTemplateFunctionToParamType(String(call.functionName));
        if (mappedType && shouldParseTemplateFunctionAsParam(mappedType)) {
          const args = splitTopLevelComma(String(call.argsRaw));
          if (args.length > 0) {
            const paramName = args[0]?.trim() ?? "";
            if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(paramName)) {
              replacement = `${String(call.functionName)}(${paramName})`;
            }
          }
        }
      }

      if (replacement !== call.fullCall) {
        changed = true;
      }
      rewritten += replacement;
      cursor = call.end;
    }
    rewritten += expression.slice(cursor);

    if (!changed) {
      return fullMatch;
    }
    return `{{ ${rewritten.trim()} }}`;
  });
}

function inferParamsFromSql(
  sql: string,
  filePath: string,
  resourceName: string
): PipeParamModel[] {
  const regex = /\{\{\s*([^{}]+?)\s*\}\}/g;
  const params = new Map<string, PipeParamModel>();
  let match: RegExpExecArray | null = regex.exec(sql);

  while (match) {
    const expression = match[1] ?? "";
    const calls = extractTemplateFunctionCalls(expression);

    for (const call of calls) {
      const templateFunction = call.functionName;
      const normalizedTemplateFunction = templateFunction.toLowerCase();
      if (normalizedTemplateFunction === "error" || normalizedTemplateFunction === "custom_error") {
        continue;
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

      const args = splitTopLevelComma(call.argsRaw);
      if (args.length === 0) {
        throw new MigrationParseError(
          filePath,
          "pipe",
          resourceName,
          `Invalid template placeholder: "${call.fullCall}"`
        );
      }

      const paramName = args[0]?.trim() ?? "";
      const isIdentifier = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(paramName);
      if (!isIdentifier) {
        if (mappedType === "column") {
          continue;
        }
        throw new MigrationParseError(
          filePath,
          "pipe",
          resourceName,
          `Unsupported parameter name in placeholder: "{{ ${call.fullCall} }}"`
        );
      }

      let defaultValue: string | number | boolean | undefined;
      let required: boolean | undefined;
      let description: string | undefined;
      if (args.length > 1 && shouldParseTemplateFunctionAsParam(mappedType)) {
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
          // Keep the last explicit type seen in SQL.
          existing.type = mappedType;
        }

        // Match backend merge semantics: prefer the latest truthy value.
        if (defaultValue !== undefined || existing.defaultValue !== undefined) {
          existing.defaultValue =
            (defaultValue as string | number | boolean | undefined) || existing.defaultValue;
        }
        if (description !== undefined || existing.description !== undefined) {
          existing.description = description || existing.description;
        }
        const optionalInAnyUsage =
          existing.required === false ||
          required === false ||
          existing.defaultValue !== undefined ||
          defaultValue !== undefined;
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

function normalizeExportStrategy(rawValue: string): "create_new" | "replace" {
  const normalized = parseQuotedValue(rawValue).toLowerCase();
  if (normalized === "create_new") {
    return "create_new";
  }
  if (normalized === "replace" || normalized === "truncate") {
    return "replace";
  }
  throw new Error(`Unsupported sink strategy in strict mode: "${rawValue}"`);
}

export function parsePipeFile(resource: ResourceFile): PipeModel {
  const lines = splitLines(resource.content);
  const nodes: PipeModel["nodes"] = [];
  const rawNodeSqls: string[] = [];
  const tokens: PipeTokenModel[] = [];
  let description: string | undefined;
  let pipeType: PipeModel["type"] = "pipe";
  let cacheTtl: number | undefined;
  let materializedDatasource: string | undefined;
  let deploymentMethod: "alter" | undefined;
  let copyTargetDatasource: string | undefined;
  let copySchedule: string | undefined;
  let copyMode: "append" | "replace" | undefined;
  let exportService: "kafka" | "s3" | undefined;
  let exportConnectionName: string | undefined;
  let exportTopic: string | undefined;
  let exportBucketUri: string | undefined;
  let exportFileTemplate: string | undefined;
  let exportFormat: string | undefined;
  let exportSchedule: string | undefined;
  let exportStrategy: "create_new" | "replace" | undefined;
  let exportCompression: "none" | "gzip" | "snappy" | undefined;

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

      rawNodeSqls.push(sql);
      nodes.push({
        name: nodeName,
        description: nodeDescription,
        sql: normalizeSqlPlaceholders(sql),
      });

      i = sqlBlock.nextIndex;
      continue;
    }

    const { key, value } = parseDirectiveLine(line);
    switch (key) {
      case "TYPE": {
        const normalizedType = parseQuotedValue(value).toLowerCase();
        if (normalizedType === "endpoint") {
          pipeType = "endpoint";
        } else if (normalizedType === "materialized") {
          pipeType = "materialized";
        } else if (normalizedType === "copy") {
          pipeType = "copy";
        } else if (normalizedType === "sink") {
          pipeType = "sink";
        } else {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Unsupported TYPE value in strict mode: "${parseQuotedValue(value)}"`
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
      case "EXPORT_SERVICE": {
        const normalized = parseQuotedValue(value).toLowerCase();
        if (normalized !== "kafka" && normalized !== "s3") {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Unsupported EXPORT_SERVICE in strict mode: "${value}"`
          );
        }
        exportService = normalized;
        break;
      }
      case "EXPORT_CONNECTION_NAME":
        exportConnectionName = parseQuotedValue(value);
        break;
      case "EXPORT_KAFKA_TOPIC":
        exportTopic = parseQuotedValue(value);
        break;
      case "EXPORT_BUCKET_URI":
        exportBucketUri = parseQuotedValue(value);
        break;
      case "EXPORT_FILE_TEMPLATE":
        exportFileTemplate = parseQuotedValue(value);
        break;
      case "EXPORT_FORMAT":
        exportFormat = parseQuotedValue(value);
        break;
      case "EXPORT_SCHEDULE":
        exportSchedule = parseQuotedValue(value);
        break;
      case "EXPORT_STRATEGY": {
        try {
          exportStrategy = normalizeExportStrategy(value);
        } catch {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Unsupported EXPORT_STRATEGY in strict mode: "${value}"`
          );
        }
        break;
      }
      case "EXPORT_WRITE_STRATEGY": {
        try {
          exportStrategy = normalizeExportStrategy(value);
        } catch {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Unsupported EXPORT_WRITE_STRATEGY in strict mode: "${value}"`
          );
        }
        break;
      }
      case "EXPORT_COMPRESSION": {
        const normalized = parseQuotedValue(value).toLowerCase();
        if (normalized !== "none" && normalized !== "gzip" && normalized !== "snappy") {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Unsupported EXPORT_COMPRESSION in strict mode: "${value}"`
          );
        }
        exportCompression = normalized;
        break;
      }
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

  const hasSinkDirectives =
    exportService !== undefined ||
    exportConnectionName !== undefined ||
    exportTopic !== undefined ||
    exportBucketUri !== undefined ||
    exportFileTemplate !== undefined ||
    exportFormat !== undefined ||
    exportSchedule !== undefined ||
    exportStrategy !== undefined ||
    exportCompression !== undefined;

  if (pipeType !== "sink" && hasSinkDirectives) {
    throw new MigrationParseError(
      resource.filePath,
      "pipe",
      resource.name,
      "EXPORT_* directives are only supported for TYPE sink."
    );
  }

  let sink: PipeModel["sink"];
  if (pipeType === "sink") {
    if (!exportConnectionName) {
      throw new MigrationParseError(
        resource.filePath,
        "pipe",
        resource.name,
        "EXPORT_CONNECTION_NAME is required for TYPE sink."
      );
    }

    const hasKafkaDirectives = exportTopic !== undefined;
    const hasS3Directives =
      exportBucketUri !== undefined ||
      exportFileTemplate !== undefined ||
      exportFormat !== undefined ||
      exportCompression !== undefined;

    if (hasKafkaDirectives && hasS3Directives) {
      throw new MigrationParseError(
        resource.filePath,
        "pipe",
        resource.name,
        "Sink pipe cannot mix Kafka and S3 export directives."
      );
    }

    const inferredService =
      exportService ?? (hasKafkaDirectives ? "kafka" : hasS3Directives ? "s3" : undefined);

    if (!inferredService) {
      throw new MigrationParseError(
        resource.filePath,
        "pipe",
        resource.name,
        "Sink pipe must define EXPORT_SERVICE or include service-specific export directives."
      );
    }

    if (inferredService === "kafka") {
      if (hasS3Directives) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "S3 export directives are not valid for Kafka sinks."
        );
      }
      if (!exportTopic) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "EXPORT_KAFKA_TOPIC is required for Kafka sinks."
        );
      }
      if (!exportSchedule) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "EXPORT_SCHEDULE is required for Kafka sinks."
        );
      }
      if (exportStrategy !== undefined) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "EXPORT_STRATEGY is only valid for S3 sinks."
        );
      }
      if (exportCompression !== undefined) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "EXPORT_COMPRESSION is only valid for S3 sinks."
        );
      }

      sink = {
        service: "kafka",
        connectionName: exportConnectionName,
        topic: exportTopic,
        schedule: exportSchedule,
      };
    } else {
      if (hasKafkaDirectives) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "Kafka export directives are not valid for S3 sinks."
        );
      }
      if (!exportBucketUri || !exportFileTemplate || !exportFormat || !exportSchedule) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "S3 sinks require EXPORT_BUCKET_URI, EXPORT_FILE_TEMPLATE, EXPORT_FORMAT, and EXPORT_SCHEDULE."
        );
      }

      sink = {
        service: "s3",
        connectionName: exportConnectionName,
        bucketUri: exportBucketUri,
        fileTemplate: exportFileTemplate,
        format: exportFormat,
        schedule: exportSchedule,
        strategy: exportStrategy,
        compression: exportCompression,
      };
    }
  }

  const params =
    pipeType === "materialized" || pipeType === "copy"
      ? []
      : inferParamsFromSql(
          rawNodeSqls.join("\n"),
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
    sink,
    tokens,
    params,
    inferredOutputColumns,
  };
}
