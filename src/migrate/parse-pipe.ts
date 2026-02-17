import type { PipeModel, PipeParamModel, PipeTokenModel, ResourceFile } from "./types.js";
import {
  MigrationParseError,
  isBlank,
  parseDirectiveLine,
  parseQuotedValue,
  splitLines,
  splitTopLevelComma,
  stripIndent,
} from "./parser-utils.js";

interface BlockReadResult {
  lines: string[];
  nextIndex: number;
}

function readIndentedBlock(lines: string[], startIndex: number): BlockReadResult {
  const collected: string[] = [];
  let i = startIndex;

  while (i < lines.length) {
    const line = lines[i] ?? "";
    if (line.startsWith("    ")) {
      collected.push(stripIndent(line));
      i += 1;
      continue;
    }

    if (isBlank(line)) {
      let j = i + 1;
      while (j < lines.length && isBlank(lines[j] ?? "")) {
        j += 1;
      }
      if (j < lines.length && (lines[j] ?? "").startsWith("    ")) {
        collected.push("");
        i += 1;
        continue;
      }
    }

    break;
  }

  return { lines: collected, nextIndex: i };
}

function nextNonBlank(lines: string[], startIndex: number): number {
  let i = startIndex;
  while (i < lines.length && isBlank(lines[i] ?? "")) {
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

function parseParamDefault(rawValue: string): string | number {
  const trimmed = rawValue.trim();
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    return Number(trimmed);
  }
  if (
    (trimmed.startsWith("'") && trimmed.endsWith("'")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    return trimmed.slice(1, -1);
  }
  throw new Error(`Unsupported parameter default value: "${rawValue}"`);
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

    let defaultValue: string | number | undefined;
    if (args.length > 1) {
      try {
        defaultValue = parseParamDefault(args[1] ?? "");
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
        existing.required = false;
      }
    } else {
      params.set(paramName, {
        name: paramName,
        type: mappedType,
        required: defaultValue === undefined,
        defaultValue,
      });
    }

    match = regex.exec(sql);
  }

  return Array.from(params.values()).sort((a, b) => a.name.localeCompare(b.name));
}

function parseToken(filePath: string, resourceName: string, value: string): PipeTokenModel {
  const parts = value.split(/\s+/).filter(Boolean);
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

  const tokenName = parts[0];
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
  let exportService: "kafka" | "s3" | undefined;
  let exportConnectionName: string | undefined;
  let exportTopic: string | undefined;
  let exportBucketUri: string | undefined;
  let exportFileTemplate: string | undefined;
  let exportFormat: string | undefined;
  let exportSchedule: string | undefined;
  let exportStrategy: "append" | "replace" | undefined;

  let i = 0;
  while (i < lines.length) {
    const line = (lines[i] ?? "").trim();
    if (!line) {
      i += 1;
      continue;
    }

    if (line === "DESCRIPTION >") {
      const block = readIndentedBlock(lines, i + 1);
      if (block.lines.length === 0) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "DESCRIPTION block is empty."
        );
      }

      if (!description) {
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
        const descriptionBlock = readIndentedBlock(lines, i + 1);
        if (descriptionBlock.lines.length === 0) {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Node "${nodeName}" has an empty DESCRIPTION block.`
          );
        }
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
      const sqlBlock = readIndentedBlock(lines, i + 1);
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
      case "EXPORT_TOPIC":
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
        const normalized = parseQuotedValue(value).toLowerCase();
        if (normalized !== "append" && normalized !== "replace") {
          throw new MigrationParseError(
            resource.filePath,
            "pipe",
            resource.name,
            `Unsupported EXPORT_STRATEGY in strict mode: "${value}"`
          );
        }
        exportStrategy = normalized;
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
    exportStrategy !== undefined;

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
      exportFormat !== undefined;

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
          "EXPORT_TOPIC is required for Kafka sinks."
        );
      }

      sink = {
        service: "kafka",
        connectionName: exportConnectionName,
        topic: exportTopic,
        schedule: exportSchedule,
        strategy: exportStrategy,
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
      if (!exportBucketUri || !exportFileTemplate) {
        throw new MigrationParseError(
          resource.filePath,
          "pipe",
          resource.name,
          "S3 sinks require EXPORT_BUCKET_URI and EXPORT_FILE_TEMPLATE."
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
      };
    }
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
    sink,
    tokens,
    params,
    inferredOutputColumns,
  };
}
