import { clickhouseTypeToValidator } from "../codegen/type-mapper.js";
import { toCamelCase } from "../codegen/utils.js";
import { parseLiteralFromDatafile, toTsLiteral } from "./parser-utils.js";
import type {
  DatasourceModel,
  KafkaConnectionModel,
  ParsedResource,
  PipeModel,
  S3ConnectionModel,
} from "./types.js";

function escapeString(value: string): string {
  return JSON.stringify(value);
}

function normalizedBaseType(type: string): string {
  let current = type.trim();
  let updated = true;
  while (updated) {
    updated = false;
    const nullable = current.match(/^Nullable\((.+)\)$/);
    if (nullable?.[1]) {
      current = nullable[1];
      updated = true;
      continue;
    }
    const lowCard = current.match(/^LowCardinality\((.+)\)$/);
    if (lowCard?.[1]) {
      current = lowCard[1];
      updated = true;
      continue;
    }
  }
  return current;
}

function isBooleanType(type: string): boolean {
  const base = normalizedBaseType(type);
  return base === "Bool" || base === "Boolean";
}

function strictColumnTypeToValidator(type: string): string {
  const validator = clickhouseTypeToValidator(type);
  if (validator.includes("TODO: Unknown type") || validator.includes("/*")) {
    throw new Error(`Unsupported column type in strict mode: "${type}"`);
  }
  return validator;
}

function strictParamBaseValidator(type: string): string {
  const map: Record<string, string> = {
    String: "p.string()",
    UUID: "p.uuid()",
    Int8: "p.int8()",
    Int16: "p.int16()",
    Int32: "p.int32()",
    Int64: "p.int64()",
    UInt8: "p.uint8()",
    UInt16: "p.uint16()",
    UInt32: "p.uint32()",
    UInt64: "p.uint64()",
    Float32: "p.float32()",
    Float64: "p.float64()",
    Boolean: "p.boolean()",
    Bool: "p.boolean()",
    Date: "p.date()",
    DateTime: "p.dateTime()",
    DateTime64: "p.dateTime64()",
    Array: "p.array(p.string())",
  };
  const validator = map[type];
  if (!validator) {
    throw new Error(`Unsupported parameter type in strict mode: "${type}"`);
  }
  return validator;
}

function applyParamOptional(
  baseValidator: string,
  required: boolean,
  defaultValue: string | number | undefined
): string {
  const withDefault = defaultValue !== undefined;
  if (!withDefault && required) {
    return baseValidator;
  }

  const optionalSuffix = withDefault
    ? `.optional(${typeof defaultValue === "string" ? JSON.stringify(defaultValue) : defaultValue})`
    : ".optional()";

  if (baseValidator.endsWith(")")) {
    return `${baseValidator}${optionalSuffix}`;
  }
  return `${baseValidator}${optionalSuffix}`;
}

function engineFunctionName(type: string): string {
  const map: Record<string, string> = {
    MergeTree: "mergeTree",
    ReplacingMergeTree: "replacingMergeTree",
    SummingMergeTree: "summingMergeTree",
    AggregatingMergeTree: "aggregatingMergeTree",
    CollapsingMergeTree: "collapsingMergeTree",
    VersionedCollapsingMergeTree: "versionedCollapsingMergeTree",
  };
  const functionName = map[type];
  if (!functionName) {
    throw new Error(`Unsupported engine type in strict mode: "${type}"`);
  }
  return functionName;
}

function emitEngineOptions(ds: DatasourceModel): string {
  const options: string[] = [];
  const { engine } = ds;

  if (engine.sortingKey.length === 1) {
    options.push(`sortingKey: ${escapeString(engine.sortingKey[0]!)}`);
  } else {
    options.push(
      `sortingKey: [${engine.sortingKey.map((k) => escapeString(k)).join(", ")}]`
    );
  }

  if (engine.partitionKey) {
    options.push(`partitionKey: ${escapeString(engine.partitionKey)}`);
  }
  if (engine.primaryKey && engine.primaryKey.length > 0) {
    if (engine.primaryKey.length === 1) {
      options.push(`primaryKey: ${escapeString(engine.primaryKey[0]!)}`);
    } else {
      options.push(
        `primaryKey: [${engine.primaryKey.map((k) => escapeString(k)).join(", ")}]`
      );
    }
  }
  if (engine.ttl) {
    options.push(`ttl: ${escapeString(engine.ttl)}`);
  }
  if (engine.ver) {
    options.push(`ver: ${escapeString(engine.ver)}`);
  }
  if (engine.sign) {
    options.push(`sign: ${escapeString(engine.sign)}`);
  }
  if (engine.version) {
    options.push(`version: ${escapeString(engine.version)}`);
  }
  if (engine.summingColumns && engine.summingColumns.length > 0) {
    options.push(
      `columns: [${engine.summingColumns.map((k) => escapeString(k)).join(", ")}]`
    );
  }
  if (engine.settings && Object.keys(engine.settings).length > 0) {
    const settingsEntries = Object.entries(engine.settings).map(([k, v]) => {
      if (typeof v === "string") {
        return `${escapeString(k)}: ${escapeString(v)}`;
      }
      return `${escapeString(k)}: ${v}`;
    });
    options.push(`settings: { ${settingsEntries.join(", ")} }`);
  }

  const engineFn = engineFunctionName(engine.type);
  return `engine.${engineFn}({ ${options.join(", ")} })`;
}

function emitDatasource(ds: DatasourceModel): string {
  const variableName = toCamelCase(ds.name);
  const lines: string[] = [];
  const hasJsonPath = ds.columns.some((column) => column.jsonPath !== undefined);
  const hasMissingJsonPath = ds.columns.some((column) => column.jsonPath === undefined);

  if (hasJsonPath && hasMissingJsonPath) {
    throw new Error(
      `Datasource "${ds.name}" has mixed json path usage. This is not representable in strict mode.`
    );
  }

  if (ds.description) {
    lines.push("/**");
    for (const row of ds.description.split("\n")) {
      lines.push(` * ${row}`);
    }
    lines.push(" */");
  }

  lines.push(`export const ${variableName} = defineDatasource(${escapeString(ds.name)}, {`);
  if (ds.description) {
    lines.push(`  description: ${escapeString(ds.description)},`);
  }
  if (!hasJsonPath) {
    lines.push("  jsonPaths: false,");
  }

  lines.push("  schema: {");
  for (const column of ds.columns) {
    let validator = strictColumnTypeToValidator(column.type);

    if (column.defaultExpression !== undefined) {
      const parsedDefault = parseLiteralFromDatafile(column.defaultExpression);
      let literalValue = parsedDefault;
      if (typeof parsedDefault === "number" && isBooleanType(column.type)) {
        if (parsedDefault === 0 || parsedDefault === 1) {
          literalValue = parsedDefault === 1;
        } else {
          throw new Error(
            `Boolean default value must be 0 or 1 for column "${column.name}" in datasource "${ds.name}".`
          );
        }
      }
      validator += `.default(${toTsLiteral(
        literalValue as string | number | boolean | null | Record<string, unknown> | unknown[]
      )})`;
    }

    if (column.codec) {
      validator += `.codec(${escapeString(column.codec)})`;
    }

    if (column.jsonPath) {
      lines.push(
        `    ${column.name}: column(${validator}, { jsonPath: ${escapeString(column.jsonPath)} }),`
      );
    } else {
      lines.push(`    ${column.name}: ${validator},`);
    }
  }
  lines.push("  },");
  lines.push(`  engine: ${emitEngineOptions(ds)},`);

  if (ds.kafka) {
    const connectionVar = toCamelCase(ds.kafka.connectionName);
    lines.push("  kafka: {");
    lines.push(`    connection: ${connectionVar},`);
    lines.push(`    topic: ${escapeString(ds.kafka.topic)},`);
    if (ds.kafka.groupId) {
      lines.push(`    groupId: ${escapeString(ds.kafka.groupId)},`);
    }
    if (ds.kafka.autoOffsetReset) {
      lines.push(`    autoOffsetReset: ${escapeString(ds.kafka.autoOffsetReset)},`);
    }
    lines.push("  },");
  }

  if (ds.s3) {
    const connectionVar = toCamelCase(ds.s3.connectionName);
    lines.push("  s3: {");
    lines.push(`    connection: ${connectionVar},`);
    lines.push(`    bucketUri: ${escapeString(ds.s3.bucketUri)},`);
    if (ds.s3.schedule) {
      lines.push(`    schedule: ${escapeString(ds.s3.schedule)},`);
    }
    if (ds.s3.fromTimestamp) {
      lines.push(`    fromTimestamp: ${escapeString(ds.s3.fromTimestamp)},`);
    }
    lines.push("  },");
  }

  if (ds.forwardQuery) {
    lines.push("  forwardQuery: `");
    lines.push(ds.forwardQuery.replace(/`/g, "\\`").replace(/\${/g, "\\${"));
    lines.push("  `,");
  }

  if (ds.tokens.length > 0) {
    lines.push("  tokens: [");
    for (const token of ds.tokens) {
      lines.push(
        `    { name: ${escapeString(token.name)}, permissions: [${escapeString(token.scope)}] },`
      );
    }
    lines.push("  ],");
  }

  if (ds.sharedWith.length > 0) {
    lines.push(
      `  sharedWith: [${ds.sharedWith.map((workspace) => escapeString(workspace)).join(", ")}],`
    );
  }

  lines.push("});");
  lines.push("");
  return lines.join("\n");
}

function emitConnection(connection: KafkaConnectionModel | S3ConnectionModel): string {
  const variableName = toCamelCase(connection.name);
  const lines: string[] = [];

  if (connection.connectionType === "kafka") {
    lines.push(
      `export const ${variableName} = defineKafkaConnection(${escapeString(connection.name)}, {`
    );
    lines.push(`  bootstrapServers: ${escapeString(connection.bootstrapServers)},`);
    if (connection.securityProtocol) {
      lines.push(`  securityProtocol: ${escapeString(connection.securityProtocol)},`);
    }
    if (connection.saslMechanism) {
      lines.push(`  saslMechanism: ${escapeString(connection.saslMechanism)},`);
    }
    if (connection.key) {
      lines.push(`  key: ${escapeString(connection.key)},`);
    }
    if (connection.secret) {
      lines.push(`  secret: ${escapeString(connection.secret)},`);
    }
    if (connection.sslCaPem) {
      lines.push(`  sslCaPem: ${escapeString(connection.sslCaPem)},`);
    }
    lines.push("});");
    lines.push("");
    return lines.join("\n");
  }

  lines.push(
    `export const ${variableName} = defineS3Connection(${escapeString(connection.name)}, {`
  );
  lines.push(`  region: ${escapeString(connection.region)},`);
  if (connection.arn) {
    lines.push(`  arn: ${escapeString(connection.arn)},`);
  }
  if (connection.accessKey) {
    lines.push(`  accessKey: ${escapeString(connection.accessKey)},`);
  }
  if (connection.secret) {
    lines.push(`  secret: ${escapeString(connection.secret)},`);
  }
  lines.push("});");
  lines.push("");
  return lines.join("\n");
}

function emitPipe(pipe: PipeModel): string {
  const variableName = toCamelCase(pipe.name);
  const lines: string[] = [];
  const endpointOutputColumns =
    pipe.inferredOutputColumns.length > 0 ? pipe.inferredOutputColumns : ["result"];

  if (pipe.description) {
    lines.push("/**");
    for (const row of pipe.description.split("\n")) {
      lines.push(` * ${row}`);
    }
    lines.push(" */");
  }

  if (pipe.type === "materialized") {
    lines.push(`export const ${variableName} = defineMaterializedView(${escapeString(pipe.name)}, {`);
  } else if (pipe.type === "copy") {
    lines.push(`export const ${variableName} = defineCopyPipe(${escapeString(pipe.name)}, {`);
  } else {
    lines.push(`export const ${variableName} = definePipe(${escapeString(pipe.name)}, {`);
  }

  if (pipe.description) {
    lines.push(`  description: ${escapeString(pipe.description)},`);
  }

  if (pipe.type === "pipe" || pipe.type === "endpoint") {
    if (pipe.params.length > 0) {
      lines.push("  params: {");
      for (const param of pipe.params) {
        const baseValidator = strictParamBaseValidator(param.type);
        const validator = applyParamOptional(
          baseValidator,
          param.required,
          param.defaultValue
        );
        lines.push(`    ${param.name}: ${validator},`);
      }
      lines.push("  },");
    }
  }

  if (pipe.type === "materialized") {
    lines.push(`  datasource: ${toCamelCase(pipe.materializedDatasource ?? "")},`);
    if (pipe.deploymentMethod) {
      lines.push(`  deploymentMethod: ${escapeString(pipe.deploymentMethod)},`);
    }
  }

  if (pipe.type === "copy") {
    lines.push(`  datasource: ${toCamelCase(pipe.copyTargetDatasource ?? "")},`);
    if (pipe.copyMode) {
      lines.push(`  copy_mode: ${escapeString(pipe.copyMode)},`);
    }
    if (pipe.copySchedule) {
      lines.push(`  copy_schedule: ${escapeString(pipe.copySchedule)},`);
    }
  }

  lines.push("  nodes: [");
  for (const node of pipe.nodes) {
    lines.push("    node({");
    lines.push(`      name: ${escapeString(node.name)},`);
    if (node.description) {
      lines.push(`      description: ${escapeString(node.description)},`);
    }
    lines.push("      sql: `");
    lines.push(node.sql.replace(/`/g, "\\`").replace(/\${/g, "\\${"));
    lines.push("      `,");
    lines.push("    }),");
  }
  lines.push("  ],");

  if (pipe.type === "endpoint") {
    if (pipe.cacheTtl !== undefined) {
      lines.push(`  endpoint: { enabled: true, cache: { enabled: true, ttl: ${pipe.cacheTtl} } },`);
    } else {
      lines.push("  endpoint: true,");
    }
    lines.push("  output: {");
    for (const columnName of endpointOutputColumns) {
      lines.push(`    ${columnName}: t.string(),`);
    }
    lines.push("  },");
  }

  if (pipe.tokens.length > 0) {
    lines.push("  tokens: [");
    for (const token of pipe.tokens) {
      lines.push(`    { name: ${escapeString(token.name)} },`);
    }
    lines.push("  ],");
  }

  lines.push("});");
  lines.push("");
  return lines.join("\n");
}

export function emitMigrationFileContent(resources: ParsedResource[]): string {
  const connections = resources.filter(
    (resource): resource is KafkaConnectionModel | S3ConnectionModel =>
      resource.kind === "connection"
  );
  const datasources = resources.filter(
    (resource): resource is DatasourceModel => resource.kind === "datasource"
  );
  const pipes = resources.filter(
    (resource): resource is PipeModel => resource.kind === "pipe"
  );

  const needsColumn = datasources.some((ds) =>
    ds.columns.some((column) => column.jsonPath !== undefined)
  );
  const needsParams = pipes.some((pipe) => pipe.params.length > 0);

  const imports = new Set<string>([
    "defineDatasource",
    "definePipe",
    "defineMaterializedView",
    "defineCopyPipe",
    "node",
    "t",
    "engine",
  ]);
  if (connections.some((connection) => connection.connectionType === "kafka")) {
    imports.add("defineKafkaConnection");
  }
  if (connections.some((connection) => connection.connectionType === "s3")) {
    imports.add("defineS3Connection");
  }
  if (needsColumn) {
    imports.add("column");
  }
  if (needsParams) {
    imports.add("p");
  }

  const orderedImports = [
    "defineKafkaConnection",
    "defineS3Connection",
    "defineDatasource",
    "definePipe",
    "defineMaterializedView",
    "defineCopyPipe",
    "node",
    "t",
    "engine",
    "column",
    "p",
  ].filter((name) => imports.has(name));

  const lines: string[] = [];
  lines.push("/**");
  lines.push(" * Generated by tinybird migrate.");
  lines.push(" * Review endpoint output schemas and any defaults before production use.");
  lines.push(" */");
  lines.push("");
  lines.push(`import { ${orderedImports.join(", ")} } from "@tinybirdco/sdk";`);
  lines.push("");

  if (connections.length > 0) {
    lines.push("// Connections");
    lines.push("");
    for (const connection of connections) {
      lines.push(emitConnection(connection));
    }
  }

  if (datasources.length > 0) {
    lines.push("// Datasources");
    lines.push("");
    for (const datasource of datasources) {
      lines.push(emitDatasource(datasource));
    }
  }

  if (pipes.length > 0) {
    lines.push("// Pipes");
    lines.push("");
    for (const pipe of pipes) {
      lines.push(emitPipe(pipe));
    }
  }

  return lines.join("\n").trimEnd() + "\n";
}

export function validateResourceForEmission(resource: ParsedResource): void {
  if (resource.kind === "connection") {
    emitConnection(resource);
    return;
  }
  if (resource.kind === "datasource") {
    emitDatasource(resource);
    return;
  }
  emitPipe(resource);
}
