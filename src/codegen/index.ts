/**
 * Code generator for converting Tinybird API resources to TypeScript SDK code
 */

import type { DatasourceInfo, PipeInfo } from "../api/resources.js";
import { clickhouseTypeToValidator, paramTypeToValidator } from "./type-mapper.js";
import {
  toCamelCase,
  toPascalCase,
  escapeString,
  generateEngineCode,
  formatSqlForTemplate,
} from "./utils.js";

/**
 * Generate TypeScript code for a single datasource
 */
export function generateDatasourceCode(ds: DatasourceInfo): string {
  const varName = toCamelCase(ds.name);
  const typeName = toPascalCase(ds.name);
  const lines: string[] = [];

  // Check if any columns have jsonpath set
  const hasJsonpath = ds.columns.some((col) => col.jsonpath);

  // JSDoc comment
  if (ds.description) {
    lines.push("/**");
    lines.push(` * ${ds.description}`);
    lines.push(" */");
  }

  lines.push(`export const ${varName} = defineDatasource("${ds.name}", {`);

  if (ds.description) {
    lines.push(`  description: "${escapeString(ds.description)}",`);
  }

  // Add jsonPaths: false if no columns use jsonpath
  if (!hasJsonpath) {
    lines.push("  jsonPaths: false,");
  }

  // Schema
  lines.push("  schema: {");
  for (const col of ds.columns) {
    const validator = clickhouseTypeToValidator(col.type);
    lines.push(`    ${col.name}: ${validator},`);
  }
  lines.push("  },");

  // Engine
  const engineCode = generateEngineCode(ds.engine);
  lines.push(`  engine: ${engineCode},`);

  if (ds.forward_query) {
    const formattedQuery = formatSqlForTemplate(ds.forward_query);
    lines.push(`  forwardQuery: \`${formattedQuery}\`,`);
  }

  lines.push("});");
  lines.push("");
  lines.push(`export type ${typeName}Row = InferRow<typeof ${varName}>;`);

  return lines.join("\n");
}

/**
 * Generate TypeScript code for a single pipe
 */
export function generatePipeCode(pipe: PipeInfo): string {
  const varName = toCamelCase(pipe.name);
  const typeName = toPascalCase(pipe.name);
  const lines: string[] = [];

  // Determine which define function to use
  let defineFunc = "definePipe";
  if (pipe.type === "endpoint") {
    defineFunc = "defineEndpoint";
  } else if (pipe.type === "materialized") {
    defineFunc = "defineMaterializedView";
  } else if (pipe.type === "copy") {
    defineFunc = "defineCopyPipe";
  }

  // JSDoc comment
  if (pipe.description) {
    lines.push("/**");
    lines.push(` * ${pipe.description}`);
    lines.push(" */");
  }

  lines.push(`export const ${varName} = ${defineFunc}("${pipe.name}", {`);

  if (pipe.description) {
    lines.push(`  description: "${escapeString(pipe.description)}",`);
  }

  // For materialized views and copy pipes, add datasource first
  if (pipe.type === "materialized" && pipe.materialized) {
    const dsVarName = toCamelCase(pipe.materialized.datasource);
    lines.push(`  datasource: ${dsVarName},`);
  } else if (pipe.type === "copy" && pipe.copy) {
    const dsVarName = toCamelCase(pipe.copy.target_datasource);
    lines.push(`  datasource: ${dsVarName},`);
    if (pipe.copy.copy_schedule) {
      lines.push(`  copy_schedule: "${pipe.copy.copy_schedule}",`);
    }
    if (pipe.copy.copy_mode) {
      lines.push(`  copy_mode: "${pipe.copy.copy_mode}",`);
    }
  }

  // Params (for endpoints and regular pipes with params)
  if (pipe.params.length > 0 && pipe.type !== "materialized" && pipe.type !== "copy") {
    lines.push("  params: {");
    for (const param of pipe.params) {
      const validator = paramTypeToValidator(param.type, param.default, param.required);
      if (param.description) {
        lines.push(`    ${param.name}: ${validator}.describe("${escapeString(param.description)}"),`);
      } else {
        lines.push(`    ${param.name}: ${validator},`);
      }
    }
    lines.push("  },");
  }

  // Nodes
  lines.push("  nodes: [");
  for (const node of pipe.nodes) {
    lines.push("    node({");
    lines.push(`      name: "${node.name}",`);
    const formattedSql = formatSqlForTemplate(node.sql);
    lines.push(`      sql: \`${formattedSql}\`,`);
    lines.push("    }),");
  }
  lines.push("  ],");

  // Output (for endpoints)
  if (pipe.type === "endpoint" && pipe.output_columns.length > 0) {
    lines.push("  output: {");
    for (const col of pipe.output_columns) {
      const validator = clickhouseTypeToValidator(col.type);
      lines.push(`    ${col.name}: ${validator},`);
    }
    lines.push("  },");
  }

  lines.push("});");

  // Type exports for endpoints
  if (pipe.type === "endpoint") {
    lines.push("");
    lines.push(`export type ${typeName}Params = InferParams<typeof ${varName}>;`);
    lines.push(`export type ${typeName}Output = InferOutputRow<typeof ${varName}>;`);
  }

  return lines.join("\n");
}

/**
 * Generate the complete datasources.ts file content
 */
export function generateDatasourcesFile(datasources: DatasourceInfo[]): string {
  if (datasources.length === 0) {
    return `import { defineDatasource, t, engine, type InferRow } from "@tinybirdco/sdk";

// No datasources found in workspace
`;
  }

  const imports = [
    'import { defineDatasource, t, engine, type InferRow } from "@tinybirdco/sdk";',
    "",
  ];

  const code = datasources.map((ds) => generateDatasourceCode(ds)).join("\n\n");

  return imports.join("\n") + "\n" + code + "\n";
}

/**
 * Generate the complete pipes.ts file content
 */
export function generatePipesFile(
  pipes: PipeInfo[],
  datasources: DatasourceInfo[]
): string {
  if (pipes.length === 0) {
    return `import { defineEndpoint, node, t, p, type InferParams, type InferOutputRow } from "@tinybirdco/sdk";

// No pipes found in workspace
`;
  }

  // Determine which imports are needed
  const hasMaterialized = pipes.some((p) => p.type === "materialized");
  const hasCopy = pipes.some((p) => p.type === "copy");
  const hasEndpoint = pipes.some((p) => p.type === "endpoint");
  const hasPlainPipe = pipes.some((p) => p.type === "pipe");
  const hasParams = pipes.some((p) => p.params.length > 0 && p.type !== "materialized" && p.type !== "copy");

  const sdkImports: string[] = ["node", "t"];
  if (hasParams) {
    sdkImports.push("p");
  }
  if (hasEndpoint) {
    sdkImports.push("defineEndpoint", "type InferParams", "type InferOutputRow");
  }
  if (hasMaterialized) {
    sdkImports.push("defineMaterializedView");
  }
  if (hasCopy) {
    sdkImports.push("defineCopyPipe");
  }
  if (hasPlainPipe) {
    sdkImports.push("definePipe");
  }

  const lines: string[] = [
    `import { ${sdkImports.join(", ")} } from "@tinybirdco/sdk";`,
  ];

  // Import datasources referenced by materialized/copy pipes
  const referencedDatasources = new Set<string>();
  for (const pipe of pipes) {
    if (pipe.materialized?.datasource) {
      referencedDatasources.add(pipe.materialized.datasource);
    }
    if (pipe.copy?.target_datasource) {
      referencedDatasources.add(pipe.copy.target_datasource);
    }
  }

  if (referencedDatasources.size > 0) {
    // Verify datasources exist
    const existingDatasourceNames = new Set(datasources.map((ds) => ds.name));
    const validReferences = Array.from(referencedDatasources).filter((name) =>
      existingDatasourceNames.has(name)
    );

    if (validReferences.length > 0) {
      const dsImports = validReferences.map((name) => toCamelCase(name)).join(", ");
      lines.push(`import { ${dsImports} } from "./datasources.js";`);
    }
  }

  lines.push("");

  const code = pipes.map((p) => generatePipeCode(p)).join("\n\n");

  return lines.join("\n") + code + "\n";
}

/**
 * Generate the complete client.ts file content
 */
export function generateClientFile(
  datasources: DatasourceInfo[],
  pipes: PipeInfo[]
): string {
  const lines: string[] = [
    "/**",
    " * Tinybird Client",
    " *",
    " * This file defines the typed Tinybird client for your project.",
    " * Generated from existing workspace resources.",
    " */",
    "",
    'import { createTinybirdClient } from "@tinybirdco/sdk";',
    "",
  ];

  // Import datasources
  if (datasources.length > 0) {
    const dsImports: string[] = [];
    const typeImports: string[] = [];

    for (const ds of datasources) {
      const varName = toCamelCase(ds.name);
      const typeName = toPascalCase(ds.name);
      dsImports.push(varName);
      typeImports.push(`type ${typeName}Row`);
    }

    lines.push(`import { ${dsImports.join(", ")}, ${typeImports.join(", ")} } from "./datasources.js";`);
  }

  // Import pipes (only endpoints are useful for the client)
  const endpoints = pipes.filter((p) => p.type === "endpoint");
  if (endpoints.length > 0) {
    const pipeImports: string[] = [];
    const typeImports: string[] = [];

    for (const pipe of endpoints) {
      const varName = toCamelCase(pipe.name);
      const typeName = toPascalCase(pipe.name);
      pipeImports.push(varName);
      typeImports.push(`type ${typeName}Params`, `type ${typeName}Output`);
    }

    lines.push(`import { ${pipeImports.join(", ")}, ${typeImports.join(", ")} } from "./pipes.js";`);
  }

  lines.push("");

  // Create client
  lines.push("// Create the typed Tinybird client");
  lines.push("export const tinybird = createTinybirdClient({");

  if (datasources.length > 0) {
    const dsNames = datasources.map((ds) => toCamelCase(ds.name)).join(", ");
    lines.push(`  datasources: { ${dsNames} },`);
  } else {
    lines.push("  datasources: {},");
  }

  if (endpoints.length > 0) {
    const pipeNames = endpoints.map((p) => toCamelCase(p.name)).join(", ");
    lines.push(`  pipes: { ${pipeNames} },`);
  } else {
    lines.push("  pipes: {},");
  }

  lines.push("});");
  lines.push("");

  // Re-export types
  if (datasources.length > 0 || endpoints.length > 0) {
    lines.push("// Re-export types for convenience");
    const typeExports: string[] = [];

    for (const ds of datasources) {
      const typeName = toPascalCase(ds.name);
      typeExports.push(`${typeName}Row`);
    }

    for (const pipe of endpoints) {
      const typeName = toPascalCase(pipe.name);
      typeExports.push(`${typeName}Params`, `${typeName}Output`);
    }

    lines.push(`export type { ${typeExports.join(", ")} };`);
    lines.push("");
  }

  // Re-export entities
  if (datasources.length > 0 || endpoints.length > 0) {
    lines.push("// Re-export entities");
    const entityExports: string[] = [];

    for (const ds of datasources) {
      entityExports.push(toCamelCase(ds.name));
    }

    for (const pipe of endpoints) {
      entityExports.push(toCamelCase(pipe.name));
    }

    lines.push(`export { ${entityExports.join(", ")} };`);
    lines.push("");
  }

  return lines.join("\n");
}

/**
 * Result of generating all files
 */
export interface GeneratedFiles {
  datasourcesContent: string;
  pipesContent: string;
  clientContent: string;
  datasourceCount: number;
  pipeCount: number;
}

/**
 * Generate all TypeScript files from resources
 */
export function generateAllFiles(
  datasources: DatasourceInfo[],
  pipes: PipeInfo[]
): GeneratedFiles {
  return {
    datasourcesContent: generateDatasourcesFile(datasources),
    pipesContent: generatePipesFile(pipes, datasources),
    clientContent: generateClientFile(datasources, pipes),
    datasourceCount: datasources.length,
    pipeCount: pipes.length,
  };
}

/**
 * Generate a single combined tinybird.ts file with all definitions
 */
export function generateCombinedFile(
  datasources: DatasourceInfo[],
  pipes: PipeInfo[]
): string {
  const lines: string[] = [
    "/**",
    " * Tinybird Definitions",
    " *",
    " * This file contains all datasource and endpoint definitions.",
    " * Generated from existing workspace resources.",
    " */",
    "",
  ];

  // Build imports
  const sdkImports: string[] = ["createTinybirdClient", "t"];

  if (datasources.length > 0) {
    sdkImports.push("defineDatasource", "engine", "type InferRow");
  }

  const hasMaterialized = pipes.some((p) => p.type === "materialized");
  const hasCopy = pipes.some((p) => p.type === "copy");
  const hasEndpoint = pipes.some((p) => p.type === "endpoint");
  const hasPlainPipe = pipes.some((p) => p.type === "pipe");
  const hasParams = pipes.some(
    (p) => p.params.length > 0 && p.type !== "materialized" && p.type !== "copy"
  );

  if (pipes.length > 0) {
    sdkImports.push("node");
  }
  if (hasParams) {
    sdkImports.push("p");
  }
  if (hasEndpoint) {
    sdkImports.push("defineEndpoint", "type InferParams", "type InferOutputRow");
  }
  if (hasMaterialized) {
    sdkImports.push("defineMaterializedView");
  }
  if (hasCopy) {
    sdkImports.push("defineCopyPipe");
  }
  if (hasPlainPipe) {
    sdkImports.push("definePipe");
  }

  lines.push(`import {`);
  lines.push(`  ${sdkImports.join(",\n  ")},`);
  lines.push(`} from "@tinybirdco/sdk";`);
  lines.push("");

  // Datasources section
  if (datasources.length > 0) {
    lines.push("// ============================================================================");
    lines.push("// Datasources");
    lines.push("// ============================================================================");
    lines.push("");

    for (const ds of datasources) {
      lines.push(generateDatasourceCode(ds));
      lines.push("");
    }
  }

  // Pipes/Endpoints section
  if (pipes.length > 0) {
    lines.push("// ============================================================================");
    lines.push("// Endpoints");
    lines.push("// ============================================================================");
    lines.push("");

    for (const pipe of pipes) {
      lines.push(generatePipeCode(pipe));
      lines.push("");
    }
  }

  // Client section
  lines.push("// ============================================================================");
  lines.push("// Client");
  lines.push("// ============================================================================");
  lines.push("");

  const dsNames =
    datasources.length > 0
      ? datasources.map((ds) => toCamelCase(ds.name)).join(", ")
      : "";
  const endpoints = pipes.filter((p) => p.type === "endpoint");
  const pipeNames =
    endpoints.length > 0
      ? endpoints.map((p) => toCamelCase(p.name)).join(", ")
      : "";

  lines.push("export const tinybird = createTinybirdClient({");
  lines.push(`  datasources: { ${dsNames} },`);
  lines.push(`  pipes: { ${pipeNames} },`);
  lines.push("});");
  lines.push("");

  return lines.join("\n");
}
