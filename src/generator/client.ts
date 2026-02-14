/**
 * Client file generator
 * Generates a typed tinybird.ts client file from discovered entities
 */

import * as path from "path";
import type { LoadedEntities } from "./loader.js";

/**
 * Options for generating the client file
 */
export interface GenerateClientOptions {
  /** Loaded entities from source files */
  entities: LoadedEntities;
  /** Output file path (relative to cwd) */
  outputPath: string;
  /** Working directory */
  cwd: string;
}

/**
 * Result of generating the client file
 */
export interface GeneratedClient {
  /** The generated file content */
  content: string;
  /** Absolute path to output file */
  absolutePath: string;
}

/**
 * Convert a file path to a relative import path
 * e.g., "src/datasources.ts" -> "./datasources" when output is "src/tinybird.ts"
 */
function toRelativeImport(fromPath: string, toPath: string): string {
  // Get directory of the from file
  const fromDir = path.dirname(fromPath);

  // Get relative path from output dir to source file
  let relativePath = path.relative(fromDir, toPath);

  // Normalize Windows separators to forward slashes for TS imports
  relativePath = relativePath.replace(/\\/g, "/");

  // Remove .ts extension
  relativePath = relativePath.replace(/\.tsx?$/, "");

  // Ensure it starts with ./ or ../
  if (!relativePath.startsWith(".") && !relativePath.startsWith("/")) {
    relativePath = "./" + relativePath;
  }

  return relativePath;
}

/**
 * Convert camelCase to PascalCase
 */
function toPascalCase(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Generate the client file content
 *
 * @param options - Generation options
 * @returns Generated client file info
 *
 * @example
 * ```ts
 * const result = generateClientFile({
 *   entities: loadedEntities,
 *   outputPath: 'src/tinybird.ts',
 *   cwd: '/path/to/project',
 * });
 *
 * fs.writeFileSync(result.absolutePath, result.content);
 * ```
 */
export function generateClientFile(options: GenerateClientOptions): GeneratedClient {
  const { entities, outputPath, cwd } = options;
  const absolutePath = path.isAbsolute(outputPath) ? outputPath : path.join(cwd, outputPath);

  // Group entities by source file for imports
  const importsByFile = new Map<string, { datasources: string[]; pipes: string[] }>();

  for (const [name, { info }] of Object.entries(entities.datasources)) {
    if (!importsByFile.has(info.sourceFile)) {
      importsByFile.set(info.sourceFile, { datasources: [], pipes: [] });
    }
    importsByFile.get(info.sourceFile)!.datasources.push(name);
  }

  for (const [name, { info }] of Object.entries(entities.pipes)) {
    if (!importsByFile.has(info.sourceFile)) {
      importsByFile.set(info.sourceFile, { datasources: [], pipes: [] });
    }
    importsByFile.get(info.sourceFile)!.pipes.push(name);
  }

  // Build import statements
  const importLines: string[] = [];
  const reexportLines: string[] = [];

  // SDK imports
  const sdkTypes = ["InferRow"];
  if (Object.keys(entities.pipes).length > 0) {
    sdkTypes.push("InferParams", "InferOutputRow");
  }
  importLines.push(
    `import { Tinybird, type ${sdkTypes.join(", type ")} } from "@tinybirdco/sdk";`
  );
  // Node imports for deriving configDir from import.meta.url (monorepo support)
  importLines.push(`import { fileURLToPath } from "url";`);
  importLines.push(`import { dirname } from "path";`);
  importLines.push("");

  // Entity imports and re-exports
  for (const [sourceFile, { datasources, pipes }] of importsByFile) {
    const allExports = [...datasources, ...pipes];
    if (allExports.length === 0) continue;

    // Resolve to absolute paths for correct relative path calculation
    const sourceAbsolute = path.isAbsolute(sourceFile) ? sourceFile : path.join(cwd, sourceFile);
    const relativePath = toRelativeImport(absolutePath, sourceAbsolute);
    importLines.push(`import { ${allExports.join(", ")} } from "${relativePath}";`);
    reexportLines.push(`export { ${allExports.join(", ")} } from "${relativePath}";`);
  }

  // Build createTinybirdClient call
  const datasourceNames = Object.keys(entities.datasources);
  const pipeNames = Object.keys(entities.pipes);

  const clientLines: string[] = [];
  // Derive configDir from import.meta.url for monorepo support
  // This ensures tinybird.json is found regardless of where the app runs from
  clientLines.push("const __configDir = dirname(fileURLToPath(import.meta.url));");
  clientLines.push("");
  clientLines.push("export const tinybird = new Tinybird({");

  if (datasourceNames.length > 0) {
    clientLines.push(`  datasources: { ${datasourceNames.join(", ")} },`);
  } else {
    clientLines.push("  datasources: {},");
  }

  if (pipeNames.length > 0) {
    clientLines.push(`  pipes: { ${pipeNames.join(", ")} },`);
  } else {
    clientLines.push("  pipes: {},");
  }

  clientLines.push("  configDir: __configDir,");
  clientLines.push("});");

  // Build type exports
  const typeLines: string[] = [];

  for (const name of datasourceNames) {
    const pascalName = toPascalCase(name);
    typeLines.push(`export type ${pascalName}Row = InferRow<typeof ${name}>;`);
  }

  for (const name of pipeNames) {
    const { definition } = entities.pipes[name];
    const pascalName = toPascalCase(name);
    typeLines.push(`export type ${pascalName}Params = InferParams<typeof ${name}>;`);
    // Only generate Output type for pipes with output schema
    if (definition._output) {
      typeLines.push(`export type ${pascalName}Output = InferOutputRow<typeof ${name}>;`);
    }
  }

  // Combine all sections
  const sections: string[] = [
    "// Auto-generated by @tinybirdco/sdk - DO NOT EDIT",
    "// This file is regenerated on every build. Manual changes will be overwritten.",
    "",
    importLines.join("\n"),
    "",
    "// Typed Tinybird client",
    clientLines.join("\n"),
    "",
    "// Re-export entities for convenience",
    reexportLines.join("\n"),
  ];

  if (typeLines.length > 0) {
    sections.push("");
    sections.push("// Inferred types from entity definitions");
    sections.push(typeLines.join("\n"));
  }

  sections.push(""); // Trailing newline

  const content = sections.join("\n");

  return {
    content,
    absolutePath,
  };
}
