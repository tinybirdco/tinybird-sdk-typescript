/**
 * Main generator module
 * Orchestrates loading schema and generating all resources
 */

import { loadSchema } from "./loader.js";
import { generateAllDatasources, type GeneratedDatasource } from "./datasource.js";
import { generateAllPipes, type GeneratedPipe } from "./pipe.js";
import type { ProjectDefinition } from "../schema/project.js";

/**
 * Generated resources ready for API push
 */
export interface GeneratedResources {
  /** Generated datasource files */
  datasources: GeneratedDatasource[];
  /** Generated pipe files */
  pipes: GeneratedPipe[];
}

/**
 * Build result with metadata
 */
export interface BuildResult {
  /** The generated resources */
  resources: GeneratedResources;
  /** Path to the schema file */
  schemaPath: string;
  /** Directory containing the schema */
  schemaDir: string;
  /** Statistics about the build */
  stats: {
    datasourceCount: number;
    pipeCount: number;
  };
}

/**
 * Generate resources from a loaded project definition
 *
 * @param project - The project definition
 * @returns Generated resources
 */
export function generateResources(project: ProjectDefinition): GeneratedResources {
  const datasources = generateAllDatasources(project.datasources);
  const pipes = generateAllPipes(project.pipes);

  return {
    datasources,
    pipes,
  };
}

/**
 * Build options
 */
export interface BuildOptions {
  /** Path to the schema file */
  schemaPath: string;
  /** Working directory (defaults to cwd) */
  cwd?: string;
}

/**
 * Build all resources from a TypeScript schema
 *
 * This is the main entry point for the generator.
 * It loads the schema, generates all resources, and returns them
 * ready for API push.
 *
 * @param options - Build options
 * @returns Build result with generated resources
 *
 * @example
 * ```ts
 * const result = await build({
 *   schemaPath: 'src/tinybird/schema.ts',
 * });
 *
 * console.log(`Generated ${result.stats.datasourceCount} datasources`);
 * console.log(`Generated ${result.stats.pipeCount} pipes`);
 *
 * // Resources are ready to push to API
 * result.resources.datasources.forEach(ds => {
 *   console.log(`${ds.name}.datasource:`);
 *   console.log(ds.content);
 * });
 * ```
 */
export async function build(options: BuildOptions): Promise<BuildResult> {
  // Load the schema
  const loaded = await loadSchema({
    schemaPath: options.schemaPath,
    cwd: options.cwd,
  });

  // Generate resources
  const resources = generateResources(loaded.project);

  return {
    resources,
    schemaPath: loaded.schemaPath,
    schemaDir: loaded.schemaDir,
    stats: {
      datasourceCount: resources.datasources.length,
      pipeCount: resources.pipes.length,
    },
  };
}

// Re-export types and utilities
export { loadSchema, type LoaderOptions, type LoadedSchema } from "./loader.js";
export { generateDatasource, generateAllDatasources, type GeneratedDatasource } from "./datasource.js";
export { generatePipe, generateAllPipes, type GeneratedPipe } from "./pipe.js";
