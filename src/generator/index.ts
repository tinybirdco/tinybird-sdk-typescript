/**
 * Main generator module
 * Orchestrates loading schema and generating all resources
 */

import { loadSchema, loadEntities, entitiesToProject, type LoadedEntities } from "./loader.js";
import { generateAllDatasources, type GeneratedDatasource } from "./datasource.js";
import { generateAllPipes, type GeneratedPipe } from "./pipe.js";
import { generateAllConnections, type GeneratedConnection } from "./connection.js";
import type { ProjectDefinition, DatasourcesDefinition, PipesDefinition, ConnectionsDefinition } from "../schema/project.js";

/**
 * Generated resources ready for API push
 */
export interface GeneratedResources {
  /** Generated datasource files */
  datasources: GeneratedDatasource[];
  /** Generated pipe files */
  pipes: GeneratedPipe[];
  /** Generated connection files */
  connections: GeneratedConnection[];
}

/**
 * Build result with metadata
 */
export interface BuildResult {
  /** The generated resources */
  resources: GeneratedResources;
  /** The loaded project definition (for validation) */
  project: ProjectDefinition;
  /** Path to the schema file */
  schemaPath: string;
  /** Directory containing the schema */
  schemaDir: string;
  /** Statistics about the build */
  stats: {
    datasourceCount: number;
    pipeCount: number;
    connectionCount: number;
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
  const connections = generateAllConnections(project.connections);

  return {
    datasources,
    pipes,
    connections,
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
    project: loaded.project,
    schemaPath: loaded.schemaPath,
    schemaDir: loaded.schemaDir,
    stats: {
      datasourceCount: resources.datasources.length,
      pipeCount: resources.pipes.length,
      connectionCount: resources.connections.length,
    },
  };
}

/**
 * Build options using include paths
 */
export interface BuildFromIncludeOptions {
  /** Array of file paths or glob patterns to scan for datasources and pipes */
  includePaths: string[];
  /** Working directory (defaults to cwd) */
  cwd?: string;
}

/**
 * Build result from include paths
 */
export interface BuildFromIncludeResult {
  /** The generated resources */
  resources: GeneratedResources;
  /** Loaded entities from source files */
  entities: LoadedEntities;
  /** Statistics about the build */
  stats: {
    datasourceCount: number;
    pipeCount: number;
    connectionCount: number;
  };
}

/**
 * Generate resources from entities
 */
export function generateResourcesFromEntities(
  datasources: DatasourcesDefinition,
  pipes: PipesDefinition,
  connections: ConnectionsDefinition = {}
): GeneratedResources {
  return {
    datasources: generateAllDatasources(datasources),
    pipes: generateAllPipes(pipes),
    connections: generateAllConnections(connections),
  };
}

/**
 * Build all resources from include paths
 *
 * This is the main entry point for the generator that works with
 * auto-discovered entities. It loads datasources and pipes from the
 * include paths and generates Tinybird resources ready to deploy.
 *
 * Supports both TypeScript files (processed through esbuild) and raw
 * .datasource/.pipe files (included directly).
 *
 * @param options - Build options with include paths
 * @returns Build result with generated resources
 *
 * @example
 * ```ts
 * const result = await buildFromInclude({
 *   includePaths: ['src/tinybird/datasources.ts', 'src/tinybird/pipes.ts'],
 * });
 *
 * // Push resources to Tinybird
 * await deploy(result.resources);
 * ```
 */
export async function buildFromInclude(
  options: BuildFromIncludeOptions
): Promise<BuildFromIncludeResult> {
  const cwd = options.cwd ?? process.cwd();

  // Load entities from include paths
  const entities = await loadEntities({
    includePaths: options.includePaths,
    cwd,
  });

  // Convert to format for generators
  const { datasources, pipes, connections } = entitiesToProject(entities);

  // Generate resources from TypeScript definitions
  const resources = generateResourcesFromEntities(datasources, pipes, connections);

  // Merge in raw datafiles (loaded directly without processing)
  for (const raw of entities.rawDatasources) {
    resources.datasources.push({
      name: raw.name,
      content: raw.content,
    });
  }

  for (const raw of entities.rawPipes) {
    resources.pipes.push({
      name: raw.name,
      content: raw.content,
    });
  }

  return {
    resources,
    entities,
    stats: {
      datasourceCount: resources.datasources.length,
      pipeCount: resources.pipes.length,
      connectionCount: resources.connections.length,
    },
  };
}

// Re-export types and utilities
export { loadSchema, loadEntities, entitiesToProject, type LoaderOptions, type LoadedSchema, type LoadedEntities, type LoadEntitiesOptions } from "./loader.js";
export { generateDatasource, generateAllDatasources, type GeneratedDatasource } from "./datasource.js";
export { generatePipe, generateAllPipes, type GeneratedPipe } from "./pipe.js";
export { generateConnection, generateAllConnections, type GeneratedConnection } from "./connection.js";
export { generateClientFile, type GenerateClientOptions, type GeneratedClient } from "./client.js";
