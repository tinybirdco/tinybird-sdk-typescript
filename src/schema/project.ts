/**
 * Project definition for Tinybird
 * Aggregates all datasources and pipes into a single schema
 */

import type { DatasourceDefinition, SchemaDefinition } from "./datasource.js";
import type { PipeDefinition, ParamsDefinition, OutputDefinition } from "./pipe.js";

// Symbol for brand typing
const PROJECT_BRAND = Symbol("tinybird.project");

/**
 * Collection of datasource definitions
 */
export type DatasourcesDefinition = Record<string, DatasourceDefinition<SchemaDefinition>>;

/**
 * Collection of pipe definitions
 */
export type PipesDefinition = Record<string, PipeDefinition<ParamsDefinition, OutputDefinition>>;

/**
 * Project configuration
 */
export interface ProjectConfig<
  TDatasources extends DatasourcesDefinition = DatasourcesDefinition,
  TPipes extends PipesDefinition = PipesDefinition
> {
  /** All datasources in this project */
  datasources?: TDatasources;
  /** All pipes in this project */
  pipes?: TPipes;
}

/**
 * A project definition with full type information
 */
export interface ProjectDefinition<
  TDatasources extends DatasourcesDefinition = DatasourcesDefinition,
  TPipes extends PipesDefinition = PipesDefinition
> {
  readonly [PROJECT_BRAND]: true;
  /** Type marker for inference */
  readonly _type: "project";
  /** All datasources */
  readonly datasources: TDatasources;
  /** All pipes */
  readonly pipes: TPipes;
}

/**
 * Define a Tinybird project
 *
 * This aggregates all datasources and pipes into a single schema definition
 * that can be used for code generation and type inference.
 *
 * @param config - Project configuration with datasources and pipes
 * @returns A project definition
 *
 * @example
 * ```ts
 * // tinybird/schema.ts
 * import { defineProject } from '@tinybird/sdk';
 * import { events, users } from './datasources';
 * import { topEvents, userActivity } from './pipes';
 *
 * export default defineProject({
 *   datasources: {
 *     events,
 *     users,
 *   },
 *   pipes: {
 *     topEvents,
 *     userActivity,
 *   },
 * });
 * ```
 */
export function defineProject<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition
>(
  config: ProjectConfig<TDatasources, TPipes>
): ProjectDefinition<TDatasources, TPipes> {
  const datasources = (config.datasources ?? {}) as TDatasources;
  const pipes = (config.pipes ?? {}) as TPipes;

  return {
    [PROJECT_BRAND]: true,
    _type: "project",
    datasources,
    pipes,
  };
}

/**
 * Check if a value is a project definition
 */
export function isProjectDefinition(value: unknown): value is ProjectDefinition {
  return (
    typeof value === "object" &&
    value !== null &&
    PROJECT_BRAND in value &&
    (value as Record<symbol, unknown>)[PROJECT_BRAND] === true
  );
}

/**
 * Get all datasource names from a project
 */
export function getDatasourceNames<T extends ProjectDefinition>(
  project: T
): (keyof T["datasources"])[] {
  return Object.keys(project.datasources) as (keyof T["datasources"])[];
}

/**
 * Get all pipe names from a project
 */
export function getPipeNames<T extends ProjectDefinition>(project: T): (keyof T["pipes"])[] {
  return Object.keys(project.pipes) as (keyof T["pipes"])[];
}

/**
 * Get a datasource by name from a project
 */
export function getDatasource<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition,
  K extends keyof TDatasources
>(project: ProjectDefinition<TDatasources, TPipes>, name: K): TDatasources[K] {
  return project.datasources[name];
}

/**
 * Get a pipe by name from a project
 */
export function getPipe<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition,
  K extends keyof TPipes
>(project: ProjectDefinition<TDatasources, TPipes>, name: K): TPipes[K] {
  return project.pipes[name];
}

/**
 * Helper type to extract datasources from a project
 */
export type ExtractDatasources<T> = T extends ProjectDefinition<infer D, PipesDefinition>
  ? D
  : never;

/**
 * Helper type to extract pipes from a project
 */
export type ExtractPipes<T> = T extends ProjectDefinition<DatasourcesDefinition, infer P>
  ? P
  : never;

/**
 * Data model type derived from a project
 * Useful for generating typed clients
 */
export type DataModel<T extends ProjectDefinition> = {
  datasources: {
    [K in keyof T["datasources"]]: T["datasources"][K] extends DatasourceDefinition<infer S>
      ? S
      : never;
  };
  pipes: {
    [K in keyof T["pipes"]]: T["pipes"][K] extends PipeDefinition<infer P, infer O>
      ? { params: P; output: O }
      : never;
  };
};
