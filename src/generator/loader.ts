/**
 * Schema loader using esbuild
 * Bundles and executes TypeScript schema files at runtime
 */

import * as esbuild from "esbuild";
import * as path from "path";
import * as fs from "fs";
import { isProjectDefinition, type ProjectDefinition } from "../schema/project.js";

/**
 * Result of loading a schema file
 */
export interface LoadedSchema {
  /** The loaded project definition */
  project: ProjectDefinition;
  /** The resolved path to the schema file */
  schemaPath: string;
  /** The directory containing the schema */
  schemaDir: string;
}

/**
 * Options for the schema loader
 */
export interface LoaderOptions {
  /** The path to the schema file (can be relative or absolute) */
  schemaPath: string;
  /** The working directory for resolution (defaults to cwd) */
  cwd?: string;
}

/**
 * Load and execute a TypeScript schema file
 *
 * Uses esbuild to bundle the schema and its dependencies,
 * then dynamically imports the bundle to get the ProjectDefinition.
 *
 * @param options - Loader options
 * @returns The loaded project definition
 *
 * @example
 * ```ts
 * const { project } = await loadSchema({
 *   schemaPath: 'src/tinybird/schema.ts',
 * });
 *
 * console.log(project.datasources);
 * console.log(project.pipes);
 * ```
 */
export async function loadSchema(options: LoaderOptions): Promise<LoadedSchema> {
  const cwd = options.cwd ?? process.cwd();
  const schemaPath = path.isAbsolute(options.schemaPath)
    ? options.schemaPath
    : path.resolve(cwd, options.schemaPath);

  // Verify the file exists
  if (!fs.existsSync(schemaPath)) {
    throw new Error(`Schema file not found: ${schemaPath}`);
  }

  const schemaDir = path.dirname(schemaPath);

  // Create a temporary output file for the bundle
  const outfile = path.join(
    schemaDir,
    `.tinybird-schema-${Date.now()}.mjs`
  );

  try {
    // Bundle the schema with esbuild
    await esbuild.build({
      entryPoints: [schemaPath],
      outfile,
      bundle: true,
      platform: "node",
      format: "esm",
      target: "node18",
      // Mark @tinybird/sdk as external - it should already be installed
      external: ["@tinybird/sdk"],
      // Enable source maps for better error messages
      sourcemap: "inline",
      // Minify is off for debugging
      minify: false,
    });

    // Import the bundled module
    const moduleUrl = `file://${outfile}`;
    const module = await import(moduleUrl);

    // Look for the project definition
    // It can be the default export or a named 'project' export
    let project: ProjectDefinition | undefined;

    if (module.default && isProjectDefinition(module.default)) {
      project = module.default;
    } else if (module.project && isProjectDefinition(module.project)) {
      project = module.project;
    } else {
      // Check all exports for a project definition
      for (const key of Object.keys(module)) {
        if (isProjectDefinition(module[key])) {
          project = module[key];
          break;
        }
      }
    }

    if (!project) {
      throw new Error(
        `No ProjectDefinition found in ${schemaPath}. ` +
        `Make sure to export a project created with defineProject().`
      );
    }

    return {
      project,
      schemaPath,
      schemaDir,
    };
  } finally {
    // Clean up the temporary bundle file
    try {
      if (fs.existsSync(outfile)) {
        fs.unlinkSync(outfile);
      }
      // Also clean up the source map if it was created separately
      const sourcemapFile = outfile + ".map";
      if (fs.existsSync(sourcemapFile)) {
        fs.unlinkSync(sourcemapFile);
      }
    } catch {
      // Ignore cleanup errors
    }
  }
}

/**
 * Watch options for the schema loader
 */
export interface WatchOptions extends LoaderOptions {
  /** Callback when the schema changes */
  onChange: (result: LoadedSchema) => void | Promise<void>;
  /** Callback when there's an error loading the schema */
  onError?: (error: Error) => void;
  /** Debounce delay in milliseconds (default: 100) */
  debounce?: number;
}
