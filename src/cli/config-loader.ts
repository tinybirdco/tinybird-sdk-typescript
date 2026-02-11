/**
 * Universal config file loader
 * Supports .json and .js files
 */

import * as fs from "node:fs/promises";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";

export type MaybePromise<T> = T | Promise<T>;

export type LoadedConfig<T> = {
  config: T;
  filepath: string;
};

export type LoadConfigOptions = {
  cwd?: string;
};

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function readJsonFile<T>(filepath: string): Promise<T> {
  const raw = await fs.readFile(filepath, "utf8");
  return JSON.parse(raw) as T;
}

/**
 * Import a module file (.js, .cjs, .mjs)
 * Tries ESM import first, falls back to CJS require
 */
async function importModule(filepath: string): Promise<unknown> {
  const url = pathToFileURL(filepath).href;

  try {
    return await import(url);
  } catch {
    // Fallback to CJS require
    const require = createRequire(import.meta.url);
    return require(filepath);
  }
}

/**
 * Resolve the config export from a module
 * Supports default export, module.exports, and function configs
 */
async function resolveConfigExport(mod: unknown): Promise<unknown> {
  const moduleObj = mod as Record<string, unknown>;
  const exported = moduleObj?.default ?? mod;

  // Allow config as function (sync/async)
  if (typeof exported === "function") {
    return await (exported as () => MaybePromise<unknown>)();
  }
  return exported;
}

/**
 * Load a config file from disk
 * Supports .json and .js files
 */
export async function loadConfigFile<T = unknown>(
  configPath: string,
  opts: LoadConfigOptions = {}
): Promise<LoadedConfig<T>> {
  const cwd = opts.cwd ?? process.cwd();
  const filepath = path.isAbsolute(configPath)
    ? configPath
    : path.resolve(cwd, configPath);

  const ext = path.extname(filepath).toLowerCase();

  if (ext === ".json") {
    const config = await readJsonFile<T>(filepath);
    return { config, filepath };
  }

  if (ext === ".js" || ext === ".cjs" || ext === ".mjs") {
    const mod = await importModule(filepath);
    const config = await resolveConfigExport(mod);

    if (!isObject(config)) {
      throw new Error(
        `Config in ${filepath} must export an object (or a function returning an object).`
      );
    }

    return { config: config as T, filepath };
  }

  throw new Error(
    `Unsupported config extension "${ext}". Use .json or .js`
  );
}
