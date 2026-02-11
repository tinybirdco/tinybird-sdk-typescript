/**
 * Universal config file loader
 * Supports .json, .cjs, and .mjs files
 */

import * as fs from "node:fs/promises";
import * as path from "node:path";
import { pathToFileURL } from "node:url";

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
 * Supports .json, .cjs, and .mjs files
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

  if (ext === ".mjs" || ext === ".cjs") {
    // Load JS modules via runtime import for bundler compatibility
    const url = pathToFileURL(filepath).href;
    const mod = await import(
      /* webpackIgnore: true */
      /* @vite-ignore */
      url
    );
    const config = await resolveConfigExport(mod);

    if (!isObject(config)) {
      throw new Error(
        `Config in ${filepath} must export an object (or a function returning an object).`
      );
    }

    return { config: config as T, filepath };
  }

  throw new Error(
    `Unsupported config extension "${ext}". Use .json, .mjs, or .cjs`
  );
}
