/**
 * Universal config file loader
 * Supports .json, .js, .cjs, .mjs, and .ts files
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
 * Convert JavaScript code to a data URL for dynamic import
 */
function toDataUrl(jsCode: string): string {
  const base64 = Buffer.from(jsCode, "utf8").toString("base64");
  return `data:text/javascript;base64,${base64}`;
}

/**
 * Import a TypeScript file using esbuild transform (in-memory, no temp files)
 */
async function importTypeScript(filepath: string): Promise<unknown> {
  // Dynamic import to avoid bundler issues
  const esbuildModule = "es" + "build";
  const esbuild = (await import(esbuildModule)) as typeof import("esbuild");

  const source = await fs.readFile(filepath, "utf8");

  const result = await esbuild.transform(source, {
    loader: "ts",
    format: "esm",
    target: "node18",
    sourcemap: "inline",
  });

  return await import(toDataUrl(result.code));
}

/**
 * Import a module file (.js, .cjs, .mjs, .ts)
 * Tries ESM import first, falls back to CJS require or esbuild for .ts
 */
async function importModule(filepath: string): Promise<unknown> {
  const url = pathToFileURL(filepath).href;

  try {
    return await import(url);
  } catch {
    // If it's a .ts file and native import failed, use esbuild transform
    if (filepath.endsWith(".ts")) {
      return await importTypeScript(filepath);
    }
    // Fallback to CJS require for .js/.cjs files
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
 * Supports .json, .js, .cjs, .mjs, and .ts files
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

  if (ext === ".js" || ext === ".cjs" || ext === ".mjs" || ext === ".ts") {
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
    `Unsupported config extension "${ext}". Use .json, .js, .cjs, .mjs, or .ts`
  );
}
