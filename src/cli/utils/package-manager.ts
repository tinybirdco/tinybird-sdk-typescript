/**
 * Package manager detection utilities
 */

import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";

export type PackageManager = "pnpm" | "yarn" | "bun" | "npm";
const TINYBIRD_SDK_PACKAGE = "@tinybirdco/sdk";

function detectPackageManagerFromLockfile(dir: string): PackageManager | undefined {
  if (existsSync(join(dir, "pnpm-lock.yaml"))) return "pnpm";
  if (existsSync(join(dir, "yarn.lock"))) return "yarn";
  if (existsSync(join(dir, "bun.lockb"))) return "bun";
  if (existsSync(join(dir, "package-lock.json"))) return "npm";
  return undefined;
}

function detectPackageManagerFromWorkspace(dir: string): PackageManager | undefined {
  if (existsSync(join(dir, "pnpm-workspace.yaml"))) return "pnpm";
  if (existsSync(join(dir, "pnpm-workspace.yml"))) return "pnpm";
  return undefined;
}

function detectPackageManagerFromPackageJson(
  dir: string
): PackageManager | undefined {
  const packageJsonPath = join(dir, "package.json");
  if (!existsSync(packageJsonPath)) return undefined;
  try {
    const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf-8"));
    const pm = packageJson.packageManager;
    if (typeof pm !== "string") return undefined;
    if (pm.startsWith("pnpm")) return "pnpm";
    if (pm.startsWith("yarn")) return "yarn";
    if (pm.startsWith("bun")) return "bun";
    if (pm.startsWith("npm")) return "npm";
  } catch {
    return undefined;
  }
  return undefined;
}

function getSearchDirs(start: string): string[] {
  const dirs: string[] = [];
  let current = resolve(start);
  while (true) {
    dirs.push(current);
    const parent = dirname(current);
    if (parent === current) break;
    current = parent;
  }
  return dirs;
}

function findNearestPackageJson(start: string): string | undefined {
  for (const dir of getSearchDirs(start)) {
    const packageJsonPath = join(dir, "package.json");
    if (existsSync(packageJsonPath)) return packageJsonPath;
  }
  return undefined;
}

export function getPackageManagerRunCmd(packageManager: PackageManager): string {
  switch (packageManager) {
    case "pnpm":
      return "pnpm run";
    case "yarn":
      return "yarn";
    case "bun":
      return "bun run";
    case "npm":
    default:
      return "npm run";
  }
}

export function getPackageManagerInstallCmd(
  packageManager: PackageManager
): string {
  switch (packageManager) {
    case "pnpm":
      return "pnpm install";
    case "yarn":
      return "yarn install";
    case "bun":
      return "bun install";
    case "npm":
    default:
      return "npm install";
  }
}

/**
 * Detect package manager (npm, pnpm, yarn, or bun)
 */
export function detectPackageManager(cwd: string = process.cwd()): PackageManager {
  for (const dir of getSearchDirs(cwd)) {
    const fromLockfile = detectPackageManagerFromLockfile(dir);
    if (fromLockfile) return fromLockfile;

    const fromWorkspace = detectPackageManagerFromWorkspace(dir);
    if (fromWorkspace) return fromWorkspace;

    const fromPackageJson = detectPackageManagerFromPackageJson(dir);
    if (fromPackageJson) return fromPackageJson;
  }

  return "npm";
}

export function detectPackageManagerInstallCmd(
  cwd: string = process.cwd()
): string {
  return getPackageManagerInstallCmd(detectPackageManager(cwd));
}

export function hasTinybirdSdkDependency(cwd: string = process.cwd()): boolean {
  const packageJsonPath = findNearestPackageJson(cwd);
  if (!packageJsonPath) return false;

  try {
    const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf-8"));
    const dependencyFields = [
      packageJson.dependencies,
      packageJson.devDependencies,
      packageJson.peerDependencies,
      packageJson.optionalDependencies,
    ];

    return dependencyFields.some(
      (deps) =>
        deps &&
        typeof deps === "object" &&
        Object.prototype.hasOwnProperty.call(deps, TINYBIRD_SDK_PACKAGE)
    );
  } catch {
    return false;
  }
}

/**
 * Detect package manager and return the appropriate run command
 */
export function detectPackageManagerRunCmd(cwd: string = process.cwd()): string {
  return getPackageManagerRunCmd(detectPackageManager(cwd));
}
