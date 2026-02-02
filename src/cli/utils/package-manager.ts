/**
 * Package manager detection utilities
 */

import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

/**
 * Detect package manager and return the appropriate run command
 */
export function detectPackageManagerRunCmd(cwd: string = process.cwd()): string {
  // Check lockfiles first (most reliable)
  if (existsSync(join(cwd, "pnpm-lock.yaml"))) {
    return "pnpm";
  }
  if (existsSync(join(cwd, "yarn.lock"))) {
    return "yarn";
  }
  if (existsSync(join(cwd, "bun.lockb"))) {
    return "bun run";
  }
  if (existsSync(join(cwd, "package-lock.json"))) {
    return "npm run";
  }

  // Check packageManager field in package.json
  const packageJsonPath = join(cwd, "package.json");
  if (existsSync(packageJsonPath)) {
    try {
      const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf-8"));
      const pm = packageJson.packageManager;
      if (typeof pm === "string") {
        if (pm.startsWith("pnpm")) return "pnpm";
        if (pm.startsWith("yarn")) return "yarn";
        if (pm.startsWith("bun")) return "bun run";
      }
    } catch {
      // Ignore parse errors
    }
  }

  // Default to npm
  return "npm run";
}
