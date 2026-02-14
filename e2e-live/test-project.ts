import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, "..");

export function ensureDistBuild(): void {
  const distIndex = path.join(PROJECT_ROOT, "dist", "index.js");
  if (fs.existsSync(distIndex)) {
    return;
  }

  execSync("pnpm build", {
    cwd: PROJECT_ROOT,
    stdio: "pipe",
  });
}

export function createTempProjectDir(): string {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-live-e2e-"));
  const nodeModulesDir = path.join(tempDir, "node_modules");
  const tinybirdcoDir = path.join(nodeModulesDir, "@tinybirdco");
  fs.mkdirSync(tinybirdcoDir, { recursive: true });
  fs.symlinkSync(PROJECT_ROOT, path.join(tinybirdcoDir, "sdk"), "dir");
  return tempDir;
}

export function cleanupTempProjectDir(tempDir: string): void {
  try {
    fs.rmSync(tempDir, { recursive: true });
  } catch {
    // Ignore cleanup errors in tests
  }
}

export function setConfigBaseUrl(projectDir: string, baseUrl: string): void {
  const configPath = path.join(projectDir, "tinybird.config.json");
  const config = JSON.parse(fs.readFileSync(configPath, "utf-8")) as {
    baseUrl?: string;
    include: string[];
    token: string;
    devMode: string;
  };
  config.baseUrl = baseUrl;
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
}
