import * as fs from "node:fs";
import * as path from "node:path";
import { resolveIncludeFiles } from "../generator/include-paths.js";
import type { MigrationError, ResourceFile, ResourceKind } from "./types.js";

const SUPPORTED_EXTENSIONS = new Map<string, ResourceKind>([
  [".datasource", "datasource"],
  [".pipe", "pipe"],
  [".connection", "connection"],
]);

function normalizePath(filePath: string): string {
  return filePath.replace(/\\/g, "/");
}

function getKindFromPath(filePath: string): ResourceKind | null {
  const ext = path.extname(filePath).toLowerCase();
  return SUPPORTED_EXTENSIONS.get(ext) ?? null;
}

function collectDirectoryFiles(directory: string): string[] {
  const entries = fs.readdirSync(directory, { withFileTypes: true });
  const files: string[] = [];

  for (const entry of entries) {
    const fullPath = path.join(directory, entry.name);

    if (entry.isDirectory()) {
      if (entry.name === "node_modules" || entry.name === ".git") {
        continue;
      }
      files.push(...collectDirectoryFiles(fullPath));
      continue;
    }

    if (entry.isFile()) {
      files.push(fullPath);
    }
  }

  return files;
}

export interface DiscoverResourcesResult {
  resources: ResourceFile[];
  errors: MigrationError[];
}

export function discoverResourceFiles(
  patterns: string[],
  cwd: string
): DiscoverResourcesResult {
  const resources: ResourceFile[] = [];
  const errors: MigrationError[] = [];
  const seen = new Set<string>();

  for (const pattern of patterns) {
    const absolutePattern = path.isAbsolute(pattern)
      ? pattern
      : path.resolve(cwd, pattern);

    if (fs.existsSync(absolutePattern)) {
      const stat = fs.statSync(absolutePattern);

      if (stat.isDirectory()) {
        const directoryFiles = collectDirectoryFiles(absolutePattern);
        for (const absoluteFilePath of directoryFiles) {
          const kind = getKindFromPath(absoluteFilePath);
          if (!kind) {
            continue;
          }

          const key = normalizePath(absoluteFilePath);
          if (seen.has(key)) {
            continue;
          }
          seen.add(key);

          const relativePath = normalizePath(path.relative(cwd, absoluteFilePath));
          const name = path.basename(absoluteFilePath, path.extname(absoluteFilePath));
          resources.push({
            kind,
            filePath: relativePath,
            absolutePath: absoluteFilePath,
            name,
            content: fs.readFileSync(absoluteFilePath, "utf-8"),
          });
        }
        continue;
      }

      const kind = getKindFromPath(absolutePattern);
      if (!kind) {
        errors.push({
          filePath: pattern,
          resourceName: path.basename(absolutePattern),
          resourceKind: "datasource",
          message: `Unsupported file extension: ${path.extname(absolutePattern) || "(none)"}. Use .datasource, .pipe, or .connection.`,
        });
        continue;
      }

      const key = normalizePath(absolutePattern);
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      resources.push({
        kind,
        filePath: normalizePath(path.relative(cwd, absolutePattern)),
        absolutePath: absolutePattern,
        name: path.basename(absolutePattern, path.extname(absolutePattern)),
        content: fs.readFileSync(absolutePattern, "utf-8"),
      });
      continue;
    }

    try {
      const matched = resolveIncludeFiles([pattern], cwd);
      for (const entry of matched) {
        const kind = getKindFromPath(entry.absolutePath);
        if (!kind) {
          continue;
        }
        const key = normalizePath(entry.absolutePath);
        if (seen.has(key)) {
          continue;
        }
        seen.add(key);
        resources.push({
          kind,
          filePath: normalizePath(entry.sourcePath),
          absolutePath: entry.absolutePath,
          name: path.basename(entry.absolutePath, path.extname(entry.absolutePath)),
          content: fs.readFileSync(entry.absolutePath, "utf-8"),
        });
      }
    } catch (error) {
      errors.push({
        filePath: pattern,
        resourceName: path.basename(pattern),
        resourceKind: "datasource",
        message: (error as Error).message,
      });
    }
  }

  resources.sort((a, b) => a.filePath.localeCompare(b.filePath));
  return { resources, errors };
}

