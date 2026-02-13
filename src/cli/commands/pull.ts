/**
 * Pull command - downloads all cloud resources as Tinybird datafiles
 */

import * as fs from "node:fs/promises";
import * as path from "node:path";
import { loadConfigAsync } from "../config.js";
import {
  pullAllResourceFiles,
  type PulledResourceFiles,
  type ResourceFile,
  type ResourceFileType,
} from "../../api/resources.js";

/**
 * Pull command options
 */
export interface PullCommandOptions {
  /** Working directory (defaults to cwd) */
  cwd?: string;
  /** Output directory for pulled files (defaults to current directory) */
  outputDir?: string;
  /** Whether to overwrite existing files (defaults to false) */
  overwrite?: boolean;
}

/**
 * Single file written by pull
 */
export interface PulledFileResult {
  /** Resource name */
  name: string;
  /** Resource type */
  type: ResourceFileType;
  /** Filename written */
  filename: string;
  /** Absolute path written */
  path: string;
  /** Path relative to cwd */
  relativePath: string;
  /** Whether this file was newly created or overwritten */
  status: "created" | "overwritten";
}

/**
 * Pull command result
 */
export interface PullCommandResult {
  /** Whether pull was successful */
  success: boolean;
  /** Output directory used */
  outputDir?: string;
  /** Files written */
  files?: PulledFileResult[];
  /** Pull statistics */
  stats?: {
    datasources: number;
    pipes: number;
    connections: number;
    total: number;
  };
  /** Error message if failed */
  error?: string;
  /** Duration in milliseconds */
  durationMs: number;
}

/**
 * Convert grouped resources to a flat file list
 */
function flattenResources(resources: PulledResourceFiles): ResourceFile[] {
  return [...resources.datasources, ...resources.pipes, ...resources.connections];
}

/**
 * Pull all resources from Tinybird and write them as datafiles
 */
export async function runPull(
  options: PullCommandOptions = {}
): Promise<PullCommandResult> {
  const startTime = Date.now();
  const cwd = options.cwd ?? process.cwd();
  const outputDir = path.resolve(cwd, options.outputDir ?? ".");
  const overwrite = options.overwrite ?? false;

  let config: Awaited<ReturnType<typeof loadConfigAsync>>;
  try {
    config = await loadConfigAsync(cwd);
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
      durationMs: Date.now() - startTime,
    };
  }

  let pulled: PulledResourceFiles;
  try {
    pulled = await pullAllResourceFiles({
      baseUrl: config.baseUrl,
      token: config.token,
    });
  } catch (error) {
    return {
      success: false,
      error: `Pull failed: ${(error as Error).message}`,
      durationMs: Date.now() - startTime,
    };
  }

  const allFiles = flattenResources(pulled).sort((a, b) =>
    a.filename.localeCompare(b.filename)
  );

  try {
    await fs.mkdir(outputDir, { recursive: true });

    const writtenFiles: PulledFileResult[] = [];

    for (const file of allFiles) {
      const absolutePath = path.join(outputDir, file.filename);
      let existed = false;

      try {
        await fs.access(absolutePath);
        existed = true;
      } catch {
        existed = false;
      }

      await fs.writeFile(absolutePath, file.content, {
        encoding: "utf-8",
        flag: overwrite ? "w" : "wx",
      });

      writtenFiles.push({
        name: file.name,
        type: file.type,
        filename: file.filename,
        path: absolutePath,
        relativePath: path.relative(cwd, absolutePath),
        status: existed ? "overwritten" : "created",
      });
    }

    return {
      success: true,
      outputDir,
      files: writtenFiles,
      stats: {
        datasources: pulled.datasources.length,
        pipes: pulled.pipes.length,
        connections: pulled.connections.length,
        total: writtenFiles.length,
      },
      durationMs: Date.now() - startTime,
    };
  } catch (error) {
    const err = error as NodeJS.ErrnoException;

    if (err.code === "EEXIST") {
      return {
        success: false,
        error:
          `File already exists: ${err.path ?? "unknown"}. ` +
          "Use --force to overwrite existing files.",
        durationMs: Date.now() - startTime,
      };
    }

    return {
      success: false,
      error: `Failed to write files: ${(error as Error).message}`,
      durationMs: Date.now() - startTime,
    };
  }
}
