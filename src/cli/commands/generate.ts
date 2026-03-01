/**
 * Generate command - build Tinybird resources from TypeScript include paths
 * and expose a stable artifact contract for external consumers.
 */

import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { loadConfigAsync } from "../config.js";
import { buildFromInclude, type BuildFromIncludeResult } from "../../generator/index.js";

export type GeneratedResourceType = "datasource" | "pipe" | "connection";

export interface GeneratedResourceArtifact {
  type: GeneratedResourceType;
  name: string;
  relativePath: string;
  content: string;
}

export interface GenerateCommandOptions {
  cwd?: string;
  outputDir?: string;
}

export interface GenerateCommandResult {
  success: boolean;
  artifacts?: GeneratedResourceArtifact[];
  stats?: {
    datasourceCount: number;
    pipeCount: number;
    connectionCount: number;
    totalCount: number;
  };
  outputDir?: string;
  configPath?: string;
  error?: string;
  durationMs: number;
}

function toArtifacts(build: BuildFromIncludeResult): GeneratedResourceArtifact[] {
  const artifacts: GeneratedResourceArtifact[] = [];

  for (const datasource of build.resources.datasources) {
    artifacts.push({
      type: "datasource",
      name: datasource.name,
      relativePath: `datasources/${datasource.name}.datasource`,
      content: datasource.content,
    });
  }

  for (const pipe of build.resources.pipes) {
    artifacts.push({
      type: "pipe",
      name: pipe.name,
      relativePath: `pipes/${pipe.name}.pipe`,
      content: pipe.content,
    });
  }

  for (const connection of build.resources.connections) {
    artifacts.push({
      type: "connection",
      name: connection.name,
      relativePath: `connections/${connection.name}.connection`,
      content: connection.content,
    });
  }

  return artifacts;
}

async function writeArtifacts(outputDir: string, artifacts: GeneratedResourceArtifact[]): Promise<void> {
  for (const artifact of artifacts) {
    const targetPath = join(outputDir, artifact.relativePath);
    const targetDir = dirname(targetPath);
    await mkdir(targetDir, { recursive: true });
    await writeFile(targetPath, artifact.content, "utf-8");
  }
}

export async function runGenerate(
  options: GenerateCommandOptions = {}
): Promise<GenerateCommandResult> {
  const startTime = Date.now();
  const cwd = options.cwd ?? process.cwd();

  try {
    const config = await loadConfigAsync(cwd);
    const build = await buildFromInclude({
      includePaths: config.include,
      cwd: config.cwd,
    });

    const artifacts = toArtifacts(build);

    if (options.outputDir) {
      await writeArtifacts(options.outputDir, artifacts);
    }

    return {
      success: true,
      artifacts,
      stats: {
        datasourceCount: build.stats.datasourceCount,
        pipeCount: build.stats.pipeCount,
        connectionCount: build.stats.connectionCount,
        totalCount: artifacts.length,
      },
      outputDir: options.outputDir,
      configPath: config.configPath,
      durationMs: Date.now() - startTime,
    };
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
      durationMs: Date.now() - startTime,
    };
  }
}
