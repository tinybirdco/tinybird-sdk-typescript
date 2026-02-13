import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { runPull } from "./pull.js";

vi.mock("../config.js", () => ({
  loadConfigAsync: vi.fn(),
}));

vi.mock("../../api/resources.js", () => ({
  pullAllResourceFiles: vi.fn(),
}));

import { loadConfigAsync } from "../config.js";
import { pullAllResourceFiles } from "../../api/resources.js";

const mockedLoadConfigAsync = vi.mocked(loadConfigAsync);
const mockedPullAllResourceFiles = vi.mocked(pullAllResourceFiles);

describe("Pull Command", () => {
  let tempDir: string;

  beforeEach(async () => {
    vi.clearAllMocks();
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "tinybird-pull-test-"));

    mockedLoadConfigAsync.mockResolvedValue({
      include: ["src/lib/tinybird.ts"],
      token: "p.test-token",
      baseUrl: "https://api.tinybird.co",
      configPath: path.join(tempDir, "tinybird.config.json"),
      cwd: tempDir,
      gitBranch: "feature/pull",
      tinybirdBranch: "feature_pull",
      isMainBranch: false,
      devMode: "branch",
    });
  });

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true });
    vi.resetAllMocks();
  });

  it("writes pulled datasource, pipe, and connection files", async () => {
    mockedPullAllResourceFiles.mockResolvedValue({
      datasources: [
        {
          name: "events",
          type: "datasource",
          filename: "events.datasource",
          content: "SCHEMA >\n    timestamp DateTime",
        },
      ],
      pipes: [
        {
          name: "top_events",
          type: "pipe",
          filename: "top_events.pipe",
          content: "NODE endpoint\nSQL >\n    SELECT 1",
        },
      ],
      connections: [
        {
          name: "main_kafka",
          type: "connection",
          filename: "main_kafka.connection",
          content: "TYPE kafka",
        },
      ],
    });

    const result = await runPull({ cwd: tempDir, outputDir: "pulled" });

    expect(result.success).toBe(true);
    expect(result.stats).toEqual({
      datasources: 1,
      pipes: 1,
      connections: 1,
      total: 3,
    });

    const outputPath = path.join(tempDir, "pulled");
    await expect(fs.readFile(path.join(outputPath, "events.datasource"), "utf-8")).resolves.toContain(
      "SCHEMA >"
    );
    await expect(fs.readFile(path.join(outputPath, "top_events.pipe"), "utf-8")).resolves.toContain(
      "NODE endpoint"
    );
    await expect(
      fs.readFile(path.join(outputPath, "main_kafka.connection"), "utf-8")
    ).resolves.toContain("TYPE kafka");
  });

  it("returns error when a file exists and overwrite is disabled", async () => {
    mockedPullAllResourceFiles.mockResolvedValue({
      datasources: [
        {
          name: "events",
          type: "datasource",
          filename: "events.datasource",
          content: "SCHEMA >\n    timestamp DateTime",
        },
      ],
      pipes: [],
      connections: [],
    });

    const outputPath = path.join(tempDir, "pulled");
    await fs.mkdir(outputPath, { recursive: true });
    await fs.writeFile(path.join(outputPath, "events.datasource"), "old", "utf-8");

    const result = await runPull({
      cwd: tempDir,
      outputDir: "pulled",
      overwrite: false,
    });

    expect(result.success).toBe(false);
    expect(result.error).toContain("File already exists");
  });

  it("overwrites existing files when overwrite is enabled", async () => {
    mockedPullAllResourceFiles.mockResolvedValue({
      datasources: [
        {
          name: "events",
          type: "datasource",
          filename: "events.datasource",
          content: "new-content",
        },
      ],
      pipes: [],
      connections: [],
    });

    const outputPath = path.join(tempDir, "pulled");
    await fs.mkdir(outputPath, { recursive: true });
    await fs.writeFile(path.join(outputPath, "events.datasource"), "old-content", "utf-8");

    const result = await runPull({
      cwd: tempDir,
      outputDir: "pulled",
      overwrite: true,
    });

    expect(result.success).toBe(true);
    expect(result.files?.[0]?.status).toBe("overwritten");
    await expect(fs.readFile(path.join(outputPath, "events.datasource"), "utf-8")).resolves.toBe(
      "new-content"
    );
  });

  it("returns error when config loading fails", async () => {
    mockedLoadConfigAsync.mockRejectedValue(new Error("No config found"));

    const result = await runPull({ cwd: tempDir });

    expect(result.success).toBe(false);
    expect(result.error).toContain("No config found");
  });

  it("returns error when pull API fails", async () => {
    mockedPullAllResourceFiles.mockRejectedValue(new Error("Unauthorized"));

    const result = await runPull({ cwd: tempDir });

    expect(result.success).toBe(false);
    expect(result.error).toContain("Pull failed");
    expect(result.error).toContain("Unauthorized");
  });
});
