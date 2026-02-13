import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runMigrate } from "./migrate.js";

function writeFile(dir: string, relativePath: string, content: string): void {
  const fullPath = path.join(dir, relativePath);
  fs.mkdirSync(path.dirname(fullPath), { recursive: true });
  fs.writeFileSync(fullPath, content);
}

describe("runMigrate", () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    for (const dir of tempDirs) {
      try {
        fs.rmSync(dir, { recursive: true });
      } catch {
        // Ignore cleanup failures
      }
    }
    tempDirs.length = 0;
  });

  it("migrates datasource, pipe, and connection files into a single output file", async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-migrate-"));
    tempDirs.push(tempDir);

    writeFile(
      tempDir,
      "events.datasource",
      `SCHEMA >
    id String,
    ts DateTime

ENGINE "MergeTree"
ENGINE_SORTING_KEY "id"
`
    );

    writeFile(
      tempDir,
      "stats.pipe",
      `NODE endpoint
SQL >
    SELECT id AS id
    FROM events

TYPE endpoint
`
    );

    writeFile(
      tempDir,
      "main_kafka.connection",
      `TYPE kafka
KAFKA_BOOTSTRAP_SERVERS localhost:9092
`
    );

    const result = await runMigrate({
      cwd: tempDir,
      patterns: ["."],
      strict: true,
    });

    expect(result.success).toBe(true);
    expect(result.errors).toHaveLength(0);
    expect(result.migrated).toHaveLength(3);
    expect(path.basename(result.outputPath)).toBe("tinybird.migration.ts");
    expect(fs.existsSync(result.outputPath)).toBe(true);

    const output = fs.readFileSync(result.outputPath, "utf-8");
    expect(output).toContain('createKafkaConnection("main_kafka"');
    expect(output).toContain('defineDatasource("events"');
    expect(output).toContain('definePipe("stats"');
  });

  it("continues processing and reports all errors while still writing migrated resources", async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-migrate-"));
    tempDirs.push(tempDir);

    writeFile(
      tempDir,
      "events.datasource",
      `SCHEMA >
    id String

ENGINE "MergeTree"
ENGINE_SORTING_KEY "id"
`
    );

    writeFile(
      tempDir,
      "broken.pipe",
      `NODE endpoint
SQL >
    SELECT id FROM events

TYPE endpoint
FOO bar
`
    );

    const result = await runMigrate({
      cwd: tempDir,
      patterns: ["*.datasource", "*.pipe"],
      strict: true,
    });

    expect(result.success).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.migrated.some((resource) => resource.kind === "datasource")).toBe(true);
    expect(fs.existsSync(result.outputPath)).toBe(true);

    const output = fs.readFileSync(result.outputPath, "utf-8");
    expect(output).toContain('defineDatasource("events"');
    expect(output).not.toContain('definePipe("broken"');
  });

  it("does not write output in dry-run mode", async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-migrate-"));
    tempDirs.push(tempDir);

    writeFile(
      tempDir,
      "events.datasource",
      `SCHEMA >
    id String

ENGINE "MergeTree"
ENGINE_SORTING_KEY "id"
`
    );

    const result = await runMigrate({
      cwd: tempDir,
      patterns: ["events.datasource"],
      strict: true,
      dryRun: true,
    });

    expect(result.success).toBe(true);
    expect(result.errors).toHaveLength(0);
    expect(result.outputContent).toBeTruthy();
    expect(fs.existsSync(result.outputPath)).toBe(false);
  });
});

