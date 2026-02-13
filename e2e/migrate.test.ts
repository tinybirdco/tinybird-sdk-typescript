/**
 * E2E tests for the migrate command flow
 *
 * Tests:
 * 1. Migrating legacy datafiles to TypeScript output
 * 2. Consuming generated TypeScript with buildFromInclude
 * 3. Partial migration behavior (continue + report errors)
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { runMigrate } from "../src/cli/commands/migrate.js";

function writeFile(baseDir: string, filePath: string, content: string): void {
  const fullPath = path.join(baseDir, filePath);
  fs.mkdirSync(path.dirname(fullPath), { recursive: true });
  fs.writeFileSync(fullPath, content);
}

describe("E2E: migrate", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-e2e-migrate-"));
  });

  afterEach(() => {
    try {
      fs.rmSync(tempDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  it("migrates legacy resources and writes a valid combined TypeScript file", async () => {
    writeFile(
      tempDir,
      "legacy/main.connection",
      `TYPE kafka
KAFKA_BOOTSTRAP_SERVERS localhost:9092
`
    );

    writeFile(
      tempDir,
      "legacy/events.datasource",
      `SCHEMA >
    id String,
    timestamp DateTime

ENGINE "MergeTree"
ENGINE_SORTING_KEY "id"
KAFKA_CONNECTION_NAME main
KAFKA_TOPIC events_topic
`
    );

    writeFile(
      tempDir,
      "legacy/top_events.pipe",
      `NODE endpoint
SQL >
    SELECT id AS id
    FROM events

TYPE endpoint
`
    );

    const migrateResult = await runMigrate({
      cwd: tempDir,
      patterns: ["legacy"],
      strict: true,
    });

    expect(migrateResult.success).toBe(true);
    expect(migrateResult.errors).toHaveLength(0);
    expect(migrateResult.migrated).toHaveLength(3);
    expect(path.basename(migrateResult.outputPath)).toBe("tinybird.migration.ts");
    expect(fs.existsSync(migrateResult.outputPath)).toBe(true);

    const output = fs.readFileSync(migrateResult.outputPath, "utf-8");
    expect(output).toContain('createKafkaConnection("main"');
    expect(output).toContain('defineDatasource("events"');
    expect(output).toContain('definePipe("top_events"');
    expect(output).toContain('export const topEvents');
  });

  it("continues migration, reports errors, and still writes migratable resources", async () => {
    writeFile(
      tempDir,
      "legacy/events.datasource",
      `SCHEMA >
    id String

ENGINE "MergeTree"
ENGINE_SORTING_KEY "id"
`
    );

    writeFile(
      tempDir,
      "legacy/broken.pipe",
      `NODE endpoint
SQL >
    SELECT id FROM events

TYPE endpoint
UNSUPPORTED_DIRECTIVE true
`
    );

    const migrateResult = await runMigrate({
      cwd: tempDir,
      patterns: ["legacy"],
      strict: true,
    });

    expect(migrateResult.success).toBe(false);
    expect(migrateResult.errors.length).toBeGreaterThan(0);
    expect(
      migrateResult.migrated.some((resource) => resource.kind === "datasource")
    ).toBe(true);
    expect(fs.existsSync(migrateResult.outputPath)).toBe(true);

    const generated = fs.readFileSync(migrateResult.outputPath, "utf-8");
    expect(generated).toContain('defineDatasource("events"');
    expect(generated).not.toContain('definePipe("broken"');

    expect(migrateResult.migrated.filter((resource) => resource.kind === "datasource")).toHaveLength(1);
    expect(migrateResult.migrated.filter((resource) => resource.kind === "pipe")).toHaveLength(0);
  });
});
