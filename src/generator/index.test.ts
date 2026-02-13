import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { generateResources, build, buildFromInclude } from "./index.js";
import { defineProject } from "../schema/project.js";
import { defineDatasource } from "../schema/datasource.js";
import { definePipe, node } from "../schema/pipe.js";
import { t } from "../schema/types.js";
import { engine } from "../schema/engines.js";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";

describe("Generator Index", () => {
  describe("generateResources", () => {
    it("generates resources from a project definition", () => {
      const events = defineDatasource("events", {
        schema: {
          timestamp: t.dateTime(),
          event_name: t.string(),
        },
      });

      const topEvents = definePipe("top_events", {
        nodes: [
          node({
            name: "endpoint",
            sql: "SELECT event_name, count() as cnt FROM events GROUP BY event_name",
          }),
        ],
        output: {
          event_name: t.string(),
          cnt: t.uint64(),
        },
        endpoint: true,
      });

      const project = defineProject({
        datasources: { events },
        pipes: { topEvents },
      });

      const result = generateResources(project);

      expect(result.datasources).toHaveLength(1);
      expect(result.datasources[0].name).toBe("events");
      expect(result.datasources[0].content).toContain("timestamp DateTime");

      expect(result.pipes).toHaveLength(1);
      expect(result.pipes[0].name).toBe("top_events");
      expect(result.pipes[0].content).toContain("SELECT event_name");
    });

    it("handles empty project", () => {
      const project = defineProject({
        datasources: {},
        pipes: {},
      });

      const result = generateResources(project);

      expect(result.datasources).toHaveLength(0);
      expect(result.pipes).toHaveLength(0);
    });

    it("generates multiple datasources and pipes", () => {
      const ds1 = defineDatasource("ds1", { schema: { id: t.string() } });
      const ds2 = defineDatasource("ds2", { schema: { name: t.string() } });
      const ds3 = defineDatasource("ds3", { schema: { count: t.int32() } });

      const pipe1 = definePipe("pipe1", {
        nodes: [node({ name: "n", sql: "SELECT * FROM ds1" })],
        output: { id: t.string() },
        endpoint: true,
      });
      const pipe2 = definePipe("pipe2", {
        nodes: [node({ name: "n", sql: "SELECT * FROM ds2" })],
        output: { name: t.string() },
        endpoint: true,
      });

      const project = defineProject({
        datasources: { ds1, ds2, ds3 },
        pipes: { pipe1, pipe2 },
      });

      const result = generateResources(project);

      expect(result.datasources).toHaveLength(3);
      expect(result.pipes).toHaveLength(2);
    });

    it("generates datasources with full options", () => {
      const events = defineDatasource("events", {
        description: "Event tracking data",
        schema: {
          timestamp: t.dateTime(),
          event_name: t.string(),
          user_id: t.string().nullable(),
          metadata: t.string().default("{}"),
        },
        engine: engine.mergeTree({
          sortingKey: ["timestamp", "event_name"],
          partitionKey: "toYYYYMM(timestamp)",
        }),
      });

      const project = defineProject({
        datasources: { events },
        pipes: {},
      });

      const result = generateResources(project);

      expect(result.datasources).toHaveLength(1);
      expect(result.datasources[0].content).toContain("DESCRIPTION >");
      expect(result.datasources[0].content).toContain("Event tracking data");
      expect(result.datasources[0].content).toContain("ENGINE_SORTING_KEY");
      expect(result.datasources[0].content).toContain("ENGINE_PARTITION_KEY");
    });

    it("generates pipes with endpoint config", () => {
      const stats = definePipe("stats", {
        description: "Get stats",
        nodes: [
          node({
            name: "calc",
            description: "Calculate statistics",
            sql: "SELECT count() as total FROM events",
          }),
        ],
        output: { total: t.uint64() },
        endpoint: {
          enabled: true,
          cache: { enabled: true, ttl: 120 },
        },
      });

      const project = defineProject({
        datasources: {},
        pipes: { stats },
      });

      const result = generateResources(project);

      expect(result.pipes).toHaveLength(1);
      expect(result.pipes[0].content).toContain("DESCRIPTION >");
      expect(result.pipes[0].content).toContain("Get stats");
      expect(result.pipes[0].content).toContain("TYPE endpoint");
      expect(result.pipes[0].content).toContain("CACHE 120");
    });

    it("generates non-endpoint pipes", () => {
      const materialize = definePipe("materialize", {
        nodes: [
          node({
            name: "aggregate",
            sql: "SELECT event_name, count() FROM events GROUP BY event_name",
          }),
        ],
        output: { event_name: t.string() },
        endpoint: false,
      });

      const project = defineProject({
        datasources: {},
        pipes: { materialize },
      });

      const result = generateResources(project);

      expect(result.pipes).toHaveLength(1);
      expect(result.pipes[0].content).not.toContain("TYPE endpoint");
    });
  });

  describe("build", () => {
    let tempDir: string;

    beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-test-"));
    });

    afterEach(() => {
      try {
        fs.rmSync(tempDir, { recursive: true });
      } catch {
        // Ignore cleanup errors
      }
    });

    it("throws error for non-existent schema file", async () => {
      const schemaPath = path.join(tempDir, "nonexistent.ts");

      await expect(build({ schemaPath })).rejects.toThrow("Schema file not found");
    });

    it("throws error when no project definition is exported", async () => {
      const schemaContent = `
export const notAProject = { foo: "bar" };
`;

      const schemaPath = path.join(tempDir, "invalid-schema.ts");
      fs.writeFileSync(schemaPath, schemaContent);

      await expect(build({ schemaPath })).rejects.toThrow(
        "No ProjectDefinition found"
      );
    });
  });

  describe("buildFromInclude with raw datafiles", () => {
    let tempDir: string;

    beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-raw-test-"));
    });

    afterEach(() => {
      try {
        fs.rmSync(tempDir, { recursive: true });
      } catch {
        // Ignore cleanup errors
      }
    });

    it("includes raw .datasource files directly", async () => {
      // Create a raw datasource file
      const datasourceContent = `SCHEMA >
    timestamp DateTime,
    user_id String

ENGINE "MergeTree"
ENGINE_SORTING_KEY "timestamp"
`;
      const datasourcePath = path.join(tempDir, "events.datasource");
      fs.writeFileSync(datasourcePath, datasourceContent);

      const result = await buildFromInclude({
        includePaths: [datasourcePath],
        cwd: tempDir,
      });

      expect(result.resources.datasources).toHaveLength(1);
      expect(result.resources.datasources[0].name).toBe("events");
      expect(result.resources.datasources[0].content).toBe(datasourceContent);
      expect(result.stats.datasourceCount).toBe(1);
    });

    it("includes raw .pipe files directly", async () => {
      // Create a raw pipe file
      const pipeContent = `NODE endpoint
SQL >
    SELECT count() AS total FROM events

TYPE endpoint
`;
      const pipePath = path.join(tempDir, "stats.pipe");
      fs.writeFileSync(pipePath, pipeContent);

      const result = await buildFromInclude({
        includePaths: [pipePath],
        cwd: tempDir,
      });

      expect(result.resources.pipes).toHaveLength(1);
      expect(result.resources.pipes[0].name).toBe("stats");
      expect(result.resources.pipes[0].content).toBe(pipeContent);
      expect(result.stats.pipeCount).toBe(1);
    });

    it("includes multiple raw datasource and pipe files", async () => {
      // Create multiple raw datasource files
      const datasource1Content = `SCHEMA >
    event_id String,
    timestamp DateTime

ENGINE "MergeTree"
ENGINE_SORTING_KEY "timestamp"
`;
      const datasource1Path = path.join(tempDir, "raw_events.datasource");
      fs.writeFileSync(datasource1Path, datasource1Content);

      const datasource2Content = `SCHEMA >
    user_id String,
    name String

ENGINE "MergeTree"
ENGINE_SORTING_KEY "user_id"
`;
      const datasource2Path = path.join(tempDir, "users.datasource");
      fs.writeFileSync(datasource2Path, datasource2Content);

      // Create a raw pipe file
      const rawPipeContent = `NODE main
SQL >
    SELECT * FROM raw_events

TYPE endpoint
`;
      const rawPipePath = path.join(tempDir, "raw_endpoint.pipe");
      fs.writeFileSync(rawPipePath, rawPipeContent);

      const result = await buildFromInclude({
        includePaths: [datasource1Path, datasource2Path, rawPipePath],
        cwd: tempDir,
      });

      // Should have 2 datasources (both from raw)
      expect(result.resources.datasources).toHaveLength(2);
      expect(result.stats.datasourceCount).toBe(2);

      // Should have 1 pipe (from raw)
      expect(result.resources.pipes).toHaveLength(1);
      expect(result.stats.pipeCount).toBe(1);

      // Check the raw files are included with correct content
      const rawDs1 = result.resources.datasources.find(d => d.name === "raw_events");
      expect(rawDs1).toBeDefined();
      expect(rawDs1!.content).toBe(datasource1Content);

      const rawDs2 = result.resources.datasources.find(d => d.name === "users");
      expect(rawDs2).toBeDefined();
      expect(rawDs2!.content).toBe(datasource2Content);

      const rawPipe = result.resources.pipes.find(p => p.name === "raw_endpoint");
      expect(rawPipe).toBeDefined();
      expect(rawPipe!.content).toBe(rawPipeContent);
    });

    it("supports glob include patterns", async () => {
      const nestedDir = path.join(tempDir, "tinybird", "legacy");
      fs.mkdirSync(nestedDir, { recursive: true });

      const datasourceContent = `SCHEMA >
    id String

ENGINE "MergeTree"
ENGINE_SORTING_KEY "id"
`;
      const datasourcePath = path.join(nestedDir, "events.datasource");
      fs.writeFileSync(datasourcePath, datasourceContent);

      const pipeContent = `NODE endpoint
SQL >
    SELECT * FROM events

TYPE endpoint
`;
      const pipePath = path.join(nestedDir, "events.pipe");
      fs.writeFileSync(pipePath, pipeContent);

      const result = await buildFromInclude({
        includePaths: ["tinybird/**/*.datasource", "tinybird/**/*.pipe"],
        cwd: tempDir,
      });

      expect(result.resources.datasources).toHaveLength(1);
      expect(result.resources.datasources[0].name).toBe("events");
      expect(result.resources.datasources[0].content).toBe(datasourceContent);
      expect(result.resources.pipes).toHaveLength(1);
      expect(result.resources.pipes[0].name).toBe("events");
      expect(result.resources.pipes[0].content).toBe(pipeContent);
    });

    it("throws when include glob matches no files", async () => {
      await expect(
        buildFromInclude({
          includePaths: ["tinybird/**/*.datasource"],
          cwd: tempDir,
        })
      ).rejects.toThrow("Include pattern matched no files");
    });
  });
});
