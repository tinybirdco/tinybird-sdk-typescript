import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { generateResources, build } from "./index.js";
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
});
