import { describe, it, expect } from "vitest";
import {
  definePipe,
  node,
  isPipeDefinition,
  getEndpointConfig,
  getNodeNames,
  getNode,
  sql,
} from "./pipe.js";
import { defineDatasource } from "./datasource.js";
import { t } from "./types.js";
import { p } from "./params.js";

describe("Pipe Schema", () => {
  describe("node", () => {
    it("creates a node with required fields", () => {
      const n = node({
        name: "endpoint",
        sql: "SELECT * FROM events",
      });

      expect(n._name).toBe("endpoint");
      expect(n.sql).toBe("SELECT * FROM events");
    });

    it("creates a node with description", () => {
      const n = node({
        name: "endpoint",
        description: "Main query node",
        sql: "SELECT * FROM events",
      });

      expect(n.description).toBe("Main query node");
    });
  });

  describe("definePipe", () => {
    it("creates a pipe with required fields", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      expect(pipe._name).toBe("my_pipe");
      expect(pipe._type).toBe("pipe");
      expect(pipe.options.nodes).toHaveLength(1);
    });

    it("creates a pipe with params", () => {
      const pipe = definePipe("my_pipe", {
        params: {
          start_date: p.dateTime(),
          limit: p.int32().optional(10),
        },
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      expect(pipe._params).toBeDefined();
      expect(pipe.options.params).toBeDefined();
    });

    it("creates a pipe with description", () => {
      const pipe = definePipe("my_pipe", {
        description: "A test pipe",
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      expect(pipe.options.description).toBe("A test pipe");
    });

    it("throws error for invalid pipe name", () => {
      expect(() =>
        definePipe("123invalid", {
          nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
          output: { value: t.int32() },
          endpoint: true,
        })
      ).toThrow("Invalid pipe name");

      expect(() =>
        definePipe("my-pipe", {
          nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
          output: { value: t.int32() },
          endpoint: true,
        })
      ).toThrow("Invalid pipe name");
    });

    it("throws error for empty nodes", () => {
      expect(() =>
        definePipe("my_pipe", {
          nodes: [],
          output: { value: t.int32() },
          endpoint: true,
        })
      ).toThrow("must have at least one node");
    });

    it("throws error for empty output", () => {
      expect(() =>
        definePipe("my_pipe", {
          nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
          output: {},
          endpoint: true,
        })
      ).toThrow("must have an output schema");
    });

    it("allows valid naming patterns", () => {
      const pipe1 = definePipe("_private_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });
      expect(pipe1._name).toBe("_private_pipe");

      const pipe2 = definePipe("pipe_v2", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });
      expect(pipe2._name).toBe("pipe_v2");
    });
  });

  describe("isPipeDefinition", () => {
    it("returns true for valid pipe", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      expect(isPipeDefinition(pipe)).toBe(true);
    });

    it("returns false for non-pipe objects", () => {
      expect(isPipeDefinition({})).toBe(false);
      expect(isPipeDefinition(null)).toBe(false);
      expect(isPipeDefinition(undefined)).toBe(false);
      expect(isPipeDefinition("string")).toBe(false);
      expect(isPipeDefinition(123)).toBe(false);
      expect(isPipeDefinition({ _name: "test" })).toBe(false);
    });
  });

  describe("getEndpointConfig", () => {
    it("returns null when endpoint is false", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: false,
      });

      expect(getEndpointConfig(pipe)).toBeNull();
    });

    it("returns config when endpoint is true", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      const config = getEndpointConfig(pipe);
      expect(config).toEqual({ enabled: true });
    });

    it("returns config with cache settings", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: {
          enabled: true,
          cache: { enabled: true, ttl: 300 },
        },
      });

      const config = getEndpointConfig(pipe);
      expect(config?.enabled).toBe(true);
      expect(config?.cache?.ttl).toBe(300);
    });

    it("returns null when endpoint config has enabled: false", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: {
          enabled: false,
        },
      });

      expect(getEndpointConfig(pipe)).toBeNull();
    });
  });

  describe("getNodeNames", () => {
    it("returns all node names", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [
          node({ name: "first", sql: "SELECT 1" }),
          node({ name: "second", sql: "SELECT 2" }),
          node({ name: "endpoint", sql: "SELECT 3" }),
        ],
        output: { value: t.int32() },
        endpoint: true,
      });

      const names = getNodeNames(pipe);
      expect(names).toEqual(["first", "second", "endpoint"]);
    });
  });

  describe("getNode", () => {
    it("returns node by name", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [
          node({ name: "first", sql: "SELECT 1" }),
          node({ name: "endpoint", sql: "SELECT 2" }),
        ],
        output: { value: t.int32() },
        endpoint: true,
      });

      const n = getNode(pipe, "first");
      expect(n?._name).toBe("first");
      expect(n?.sql).toBe("SELECT 1");
    });

    it("returns undefined for non-existent node", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      expect(getNode(pipe, "nonexistent")).toBeUndefined();
    });
  });

  describe("sql template helper", () => {
    it("interpolates datasource references", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });

      const query = sql`SELECT * FROM ${events}`;
      expect(query).toBe("SELECT * FROM events");
    });

    it("interpolates node references", () => {
      const n = node({ name: "aggregated", sql: "SELECT 1" });

      const query = sql`SELECT * FROM ${n}`;
      expect(query).toBe("SELECT * FROM aggregated");
    });

    it("interpolates string values", () => {
      const tableName = "events";
      const query = sql`SELECT * FROM ${tableName}`;
      expect(query).toBe("SELECT * FROM events");
    });

    it("interpolates number values", () => {
      const limit = 10;
      const query = sql`SELECT * FROM events LIMIT ${limit}`;
      expect(query).toBe("SELECT * FROM events LIMIT 10");
    });

    it("handles multiple interpolations", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });
      const limit = 100;

      const query = sql`SELECT * FROM ${events} WHERE id = ${"test"} LIMIT ${limit}`;
      expect(query).toBe("SELECT * FROM events WHERE id = test LIMIT 100");
    });

    it("handles no interpolations", () => {
      const query = sql`SELECT 1`;
      expect(query).toBe("SELECT 1");
    });
  });
});
