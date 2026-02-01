import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  defineProject,
  isProjectDefinition,
  getDatasourceNames,
  getPipeNames,
  getDatasource,
  getPipe,
} from "./project.js";
import { defineDatasource } from "./datasource.js";
import { definePipe, node } from "./pipe.js";
import { t } from "./types.js";
import { p } from "./params.js";

describe("Project Schema", () => {
  describe("defineProject", () => {
    it("creates a project with empty config", () => {
      const project = defineProject({});

      expect(project._type).toBe("project");
      expect(project.datasources).toEqual({});
      expect(project.pipes).toEqual({});
    });

    it("creates a project with datasources", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string(), timestamp: t.dateTime() },
      });

      const project = defineProject({
        datasources: { events },
      });

      expect(project.datasources.events).toBe(events);
      expect(project.pipes).toEqual({});
    });

    it("creates a project with pipes", () => {
      const topEvents = definePipe("top_events", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { count: t.int64() },
        endpoint: true,
      });

      const project = defineProject({
        pipes: { topEvents },
      });

      expect(project.pipes.topEvents).toBe(topEvents);
      expect(project.datasources).toEqual({});
    });

    it("creates a project with both datasources and pipes", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });

      const topEvents = definePipe("top_events", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { count: t.int64() },
        endpoint: true,
      });

      const project = defineProject({
        datasources: { events },
        pipes: { topEvents },
      });

      expect(project.datasources.events).toBe(events);
      expect(project.pipes.topEvents).toBe(topEvents);
    });

    it("creates tinybird client with query and ingest methods", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });

      const topEvents = definePipe("top_events", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { count: t.int64() },
        endpoint: true,
      });

      const project = defineProject({
        datasources: { events },
        pipes: { topEvents },
      });

      expect(project.tinybird.query).toBeDefined();
      expect(project.tinybird.ingest).toBeDefined();
      expect(typeof project.tinybird.query.topEvents).toBe("function");
      expect(typeof project.tinybird.ingest.events).toBe("function");
      expect(typeof project.tinybird.ingest.eventsBatch).toBe("function");
    });

    it("throws error when accessing client before initialization", () => {
      const project = defineProject({});

      expect(() => project.tinybird.client).toThrow(
        "Client not initialized"
      );
    });

    it("creates stub for non-endpoint pipes that throws clear error", async () => {
      const internalPipe = definePipe("internal_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { count: t.int64() },
        endpoint: false,
      });

      const project = defineProject({
        pipes: { internalPipe },
      });

      await expect(project.tinybird.query.internalPipe()).rejects.toThrow(
        'Pipe "internalPipe" is not exposed as an endpoint'
      );
    });
  });

  describe("isProjectDefinition", () => {
    it("returns true for valid project", () => {
      const project = defineProject({});

      expect(isProjectDefinition(project)).toBe(true);
    });

    it("returns false for non-project objects", () => {
      expect(isProjectDefinition({})).toBe(false);
      expect(isProjectDefinition(null)).toBe(false);
      expect(isProjectDefinition(undefined)).toBe(false);
      expect(isProjectDefinition("string")).toBe(false);
      expect(isProjectDefinition(123)).toBe(false);
      expect(isProjectDefinition({ _type: "project" })).toBe(false);
    });
  });

  describe("getDatasourceNames", () => {
    it("returns all datasource names", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });
      const users = defineDatasource("users", {
        schema: { id: t.string() },
      });

      const project = defineProject({
        datasources: { events, users },
      });

      const names = getDatasourceNames(project);

      expect(names).toHaveLength(2);
      expect(names).toContain("events");
      expect(names).toContain("users");
    });

    it("returns empty array for project with no datasources", () => {
      const project = defineProject({});

      const names = getDatasourceNames(project);

      expect(names).toHaveLength(0);
    });
  });

  describe("getPipeNames", () => {
    it("returns all pipe names", () => {
      const topEvents = definePipe("top_events", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { count: t.int64() },
        endpoint: true,
      });
      const userActivity = definePipe("user_activity", {
        nodes: [node({ name: "endpoint", sql: "SELECT 2" })],
        output: { count: t.int64() },
        endpoint: true,
      });

      const project = defineProject({
        pipes: { topEvents, userActivity },
      });

      const names = getPipeNames(project);

      expect(names).toHaveLength(2);
      expect(names).toContain("topEvents");
      expect(names).toContain("userActivity");
    });

    it("returns empty array for project with no pipes", () => {
      const project = defineProject({});

      const names = getPipeNames(project);

      expect(names).toHaveLength(0);
    });
  });

  describe("getDatasource", () => {
    it("returns datasource by name", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });

      const project = defineProject({
        datasources: { events },
      });

      const retrieved = getDatasource(project, "events");

      expect(retrieved).toBe(events);
      expect(retrieved._name).toBe("events");
    });
  });

  describe("getPipe", () => {
    it("returns pipe by name", () => {
      const topEvents = definePipe("top_events", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { count: t.int64() },
        endpoint: true,
      });

      const project = defineProject({
        pipes: { topEvents },
      });

      const retrieved = getPipe(project, "topEvents");

      expect(retrieved).toBe(topEvents);
      expect(retrieved._name).toBe("top_events");
    });
  });
});
