import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  defineProject,
  createTinybirdClient,
  isProjectDefinition,
  getDatasourceNames,
  getPipeNames,
  getDatasource,
  getPipe,
} from "./project.js";
import { defineDatasource } from "./datasource.js";
import { definePipe, node } from "./pipe.js";
import { t } from "./types.js";

describe("Project Schema", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    process.env = { ...originalEnv };
    delete process.env.VERCEL_ENV;
    delete process.env.GITHUB_HEAD_REF;
    delete process.env.CI_MERGE_REQUEST_SOURCE_BRANCH_NAME;
    delete process.env.CI;
    delete process.env.TINYBIRD_PREVIEW_MODE;
    delete process.env.VERCEL_GIT_COMMIT_REF;
    delete process.env.GITHUB_REF_NAME;
    delete process.env.CI_COMMIT_BRANCH;
    delete process.env.CIRCLE_BRANCH;
    delete process.env.BUILD_SOURCEBRANCHNAME;
    delete process.env.BITBUCKET_BRANCH;
    delete process.env.TINYBIRD_BRANCH_NAME;
    delete process.env.TINYBIRD_BRANCH_TOKEN;
  });

  afterEach(() => {
    process.env = { ...originalEnv };
  });

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

    it("creates tinybird client with pipe and datasource accessors", () => {
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

      expect(typeof project.tinybird.topEvents.query).toBe("function");
      expect(typeof project.tinybird.events.ingest).toBe("function");
      expect(typeof project.tinybird.events.append).toBe("function");
      expect(typeof project.tinybird.events.replace).toBe("function");
      expect((project.tinybird as unknown as Record<string, unknown>).query).toBeUndefined();
      expect((project.tinybird as unknown as Record<string, unknown>).pipes).toBeUndefined();
    });

    it("creates datasource accessors with ingest/ingestBatch/append/replace/delete/truncate methods", () => {
      const events = defineDatasource("events", {
        schema: { timestamp: t.dateTime() },
      });

      const project = defineProject({
        datasources: { events },
      });

      expect(project.tinybird.events).toBeDefined();
      expect(typeof project.tinybird.events.ingest).toBe("function");
      expect(typeof project.tinybird.events.ingestBatch).toBe("function");
      expect(typeof project.tinybird.events.append).toBe("function");
      expect(typeof project.tinybird.events.replace).toBe("function");
      expect(typeof project.tinybird.events.delete).toBe("function");
      expect(typeof project.tinybird.events.truncate).toBe("function");
    });

    it("creates multiple datasource accessors", () => {
      const events = defineDatasource("events", {
        schema: { timestamp: t.dateTime() },
      });
      const pageViews = defineDatasource("page_views", {
        schema: { pathname: t.string() },
      });

      const project = defineProject({
        datasources: { events, pageViews },
      });

      expect(project.tinybird.events).toBeDefined();
      expect(project.tinybird.pageViews).toBeDefined();
      expect(typeof project.tinybird.events.ingest).toBe("function");
      expect(typeof project.tinybird.pageViews.ingest).toBe("function");
      expect(typeof project.tinybird.events.append).toBe("function");
      expect(typeof project.tinybird.pageViews.append).toBe("function");
      expect(typeof project.tinybird.events.replace).toBe("function");
      expect(typeof project.tinybird.pageViews.replace).toBe("function");
      expect(typeof project.tinybird.events.delete).toBe("function");
      expect(typeof project.tinybird.pageViews.delete).toBe("function");
      expect(typeof project.tinybird.events.truncate).toBe("function");
      expect(typeof project.tinybird.pageViews.truncate).toBe("function");
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

      // Cast to any since the type system expects params but stub throws regardless
      const queryFn = project.tinybird.internalPipe.query as () => Promise<unknown>;
      await expect(queryFn()).rejects.toThrow(
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

  describe("createTinybirdClient", () => {
    const originalNodeEnv = process.env.NODE_ENV;

    beforeEach(() => {
      vi.resetModules();
    });

    afterEach(() => {
      process.env.NODE_ENV = originalNodeEnv;
    });

    it("creates a client with pipe and datasource accessors", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });

      const topEvents = definePipe("top_events", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { count: t.int64() },
        endpoint: true,
      });

      const client = createTinybirdClient({
        datasources: { events },
        pipes: { topEvents },
      });

      expect(client.sql).toBeDefined();
      expect(typeof client.topEvents.query).toBe("function");
      expect(typeof client.events.ingest).toBe("function");
      expect(typeof client.events.append).toBe("function");
      expect(typeof client.events.replace).toBe("function");
      expect(typeof client.sql).toBe("function");
      expect((client as unknown as Record<string, unknown>).query).toBeUndefined();
      expect((client as unknown as Record<string, unknown>).pipes).toBeUndefined();
    });

    it("creates datasource accessors with ingest/ingestBatch/append/replace/delete/truncate methods", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });

      const client = createTinybirdClient({
        datasources: { events },
        pipes: {},
      });

      expect(client.events).toBeDefined();
      expect(typeof client.events.ingest).toBe("function");
      expect(typeof client.events.ingestBatch).toBe("function");
      expect(typeof client.events.append).toBe("function");
      expect(typeof client.events.replace).toBe("function");
      expect(typeof client.events.delete).toBe("function");
      expect(typeof client.events.truncate).toBe("function");
    });

    it("creates multiple datasource accessors", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });
      const pageViews = defineDatasource("page_views", {
        schema: { pathname: t.string() },
      });

      const client = createTinybirdClient({
        datasources: { events, pageViews },
        pipes: {},
      });

      expect(client.events).toBeDefined();
      expect(client.pageViews).toBeDefined();
      expect(typeof client.events.ingest).toBe("function");
      expect(typeof client.pageViews.ingest).toBe("function");
      expect(typeof client.events.append).toBe("function");
      expect(typeof client.pageViews.append).toBe("function");
      expect(typeof client.events.replace).toBe("function");
      expect(typeof client.pageViews.replace).toBe("function");
      expect(typeof client.events.delete).toBe("function");
      expect(typeof client.pageViews.delete).toBe("function");
      expect(typeof client.events.truncate).toBe("function");
      expect(typeof client.pageViews.truncate).toBe("function");
    });

    it("accepts devMode option", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });

      // Should not throw when devMode is explicitly set
      const clientWithDevMode = createTinybirdClient({
        datasources: { events },
        pipes: {},
        devMode: true,
      });

      expect(clientWithDevMode.events.ingest).toBeDefined();

      const clientWithoutDevMode = createTinybirdClient({
        datasources: { events },
        pipes: {},
        devMode: false,
      });

      expect(clientWithoutDevMode.events.ingest).toBeDefined();
    });

    it("accepts all configuration options", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });
      const customFetch = vi.fn();

      // Should accept all options without throwing
      const client = createTinybirdClient({
        datasources: { events },
        pipes: {},
        baseUrl: "https://custom.tinybird.co",
        token: "test-token",
        fetch: customFetch as typeof fetch,
        configDir: "/custom/config/dir",
        devMode: true,
      });

      expect(client.events.ingest).toBeDefined();
    });

    it("throws error when accessing underlying client before initialization", () => {
      const client = createTinybirdClient({
        datasources: {},
        pipes: {},
      });

      expect(() => client.client).toThrow("Client not initialized");
    });

    it("does not allow datasource names to overwrite internal client state", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });

      const client = createTinybirdClient({
        datasources: { _client: events },
        pipes: {},
      });

      expect(client._client).toBeDefined();
      expect(() => client.client).toThrow("Client not initialized");
    });

    it("does not allow datasource names to overwrite internal options state", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });

      const client = createTinybirdClient({
        datasources: { _options: events },
        pipes: {},
      });

      expect(client._options).toBeDefined();
      expect(() => client.client).toThrow("Client not initialized");
    });

    it("uses custom fetch for typed endpoint queries", async () => {
      const topEvents = definePipe("top_events", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { count: t.int64() },
        endpoint: true,
      });
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            data: [],
            meta: [],
            rows: 0,
            statistics: {
              elapsed: 0,
              rows_read: 0,
              bytes_read: 0,
            },
          }),
      });
      const originalFetch = global.fetch;
      const globalFetch = vi.fn().mockRejectedValue(
        new Error("global fetch should not be called")
      );
      global.fetch = globalFetch as typeof fetch;

      try {
        const client = createTinybirdClient({
          datasources: {},
          pipes: { topEvents },
          baseUrl: "https://api.tinybird.co",
          token: "test-token",
          fetch: customFetch as typeof fetch,
          devMode: false,
        });

        const result = await client.topEvents.query({});

        expect(result.rows).toBe(0);
        expect(customFetch).toHaveBeenCalledTimes(1);
        expect(globalFetch).not.toHaveBeenCalled();

        const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
        const parsed = new URL(url);
        expect(parsed.pathname).toBe("/v0/pipes/top_events.json");
        expect(parsed.searchParams.get("from")).toBe("ts-sdk");
        expect(new Headers(init.headers).get("Authorization")).toBe(
          "Bearer test-token"
        );
      } finally {
        global.fetch = originalFetch;
      }
    });
  });
});
