import { describe, it, expect, beforeAll, afterEach, afterAll } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import { buildToTinybird, validateBuildConfig, type BuildConfig } from "./build.js";
import {
  BASE_URL,
  createBuildSuccessResponse,
  createBuildFailureResponse,
  createBuildMultipleErrorsResponse,
  createNoChangesResponse,
} from "../test/handlers.js";
import type { GeneratedResources } from "../generator/index.js";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("Build API", () => {
  const config: BuildConfig = {
    baseUrl: BASE_URL,
    token: "p.test-token",
  };

  const resources: GeneratedResources = {
    datasources: [
      { name: "events", content: "SCHEMA > timestamp DateTime" },
      { name: "users", content: "SCHEMA > id String" },
    ],
    pipes: [
      { name: "top_events", content: "NODE main\nSQL > SELECT * FROM events" },
    ],
    connections: [],
  };

  describe("buildToTinybird", () => {
    it("successfully builds resources", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/build?from=ts-sdk`, () => {
          return HttpResponse.json(
            createBuildSuccessResponse({
              buildId: "build-abc",
              newPipes: ["top_events"],
              newDatasources: ["events", "users"],
            })
          );
        })
      );

      const result = await buildToTinybird(config, resources);

      expect(result.success).toBe(true);
      expect(result.result).toBe("success");
      expect(result.buildId).toBe("build-abc");
      expect(result.datasourceCount).toBe(2);
      expect(result.pipeCount).toBe(1);
      expect(result.pipes?.created).toEqual(["top_events"]);
      expect(result.datasources?.created).toEqual(["events", "users"]);
    });

    it("handles no changes response", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/build?from=ts-sdk`, () => {
          return HttpResponse.json(createNoChangesResponse());
        })
      );

      const result = await buildToTinybird(config, resources);

      expect(result.success).toBe(true);
      expect(result.result).toBe("no_changes");
    });

    it("handles build failure with single error", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/build?from=ts-sdk`, () => {
          return HttpResponse.json(
            createBuildFailureResponse("Invalid SQL syntax"),
            { status: 200 }
          );
        })
      );

      const result = await buildToTinybird(config, resources);

      expect(result.success).toBe(false);
      expect(result.result).toBe("failed");
      expect(result.error).toBe("Invalid SQL syntax");
    });

    it("handles build failure with multiple errors", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/build?from=ts-sdk`, () => {
          return HttpResponse.json(
            createBuildMultipleErrorsResponse([
              { filename: "events.datasource", error: "Invalid schema" },
              { filename: "top_events.pipe", error: "Unknown column" },
            ]),
            { status: 200 }
          );
        })
      );

      const result = await buildToTinybird(config, resources);

      expect(result.success).toBe(false);
      expect(result.error).toContain("[events.datasource] Invalid schema");
      expect(result.error).toContain("[top_events.pipe] Unknown column");
    });

    it("handles HTTP error responses", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/build?from=ts-sdk`, () => {
          return HttpResponse.json(
            { result: "failed", error: "Unauthorized" },
            { status: 401 }
          );
        })
      );

      const result = await buildToTinybird(config, resources);

      expect(result.success).toBe(false);
      expect(result.error).toBe("Unauthorized");
    });

    it("handles malformed JSON response", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/build?from=ts-sdk`, () => {
          return new HttpResponse("not json", {
            status: 200,
            headers: { "Content-Type": "text/plain" },
          });
        })
      );

      await expect(buildToTinybird(config, resources)).rejects.toThrow(
        "Failed to parse response"
      );
    });

    it("tracks changed pipes and datasources", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/build?from=ts-sdk`, () => {
          return HttpResponse.json(
            createBuildSuccessResponse({
              changedPipes: ["top_events"],
              changedDatasources: ["events"],
              deletedPipes: ["old_pipe"],
            })
          );
        })
      );

      const result = await buildToTinybird(config, resources);

      expect(result.pipes?.changed).toEqual(["top_events"]);
      expect(result.pipes?.deleted).toEqual(["old_pipe"]);
      expect(result.datasources?.changed).toEqual(["events"]);
      // Deprecated fields should still work
      expect(result.changedPipeNames).toEqual(["top_events"]);
    });

    it("sends correct authorization header", async () => {
      let capturedAuth: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v1/build?from=ts-sdk`, ({ request }) => {
          capturedAuth = request.headers.get("Authorization");
          return HttpResponse.json(createBuildSuccessResponse());
        })
      );

      await buildToTinybird(config, resources);

      expect(capturedAuth).toBe("Bearer p.test-token");
    });

    it("sends resources as multipart form data", async () => {
      let capturedFormData: FormData | null = null;

      server.use(
        http.post(`${BASE_URL}/v1/build?from=ts-sdk`, async ({ request }) => {
          capturedFormData = await request.formData();
          return HttpResponse.json(createBuildSuccessResponse());
        })
      );

      await buildToTinybird(config, resources);

      expect(capturedFormData).not.toBeNull();
      // FormData has 3 entries: 2 datasources + 1 pipe
      // Use getAll since FormData.entries() is not available in Node.js types
      const allValues = capturedFormData!.getAll("data_project://");
      expect(allValues.length).toBe(3);
    });
  });

  describe("validateBuildConfig", () => {
    it("passes with valid config", () => {
      expect(() => validateBuildConfig(config)).not.toThrow();
    });

    it("throws on missing baseUrl", () => {
      expect(() =>
        validateBuildConfig({ token: "test" })
      ).toThrow("Missing baseUrl");
    });

    it("throws on missing token", () => {
      expect(() =>
        validateBuildConfig({ baseUrl: "https://api.tinybird.co" })
      ).toThrow("Missing token");
    });
  });
});
