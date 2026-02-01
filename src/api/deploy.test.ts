import { describe, it, expect, beforeAll, afterEach, afterAll } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import { deployToMain } from "./deploy.js";
import type { BuildConfig } from "./build.js";
import {
  BASE_URL,
  createBuildSuccessResponse,
  createBuildFailureResponse,
  createBuildMultipleErrorsResponse,
  createNoChangesResponse,
} from "../test/msw-handlers.js";
import type { GeneratedResources } from "../generator/index.js";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("Deploy API", () => {
  const config: BuildConfig = {
    baseUrl: BASE_URL,
    token: "p.test-token",
  };

  const resources: GeneratedResources = {
    datasources: [
      { name: "events", content: "SCHEMA > timestamp DateTime" },
    ],
    pipes: [
      { name: "top_events", content: "NODE main\nSQL > SELECT * FROM events" },
    ],
  };

  describe("deployToMain", () => {
    it("successfully deploys resources", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(
            createBuildSuccessResponse({
              buildId: "deploy-abc",
              newPipes: ["top_events"],
              newDatasources: ["events"],
            })
          );
        })
      );

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(true);
      expect(result.result).toBe("success");
      expect(result.buildId).toBe("deploy-abc");
      expect(result.datasourceCount).toBe(1);
      expect(result.pipeCount).toBe(1);
      expect(result.pipes?.created).toEqual(["top_events"]);
      expect(result.datasources?.created).toEqual(["events"]);
    });

    it("handles no changes response", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(createNoChangesResponse());
        })
      );

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(true);
      expect(result.result).toBe("no_changes");
    });

    it("handles deploy failure with single error", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(
            createBuildFailureResponse("Permission denied"),
            { status: 200 }
          );
        })
      );

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.result).toBe("failed");
      expect(result.error).toBe("Permission denied");
    });

    it("handles deploy failure with multiple errors", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(
            createBuildMultipleErrorsResponse([
              { filename: "events.datasource", error: "Schema mismatch" },
              { error: "General error without filename" },
            ]),
            { status: 200 }
          );
        })
      );

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.error).toContain("[events.datasource] Schema mismatch");
      expect(result.error).toContain("General error without filename");
    });

    it("handles HTTP error responses", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(
            { result: "failed", error: "Forbidden" },
            { status: 403 }
          );
        })
      );

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.error).toBe("Forbidden");
    });

    it("handles malformed JSON response", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return new HttpResponse("invalid json {", {
            status: 200,
            headers: { "Content-Type": "text/plain" },
          });
        })
      );

      await expect(deployToMain(config, resources)).rejects.toThrow(
        "Failed to parse response"
      );
    });

    it("uses /v1/deploy endpoint (not /v1/build)", async () => {
      let capturedUrl: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v1/deploy`, ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json(createBuildSuccessResponse());
        })
      );

      await deployToMain(config, resources);

      expect(capturedUrl).toBe(`${BASE_URL}/v1/deploy`);
    });

    it("tracks changed and deleted resources", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(
            createBuildSuccessResponse({
              changedPipes: ["top_events"],
              deletedDatasources: ["old_ds"],
            })
          );
        })
      );

      const result = await deployToMain(config, resources);

      expect(result.pipes?.changed).toEqual(["top_events"]);
      expect(result.datasources?.deleted).toEqual(["old_ds"]);
    });

    it("normalizes baseUrl with trailing slash", async () => {
      let capturedUrl: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v1/deploy`, ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json(createBuildSuccessResponse());
        })
      );

      await deployToMain(
        { ...config, baseUrl: `${BASE_URL}/` },
        resources
      );

      expect(capturedUrl).toBe(`${BASE_URL}/v1/deploy`);
    });
  });
});
