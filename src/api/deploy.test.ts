import { describe, it, expect, beforeAll, afterEach, afterAll, vi } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import { deployToMain } from "./deploy.js";
import type { BuildConfig } from "./build.js";
import {
  BASE_URL,
  createDeploySuccessResponse,
  createBuildFailureResponse,
  createBuildMultipleErrorsResponse,
} from "../test/handlers.js";
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
    datasources: [{ name: "events", content: "SCHEMA > timestamp DateTime" }],
    pipes: [{ name: "top_events", content: "NODE main\nSQL > SELECT * FROM events" }],
    connections: [],
  };

  function setupAutoPromoteSuccessFlow(deploymentId = "deploy-abc") {
    server.use(
      http.post(`${BASE_URL}/v1/deploy`, () => {
        return HttpResponse.json(
          createDeploySuccessResponse({ deploymentId, status: "pending" })
        );
      }),
      http.get(`${BASE_URL}/v1/deployments/${deploymentId}`, () => {
        return HttpResponse.json({
          result: "success",
          deployment: {
            id: deploymentId,
            status: "data_ready",
            live: true,
          },
        });
      })
    );
  }

  describe("deployToMain", () => {
    it("successfully deploys resources with auto-promote flow", async () => {
      setupAutoPromoteSuccessFlow("deploy-abc");

      const onDeploymentLive = vi.fn();
      const result = await deployToMain(config, resources, {
        pollIntervalMs: 1,
        callbacks: { onDeploymentLive },
      });

      expect(result.success).toBe(true);
      expect(result.result).toBe("success");
      expect(result.buildId).toBe("deploy-abc");
      expect(result.datasourceCount).toBe(1);
      expect(result.pipeCount).toBe(1);
      expect(onDeploymentLive).toHaveBeenCalledWith("deploy-abc");
    });

    it("handles deploy failure with single error", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(createBuildFailureResponse("Permission denied"), {
            status: 200,
          });
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
          return HttpResponse.json({ result: "failed", error: "Forbidden" }, { status: 403 });
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

      await expect(deployToMain(config, resources)).rejects.toThrow("Failed to parse response");
    });

    it("uses /v1/deploy endpoint and sends auto_promote by default", async () => {
      let capturedUrl: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v1/deploy`, ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json(
            createDeploySuccessResponse({ deploymentId: "deploy-url-test", status: "pending" })
          );
        }),
        http.get(`${BASE_URL}/v1/deployments/deploy-url-test`, () => {
          return HttpResponse.json({
            result: "success",
            deployment: { id: "deploy-url-test", status: "data_ready", live: true },
          });
        })
      );

      await deployToMain(config, resources, { pollIntervalMs: 1 });

      const parsed = new URL(capturedUrl ?? "");
      expect(parsed.pathname).toBe("/v1/deploy");
      expect(parsed.searchParams.get("from")).toBe("ts-sdk");
      expect(parsed.searchParams.get("auto_promote")).toBe("true");
    });

    it("passes allow_destructive_operations when explicitly enabled", async () => {
      let capturedUrl: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v1/deploy`, ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json(
            createDeploySuccessResponse({ deploymentId: "deploy-destructive", status: "pending" })
          );
        }),
        http.get(`${BASE_URL}/v1/deployments/deploy-destructive`, () => {
          return HttpResponse.json({
            result: "success",
            deployment: { id: "deploy-destructive", status: "data_ready", live: true },
          });
        })
      );

      await deployToMain(config, resources, {
        pollIntervalMs: 1,
        allowDestructiveOperations: true,
      });

      const parsed = new URL(capturedUrl ?? "");
      expect(parsed.searchParams.get("allow_destructive_operations")).toBe("true");
      expect(parsed.searchParams.get("auto_promote")).toBe("true");
    });

    it("does not send auto_promote in check mode", async () => {
      let capturedUrl: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v1/deploy`, ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json({ result: "success" });
        })
      );

      const result = await deployToMain(config, resources, { check: true });

      expect(result.success).toBe(true);
      const parsed = new URL(capturedUrl ?? "");
      expect(parsed.searchParams.get("check")).toBe("true");
      expect(parsed.searchParams.get("auto_promote")).toBeNull();
    });

    it("adds actionable guidance to Forward/Classic workspace errors", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(
            {
              result: "failed",
              error:
                "This is a Tinybird Forward workspace, and this operation is only available for Tinybird Classic workspaces.",
            },
            { status: 400 }
          );
        })
      );

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.error).toContain("Tinybird Forward workspace");
      expect(result.error).toContain(
        "Use the Tinybird Classic CLI (`tb`) from a Tinybird Classic workspace for this operation."
      );
    });

    it("normalizes baseUrl with trailing slash", async () => {
      let capturedUrl: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v1/deploy`, ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json(
            createDeploySuccessResponse({ deploymentId: "deploy-slash", status: "pending" })
          );
        }),
        http.get(`${BASE_URL}/v1/deployments/deploy-slash`, () => {
          return HttpResponse.json({
            result: "success",
            deployment: { id: "deploy-slash", status: "data_ready", live: true },
          });
        })
      );

      await deployToMain(
        { ...config, baseUrl: `${BASE_URL}/` },
        resources,
        { pollIntervalMs: 1 }
      );

      const parsed = new URL(capturedUrl ?? "");
      expect(parsed.pathname).toBe("/v1/deploy");
      expect(parsed.searchParams.get("from")).toBe("ts-sdk");
      expect(parsed.searchParams.get("auto_promote")).toBe("true");
    });
  });
});
