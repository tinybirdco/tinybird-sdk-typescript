import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import { deployToMain } from "./deploy.js";
import type { BuildConfig } from "./build.js";
import {
  BASE_URL,
  createDeploySuccessResponse,
  createDeploymentStatusResponse,
  createSetLiveSuccessResponse,
  createBuildFailureResponse,
  createBuildMultipleErrorsResponse,
  createDeploymentsListResponse,
} from "../test/handlers.js";
import type { GeneratedResources } from "../generator/index.js";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
beforeEach(() => {
  // Set up default handler for deployments list (used by stale deployment cleanup)
  server.use(
    http.get(`${BASE_URL}/v1/deployments`, () => {
      return HttpResponse.json(createDeploymentsListResponse());
    })
  );
});
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
    connections: [],
  };

  // Helper to set up successful deploy flow
  function setupSuccessfulDeployFlow(deploymentId = "deploy-abc") {
    server.use(
      http.post(`${BASE_URL}/v1/deploy`, () => {
        return HttpResponse.json(
          createDeploySuccessResponse({ deploymentId, status: "pending" })
        );
      }),
      http.get(`${BASE_URL}/v1/deployments/${deploymentId}`, () => {
        return HttpResponse.json(
          createDeploymentStatusResponse({ deploymentId, status: "data_ready" })
        );
      }),
      http.post(`${BASE_URL}/v1/deployments/${deploymentId}/set-live`, () => {
        return HttpResponse.json(createSetLiveSuccessResponse());
      })
    );
  }

  describe("deployToMain", () => {
    it("successfully deploys resources with full flow", async () => {
      setupSuccessfulDeployFlow("deploy-abc");

      const result = await deployToMain(config, resources, { pollIntervalMs: 1 });

      expect(result.success).toBe(true);
      expect(result.result).toBe("success");
      expect(result.buildId).toBe("deploy-abc");
      expect(result.datasourceCount).toBe(1);
      expect(result.pipeCount).toBe(1);
    });

    it("polls until deployment is ready", async () => {
      let pollCount = 0;

      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(
            createDeploySuccessResponse({ deploymentId: "deploy-poll", status: "pending" })
          );
        }),
        http.get(`${BASE_URL}/v1/deployments/deploy-poll`, () => {
          pollCount++;
          // Return pending for first 2 polls, then data_ready
          const status = pollCount < 3 ? "pending" : "data_ready";
          return HttpResponse.json(
            createDeploymentStatusResponse({ deploymentId: "deploy-poll", status })
          );
        }),
        http.post(`${BASE_URL}/v1/deployments/deploy-poll/set-live`, () => {
          return HttpResponse.json(createSetLiveSuccessResponse());
        })
      );

      const result = await deployToMain(config, resources, { pollIntervalMs: 1 });

      expect(result.success).toBe(true);
      expect(pollCount).toBe(3);
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
          return HttpResponse.json(
            createDeploySuccessResponse({ deploymentId: "deploy-url-test" })
          );
        }),
        http.get(`${BASE_URL}/v1/deployments/deploy-url-test`, () => {
          return HttpResponse.json(
            createDeploymentStatusResponse({ deploymentId: "deploy-url-test", status: "data_ready" })
          );
        }),
        http.post(`${BASE_URL}/v1/deployments/deploy-url-test/set-live`, () => {
          return HttpResponse.json(createSetLiveSuccessResponse());
        })
      );

      await deployToMain(config, resources, { pollIntervalMs: 1 });

      expect(capturedUrl).toBe(`${BASE_URL}/v1/deploy`);
    });

    it("handles failed deployment status", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(
            createDeploySuccessResponse({ deploymentId: "deploy-fail", status: "pending" })
          );
        }),
        http.get(`${BASE_URL}/v1/deployments/deploy-fail`, () => {
          return HttpResponse.json(
            createDeploymentStatusResponse({ deploymentId: "deploy-fail", status: "failed" })
          );
        })
      );

      const result = await deployToMain(config, resources, { pollIntervalMs: 1 });

      expect(result.success).toBe(false);
      expect(result.error).toContain("Deployment failed with status: failed");
    });

    it("handles set-live failure", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(
            createDeploySuccessResponse({ deploymentId: "deploy-setlive-fail" })
          );
        }),
        http.get(`${BASE_URL}/v1/deployments/deploy-setlive-fail`, () => {
          return HttpResponse.json(
            createDeploymentStatusResponse({ deploymentId: "deploy-setlive-fail", status: "data_ready" })
          );
        }),
        http.post(`${BASE_URL}/v1/deployments/deploy-setlive-fail/set-live`, () => {
          return HttpResponse.json({ error: "Set live failed" }, { status: 500 });
        })
      );

      const result = await deployToMain(config, resources, { pollIntervalMs: 1 });

      expect(result.success).toBe(false);
      expect(result.error).toContain("Failed to set deployment as live");
    });

    it("normalizes baseUrl with trailing slash", async () => {
      let capturedUrl: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v1/deploy`, ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json(
            createDeploySuccessResponse({ deploymentId: "deploy-slash" })
          );
        }),
        http.get(`${BASE_URL}/v1/deployments/deploy-slash`, () => {
          return HttpResponse.json(
            createDeploymentStatusResponse({ deploymentId: "deploy-slash", status: "data_ready" })
          );
        }),
        http.post(`${BASE_URL}/v1/deployments/deploy-slash/set-live`, () => {
          return HttpResponse.json(createSetLiveSuccessResponse());
        })
      );

      await deployToMain(
        { ...config, baseUrl: `${BASE_URL}/` },
        resources,
        { pollIntervalMs: 1 }
      );

      expect(capturedUrl).toBe(`${BASE_URL}/v1/deploy`);
    });

    it("times out when deployment never becomes ready", async () => {
      server.use(
        http.post(`${BASE_URL}/v1/deploy`, () => {
          return HttpResponse.json(
            createDeploySuccessResponse({ deploymentId: "deploy-timeout", status: "pending" })
          );
        }),
        http.get(`${BASE_URL}/v1/deployments/deploy-timeout`, () => {
          return HttpResponse.json(
            createDeploymentStatusResponse({ deploymentId: "deploy-timeout", status: "pending" })
          );
        })
      );

      const result = await deployToMain(config, resources, {
        pollIntervalMs: 1,
        maxPollAttempts: 3,
      });

      expect(result.success).toBe(false);
      expect(result.error).toContain("Deployment timed out");
    });
  });
});
