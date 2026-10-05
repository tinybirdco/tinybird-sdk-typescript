import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll, vi } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";

import { deployToMain, MAX_CONSECUTIVE_FAILED_POLLS } from "./deploy.js";
import type { BuildConfig } from "./build.js";
import { BASE_URL, createDeploymentStatusResponse, createDeploymentsListResponse } from "../test/handlers.js";
import type { GeneratedResources } from "../generator/index.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const FAKE_TB = join(__dirname, "..", "test", "fixtures", "fake-tb-cli.mjs");

// deployToMain now shells out to the `tb` binary (via src/cli-binary/), so
// these tests drive it through the fake-tb-cli.mjs fixture instead of
// mocking a POST /v1/deploy. MSW still covers the two HTTP calls deployToMain
// makes directly: stale-deployment cleanup (/v1/deployments) and status
// polling (/v1/deployments/:id) — see the file header comment in deploy.ts
// for why those stayed local instead of moving into the binary call.
//
// Skipped on Windows: the fixture is a POSIX shebang script spawned directly
// (same constraint as src/cli-binary/run.test.ts).
const describePosix = process.platform === "win32" ? describe.skip : describe;

const server = setupServer();

beforeAll(() => {
  server.listen({ onUnhandledRequest: "error" });
  vi.stubGlobal("setTimeout", (fn: () => void) => {
    Promise.resolve().then(fn);
  });
});
beforeEach(() => {
  // Default handler for the stale-deployment cleanup list call.
  server.use(
    http.get(`${BASE_URL}/v1/deployments`, () => {
      return HttpResponse.json(createDeploymentsListResponse());
    })
  );
});
afterEach(() => server.resetHandlers());
afterAll(() => {
  server.close();
  vi.unstubAllGlobals();
});

describePosix("Deploy API (via tb binary)", () => {
  const config: BuildConfig = {
    baseUrl: BASE_URL,
    token: "p.test-token",
  };

  const resources: GeneratedResources = {
    datasources: [{ name: "events", content: "SCHEMA > timestamp DateTime" }],
    pipes: [{ name: "top_events", content: "NODE main\nSQL > SELECT * FROM events" }],
    connections: [],
  };

  const originalBinaryPath = process.env.TB_CLI_BINARY_PATH;

  beforeEach(() => {
    process.env.TB_CLI_BINARY_PATH = FAKE_TB;
  });

  afterEach(() => {
    if (originalBinaryPath === undefined) delete process.env.TB_CLI_BINARY_PATH;
    else process.env.TB_CLI_BINARY_PATH = originalBinaryPath;
    delete process.env.FAKE_TB_STDOUT;
    delete process.env.FAKE_TB_STDERR;
    delete process.env.FAKE_TB_EXIT_CODE;
    delete process.env.FAKE_TB_ECHO_ARGV;
  });

  /** Sets the fake binary to emit a successful `deployment create` JSON. */
  function setFakeTbDeploymentJson(fields: Record<string, unknown>) {
    process.env.FAKE_TB_STDOUT = JSON.stringify(fields);
    process.env.FAKE_TB_EXIT_CODE = "0";
  }

  // Helper to set up a successful deploy flow. By default the deployment
  // shows up as live on the very first status poll, matching auto_promote.
  function setupSuccessfulDeployFlow(deploymentId = "deploy-abc") {
    setFakeTbDeploymentJson({ id: deploymentId, status: "pending" });
    server.use(
      http.get(`${BASE_URL}/v1/deployments/${deploymentId}`, () => {
        return HttpResponse.json(
          createDeploymentStatusResponse({ deploymentId, status: "data_ready", live: true })
        );
      })
    );
  }

  describe("deployToMain", () => {
    it("successfully deploys resources with full flow", async () => {
      setupSuccessfulDeployFlow("deploy-abc");

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(true);
      expect(result.result).toBe("success");
      expect(result.buildId).toBe("deploy-abc");
      expect(result.datasourceCount).toBe(1);
      expect(result.pipeCount).toBe(1);
    });

    it("polls until deployment is ready and live", async () => {
      let pollCount = 0;
      setFakeTbDeploymentJson({ id: "deploy-poll", status: "pending" });

      server.use(
        http.get(`${BASE_URL}/v1/deployments/deploy-poll`, () => {
          pollCount++;
          // pending → data_ready (not live yet) → data_ready + live.
          if (pollCount < 3) {
            return HttpResponse.json(
              createDeploymentStatusResponse({ deploymentId: "deploy-poll", status: "pending", live: false })
            );
          }
          if (pollCount === 3) {
            return HttpResponse.json(
              createDeploymentStatusResponse({ deploymentId: "deploy-poll", status: "data_ready", live: false })
            );
          }
          return HttpResponse.json(
            createDeploymentStatusResponse({ deploymentId: "deploy-poll", status: "data_ready", live: true })
          );
        })
      );

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(true);
      expect(pollCount).toBe(4);
    });

    it("handles deploy failure with single error", async () => {
      process.env.FAKE_TB_STDOUT = JSON.stringify({
        id: "deploy-fail",
        status: "failed",
        errors: [{ error: "Permission denied" }],
      });
      process.env.FAKE_TB_EXIT_CODE = "1";

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.result).toBe("failed");
      expect(result.error).toBe("Permission denied");
    });

    it("handles deploy failure with multiple errors", async () => {
      process.env.FAKE_TB_STDOUT = JSON.stringify({
        id: "deploy-fail",
        status: "failed",
        errors: [
          { filename: "events.datasource", error: "Schema mismatch" },
          { error: "General error without filename" },
        ],
      });
      process.env.FAKE_TB_EXIT_CODE = "1";

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.error).toContain("[events.datasource] Schema mismatch");
      expect(result.error).toContain("General error without filename");
    });

    it("handles deployment feedback entries with null resource", async () => {
      process.env.FAKE_TB_STDOUT = JSON.stringify({
        id: "deploy-null-resource",
        status: "failed",
        feedback: [{ resource: null, level: "ERROR", message: "Invalid token" }],
      });
      process.env.FAKE_TB_EXIT_CODE = "1";

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.result).toBe("failed");
      expect(result.error).toContain("unknown: Invalid token");
    });

    it("returns a failed result (not a throw) when the binary fails with no JSON output", async () => {
      process.env.FAKE_TB_STDERR = "boom: unexpected error";
      process.env.FAKE_TB_EXIT_CODE = "1";

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.error).toContain("boom: unexpected error");
    });

    it("returns no_changes (not a throw) when the binary exits 0 with no JSON output", async () => {
      // FAKE_TB_STDOUT/FAKE_TB_EXIT_CODE unset -> empty stdout, exit 0.
      const result = await deployToMain(config, resources);

      expect(result).toMatchObject({ success: true, result: "no_changes" });
    });

    it("omits live-wait when auto is false (data_ready alone is enough)", async () => {
      setFakeTbDeploymentJson({ id: "deploy-no-auto", status: "pending" });
      server.use(
        http.get(`${BASE_URL}/v1/deployments/deploy-no-auto`, () => {
          return HttpResponse.json(
            createDeploymentStatusResponse({ deploymentId: "deploy-no-auto", status: "data_ready", live: false })
          );
        })
      );

      const result = await deployToMain(config, resources, { auto: false });

      // When !auto, we return success as soon as the deployment is
      // data_ready, even though `live` is still false (user promotes it separately).
      expect(result.success).toBe(true);
      expect(result.buildId).toBe("deploy-no-auto");
    });

    it("returns immediately when wait is false, without polling", async () => {
      setFakeTbDeploymentJson({ id: "deploy-no-wait", status: "pending" });
      // Deliberately no /v1/deployments/deploy-no-wait handler: with
      // onUnhandledRequest: "error", any poll attempt fails the test.

      const result = await deployToMain(config, resources, { wait: false });

      expect(result.success).toBe(true);
      expect(result.buildId).toBe("deploy-no-wait");
    });

    it("skips stale deployment cleanup in check mode", async () => {
      let listed = false;
      const deletedIds: string[] = [];

      server.use(
        http.get(`${BASE_URL}/v1/deployments`, () => {
          listed = true;
          return HttpResponse.json(
            createDeploymentsListResponse({ deployments: [{ id: "in-flight", status: "pending", live: false }] })
          );
        }),
        http.delete(`${BASE_URL}/v1/deployments/:id`, ({ params }) => {
          deletedIds.push(params.id as string);
          return HttpResponse.json({ result: "success" });
        })
      );
      setFakeTbDeploymentJson({ id: "deploy-check", status: "success" });

      const result = await deployToMain(config, resources, { check: true });

      expect(listed).toBe(false);
      expect(deletedIds).toEqual([]);
      expect(result).toMatchObject({ success: true, result: "success" });
    });

    it("deletes stale non-live deployments before deploy", async () => {
      const deletedIds: string[] = [];

      server.use(
        http.get(`${BASE_URL}/v1/deployments`, () => {
          return HttpResponse.json(
            createDeploymentsListResponse({
              deployments: [
                { id: "stale-1", status: "pending", live: false },
                { id: "live-1", status: "live", live: true },
                { id: "stale-2", status: "failed", live: false },
              ],
            })
          );
        }),
        http.delete(`${BASE_URL}/v1/deployments/:id`, ({ params }) => {
          deletedIds.push(params.id as string);
          return HttpResponse.json({ result: "success" });
        })
      );
      setupSuccessfulDeployFlow("deploy-cleanup");

      await deployToMain(config, resources);

      // The previous live deployment is left alone — the server removes it
      // as part of the auto-promotion once the new deployment is live.
      expect(deletedIds).toEqual(["stale-1", "stale-2"]);
    });

    it("adds actionable guidance to Forward/Classic workspace errors", async () => {
      process.env.FAKE_TB_STDOUT = JSON.stringify({
        id: "deploy-classic",
        status: "failed",
        errors: [
          {
            error:
              "This is a Tinybird Forward workspace, and this operation is only available for Tinybird Classic workspaces.",
          },
        ],
      });
      process.env.FAKE_TB_EXIT_CODE = "1";

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.error).toContain("Tinybird Forward workspace");
      expect(result.error).toContain(
        "Use the Tinybird Classic CLI (`tb`) from a Tinybird Classic workspace for this operation."
      );
    });

    it("tolerates transient failed status while server auto-deletes", async () => {
      let pollCount = 0;
      setFakeTbDeploymentJson({ id: "deploy-transient-fail", status: "pending" });

      server.use(
        http.get(`${BASE_URL}/v1/deployments/deploy-transient-fail`, () => {
          pollCount++;
          if (pollCount <= 2) {
            return HttpResponse.json(
              createDeploymentStatusResponse({ deploymentId: "deploy-transient-fail", status: "failed", live: false })
            );
          }
          return HttpResponse.json({
            result: "success",
            deployment: {
              id: "deploy-transient-fail",
              status: "deleted",
              live: false,
              feedback: [{ resource: null, level: "ERROR", message: "schema conflict" }],
            },
          });
        })
      );

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.error).toContain("deleted automatically");
      expect(result.error).toContain("schema conflict");
      expect(pollCount).toBe(3);
    });

    it("bails when the deployment is stuck in failed state", async () => {
      let pollCount = 0;
      setFakeTbDeploymentJson({ id: "deploy-stuck", status: "pending" });

      server.use(
        http.get(`${BASE_URL}/v1/deployments/deploy-stuck`, () => {
          pollCount++;
          return HttpResponse.json(
            createDeploymentStatusResponse({ deploymentId: "deploy-stuck", status: "failed", live: false })
          );
        })
      );

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.error).toContain("didn't start deleting automatically");
      expect(pollCount).toBe(MAX_CONSECUTIVE_FAILED_POLLS + 1);
    });

    it("surfaces a clear error when the binary can't be resolved", async () => {
      delete process.env.TB_CLI_BINARY_PATH; // force manifest resolution, which will fail offline
      process.env.TB_CLI_BASE_URL = "https://does-not-exist.invalid";

      const result = await deployToMain(config, resources);

      expect(result.success).toBe(false);
      expect(result.result).toBe("failed");
      expect(result.error).toBeTruthy();

      delete process.env.TB_CLI_BASE_URL;
    });
  });

  describe("materialized project contents", () => {
    it("writes datasources, pipes, and connections for tb to read, and passes the right flags", async () => {
      process.env.FAKE_TB_ECHO_ARGV = "1";

      const resourcesWithConnections: GeneratedResources = {
        datasources: [{ name: "events", content: "SCHEMA > timestamp DateTime" }],
        pipes: [{ name: "top_events", content: "NODE main\nSQL > SELECT * FROM events" }],
        connections: [{ name: "my_kafka", content: "TYPE kafka\nKAFKA_BROKERS kafka:9092\n" }],
      };

      // Capture stdout directly via runTbBinary's own plumbing by reusing
      // deployToMain, then re-deriving what was echoed from its (otherwise
      // discarded) result isn't possible — so assert via a direct call to
      // the same building blocks deployToMain uses.
      const { materializeResourcesToTempProject } = await import("../cli-binary/resources-to-project.js");
      const { runTbBinary } = await import("../cli-binary/run.js");

      const project = await materializeResourcesToTempProject(resourcesWithConnections);
      try {
        const run = await runTbBinary(
          FAKE_TB,
          ["deployment", "create", "--no-wait", "--auto", "--allow-destructive-operations"],
          { cwd: project.dir, token: config.token, host: config.baseUrl, env: { FAKE_TB_ECHO_ARGV: "1" } }
        );
        const parsed = JSON.parse(run.stdout);

        expect(parsed.argv).toEqual([
          "--output",
          "json",
          "deployment",
          "create",
          "--no-wait",
          "--auto",
          "--allow-destructive-operations",
        ]);
        expect(parsed.projectFiles).toEqual([
          "connections/my_kafka.connection",
          "datasources/events.datasource",
          "pipes/top_events.pipe",
        ]);
        expect(parsed.env.TB_TOKEN).toBe(config.token);
      } finally {
        await project.cleanup();
      }
    });
  });
});
