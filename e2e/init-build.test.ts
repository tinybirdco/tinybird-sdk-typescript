/**
 * E2E tests for the init + build happy path
 *
 * Tests the full flow of:
 * 1. Creating a new blank project
 * 2. Running init to scaffold Tinybird files
 * 3. Running build to deploy to Tinybird (mocked)
 */

import { describe, it, expect, beforeEach, afterEach, vi, type MockInstance } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { fileURLToPath } from "url";
import { runInit } from "../src/cli/commands/init.js";
import { runBuild } from "../src/cli/commands/build.js";
import { server } from "./setup.js";
import { http, HttpResponse } from "msw";
import { BASE_URL, createBuildSuccessResponse } from "./handlers.js";

// Mock the auth module to avoid browser login
vi.mock("../src/cli/auth.js", () => ({
  browserLogin: vi.fn().mockResolvedValue({ success: false }),
}));

// Get the project root directory
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, "..");

describe("E2E: Init + Build Happy Path", () => {
  let tempDir: string;
  let originalEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    // Create temp directory
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-e2e-test-"));

    // Set up node_modules so @tinybirdco/sdk can be resolved
    // This is necessary because esbuild marks it as external
    const nodeModulesDir = path.join(tempDir, "node_modules");
    const tinybirdcoDir = path.join(nodeModulesDir, "@tinybirdco");
    fs.mkdirSync(tinybirdcoDir, { recursive: true });

    // Symlink the SDK package itself (the project root)
    fs.symlinkSync(PROJECT_ROOT, path.join(tinybirdcoDir, "sdk"), "dir");

    // Save original env
    originalEnv = { ...process.env };

    // Set up environment for tests
    // Use CI env var to simulate a feature branch (avoids needing real git repo)
    process.env.GITHUB_REF_NAME = "feature/test-branch";
    process.env.TINYBIRD_TOKEN = "p.test-workspace-token";
  });

  afterEach(() => {
    // Restore original env
    process.env = originalEnv;

    // Cleanup temp directory
    try {
      fs.rmSync(tempDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  describe("new blank project", () => {
    it("init creates all required files", async () => {
      // Run init with skipLogin and predefined options
      const initResult = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      // Verify init succeeded
      expect(initResult.success).toBe(true);
      expect(initResult.devMode).toBe("branch");
      expect(initResult.clientPath).toBe("lib/tinybird.ts");

      // Verify files created
      expect(initResult.created).toContain("tinybird.config.json");
      expect(initResult.created).toContain("lib/tinybird.ts");

      // Verify files exist on disk
      expect(fs.existsSync(path.join(tempDir, "tinybird.config.json"))).toBe(true);
      expect(fs.existsSync(path.join(tempDir, "lib/tinybird.ts"))).toBe(true);

      // Verify tinybird.config.json content
      const content = fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8");
      expect(content).toContain('"lib/tinybird.ts"');
      expect(content).toContain("${TINYBIRD_TOKEN}");
      expect(content).toContain("https://api.tinybird.co");
      expect(content).toContain('"devMode": "branch"');
    });

    it("init then build succeeds with mocked API", async () => {
      // Track API calls
      let buildCalled = false;
      let branchGetCalled = false;
      let capturedFormData: FormData | null = null;

      // Set up handlers to track calls
      server.use(
        http.get(`${BASE_URL}/v0/environments/:name`, () => {
          branchGetCalled = true;
          return HttpResponse.json({
            id: "branch-feature_test_branch",
            name: "feature_test_branch",
            token: "p.branch-token-feature_test_branch",
            created_at: new Date().toISOString(),
          });
        }),
        http.post(`${BASE_URL}/v1/build`, async ({ request }) => {
          buildCalled = true;
          capturedFormData = await request.formData();
          return HttpResponse.json(
            createBuildSuccessResponse({
              newDatasources: ["page_views"],
              newPipes: ["top_pages"],
            })
          );
        })
      );

      // Step 1: Run init
      const initResult = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });
      expect(initResult.success).toBe(true);

      // Step 2: Run build
      const buildResult = await runBuild({ cwd: tempDir });

      // Verify build succeeded
      expect(buildResult.success).toBe(true);
      expect(buildResult.error).toBeUndefined();

      // Verify API was called
      expect(branchGetCalled).toBe(true);
      expect(buildCalled).toBe(true);

      // Verify build stats
      expect(buildResult.build?.stats.datasourceCount).toBe(1);
      expect(buildResult.build?.stats.pipeCount).toBe(1);

      // Verify deploy result
      expect(buildResult.deploy?.success).toBe(true);
      expect(buildResult.deploy?.datasources?.created).toEqual(["page_views"]);
      expect(buildResult.deploy?.pipes?.created).toEqual(["top_pages"]);

      // Verify form data was sent (resources)
      expect(capturedFormData).not.toBeNull();
      const formDataEntries = capturedFormData!.getAll("data_project://");
      expect(formDataEntries.length).toBe(2); // 1 datasource + 1 pipe
    });

    it("generated TypeScript file is valid", async () => {
      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      // Read generated file
      const content = fs.readFileSync(
        path.join(tempDir, "lib/tinybird.ts"),
        "utf-8"
      );

      // Verify file has expected exports
      expect(content).toContain("export const pageViews");
      expect(content).toContain("defineDatasource");
      expect(content).toContain("export const topPages");
      expect(content).toContain("defineEndpoint");
      expect(content).toContain("createTinybirdClient");
      expect(content).toContain("export const tinybird");
    });

    it("build with dry run does not call API", async () => {
      let apiCalled = false;

      server.use(
        http.post(`${BASE_URL}/v1/build`, () => {
          apiCalled = true;
          return HttpResponse.json(createBuildSuccessResponse());
        })
      );

      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      // Run build with dry run
      const buildResult = await runBuild({ cwd: tempDir, dryRun: true });

      // Verify build succeeded but no API call
      expect(buildResult.success).toBe(true);
      expect(buildResult.build).toBeDefined();
      expect(buildResult.deploy).toBeUndefined();
      expect(apiCalled).toBe(false);
    });
  });

  describe("project with package.json", () => {
    it("init adds tinybird scripts to package.json", async () => {
      // Create package.json
      const packageJson = {
        name: "test-project",
        version: "1.0.0",
        scripts: {
          dev: "next dev",
        },
      };
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify(packageJson, null, 2)
      );

      // Run init
      const initResult = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      expect(initResult.success).toBe(true);
      expect(initResult.created).toContain("package.json (added tinybird scripts)");

      // Verify scripts were added
      const updatedPackageJson = JSON.parse(
        fs.readFileSync(path.join(tempDir, "package.json"), "utf-8")
      );
      expect(updatedPackageJson.scripts["tinybird:dev"]).toBe("tinybird dev");
      expect(updatedPackageJson.scripts["tinybird:build"]).toBe("tinybird build");
      expect(updatedPackageJson.scripts["tinybird:deploy"]).toBe("tinybird deploy");
      expect(updatedPackageJson.scripts.dev).toBe("next dev"); // Original preserved
    });
  });

  describe("project with src folder", () => {
    it("init creates file in src/lib when src exists", async () => {
      // Create src folder
      fs.mkdirSync(path.join(tempDir, "src"));

      // Run init
      const initResult = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "src/lib/tinybird.ts",
      });

      expect(initResult.success).toBe(true);
      expect(initResult.created).toContain("src/lib/tinybird.ts");

      // Verify config has correct paths
      const content = fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8");
      expect(content).toContain('"src/lib/tinybird.ts"');
    });

    it("init + build works with src/lib/tinybird.ts structure", async () => {
      // Create src folder
      fs.mkdirSync(path.join(tempDir, "src"));

      // Run init
      const initResult = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "src/lib/tinybird.ts",
      });
      expect(initResult.success).toBe(true);

      // Run build
      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(true);
      expect(buildResult.build?.stats.datasourceCount).toBe(1);
      expect(buildResult.build?.stats.pipeCount).toBe(1);
    });
  });

  describe("branch creation flow", () => {
    it("creates branch when it does not exist", async () => {
      let branchCreateCalled = false;
      let jobPollCalled = false;

      server.use(
        // Branch doesn't exist (404)
        http.get(`${BASE_URL}/v0/environments/:name`, () => {
          return HttpResponse.json(
            { error: "Not found" },
            { status: 404 }
          );
        }),
        // Create branch
        http.post(`${BASE_URL}/v1/environments`, () => {
          branchCreateCalled = true;
          return HttpResponse.json({
            job: { id: "job-create-branch", status: "working" },
          });
        }),
        // Poll job
        http.get(`${BASE_URL}/v0/jobs/:jobId`, () => {
          jobPollCalled = true;
          return HttpResponse.json({
            id: "job-create-branch",
            status: "done",
          });
        })
      );

      // After job done, the code calls getBranch again
      // We need to return success on second call
      let branchGetCallCount = 0;
      server.use(
        http.get(`${BASE_URL}/v0/environments/:name`, () => {
          branchGetCallCount++;
          if (branchGetCallCount === 1) {
            return HttpResponse.json({ error: "Not found" }, { status: 404 });
          }
          return HttpResponse.json({
            id: "branch-feature_test_branch",
            name: "feature_test_branch",
            token: "p.new-branch-token",
            created_at: new Date().toISOString(),
          });
        })
      );

      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      // Run build
      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(true);
      expect(branchCreateCalled).toBe(true);
      expect(jobPollCalled).toBe(true);
    });

    it("does not create branch if it already exists", async () => {
      let branchCreateCalled = false;

      server.use(
        // Branch already exists
        http.get(`${BASE_URL}/v0/environments/:name`, () => {
          return HttpResponse.json({
            id: "branch-feature_test_branch",
            name: "feature_test_branch",
            token: "p.existing-branch-token",
            created_at: new Date().toISOString(),
          });
        }),
        // Create branch - should NOT be called
        http.post(`${BASE_URL}/v1/environments`, () => {
          branchCreateCalled = true;
          return HttpResponse.json({
            job: { id: "job-create-branch", status: "working" },
          });
        }),
        // Build endpoint
        http.post(`${BASE_URL}/v1/build`, () => {
          return HttpResponse.json(
            createBuildSuccessResponse({
              newDatasources: ["page_views"],
              newPipes: ["top_pages"],
            })
          );
        })
      );

      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      // Run build
      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(true);
      expect(branchCreateCalled).toBe(false);
    });
  });

  describe("build error handling", () => {
    it("fails when config file is missing", async () => {
      // Don't run init - no config file
      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(false);
      expect(buildResult.error).toContain("Could not find config file");
    });

    it("fails when include file does not exist", async () => {
      // Create config pointing to non-existent file
      const config = {
        include: ["lib/tinybird.ts"],
        token: "${TINYBIRD_TOKEN}",
        baseUrl: "https://api.tinybird.co",
        devMode: "branch",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.config.json"),
        JSON.stringify(config, null, 2)
      );

      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(false);
      expect(buildResult.error).toContain("not found");
    });


    it("fails when branch is created but no token returned", async () => {
      server.use(
        // Branch exists but no token
        http.get(`${BASE_URL}/v0/environments/:name`, () => {
          return HttpResponse.json({
            id: "branch-feature_test_branch",
            name: "feature_test_branch",
            // No token field
            created_at: new Date().toISOString(),
          });
        })
      );

      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(false);
      expect(buildResult.error).toContain("no token was returned");
    });

    it("fails when branch creation API fails", async () => {
      server.use(
        // Branch doesn't exist
        http.get(`${BASE_URL}/v0/environments/:name`, () => {
          return HttpResponse.json({ error: "Not found" }, { status: 404 });
        }),
        // Create branch fails
        http.post(`${BASE_URL}/v1/environments`, () => {
          return HttpResponse.json(
            { error: "Quota exceeded" },
            { status: 429 }
          );
        })
      );

      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(false);
      expect(buildResult.error).toContain("Failed to get/create branch");
    });

    it("fails when build API returns error", async () => {
      server.use(
        // Branch exists
        http.get(`${BASE_URL}/v0/environments/:name`, () => {
          return HttpResponse.json({
            id: "branch-feature_test_branch",
            name: "feature_test_branch",
            token: "p.branch-token",
            created_at: new Date().toISOString(),
          });
        }),
        // Build API fails
        http.post(`${BASE_URL}/v1/build`, () => {
          return HttpResponse.json(
            {
              result: "error",
              error: "Schema validation failed: invalid column type",
            },
            { status: 400 }
          );
        })
      );

      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(false);
      expect(buildResult.error).toBeDefined();
    });

    it("returns error details from failed build API response", async () => {
      server.use(
        // Branch exists
        http.get(`${BASE_URL}/v0/environments/:name`, () => {
          return HttpResponse.json({
            id: "branch-feature_test_branch",
            name: "feature_test_branch",
            token: "p.branch-token",
            created_at: new Date().toISOString(),
          });
        }),
        // Build API returns result: "failed" with error
        http.post(`${BASE_URL}/v1/build`, () => {
          return HttpResponse.json({
            result: "failed",
            error: "Datasource 'page_views' already exists with different schema",
          });
        })
      );

      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(false);
      expect(buildResult.deploy?.success).toBe(false);
      expect(buildResult.deploy?.error).toContain("already exists");
    });
  });

  describe("build command output format", () => {
    let consoleLogSpy: MockInstance<Console["log"]>;

    beforeEach(() => {
      consoleLogSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    });

    afterEach(() => {
      consoleLogSpy.mockRestore();
    });

    it("outputs resource changes in simple format (matching dev command)", async () => {
      server.use(
        http.get(`${BASE_URL}/v0/environments/:name`, () => {
          return HttpResponse.json({
            id: "branch-feature_test_branch",
            name: "feature_test_branch",
            token: "p.branch-token-feature_test_branch",
            created_at: new Date().toISOString(),
          });
        }),
        http.post(`${BASE_URL}/v1/build`, () => {
          return HttpResponse.json(
            createBuildSuccessResponse({
              newDatasources: ["page_views"],
              newPipes: ["top_pages"],
            })
          );
        })
      );

      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      // Import and call the CLI action directly
      const { output } = await import("../src/cli/output.js");
      const showResourceChangeSpy = vi.spyOn(output, "showResourceChange");
      const showChangesTableSpy = vi.spyOn(output, "showChangesTable");

      // Run build and simulate CLI output
      const buildResult = await runBuild({ cwd: tempDir });

      // Verify build succeeded
      expect(buildResult.success).toBe(true);

      // Simulate the CLI output logic (same as in index.ts build command)
      const { deploy } = buildResult;
      if (deploy && deploy.result !== "no_changes") {
        if (deploy.datasources) {
          for (const name of deploy.datasources.created) {
            output.showResourceChange(`${name}.datasource`, "created");
          }
          for (const name of deploy.datasources.changed) {
            output.showResourceChange(`${name}.datasource`, "changed");
          }
          for (const name of deploy.datasources.deleted) {
            output.showResourceChange(`${name}.datasource`, "deleted");
          }
        }
        if (deploy.pipes) {
          for (const name of deploy.pipes.created) {
            output.showResourceChange(`${name}.pipe`, "created");
          }
          for (const name of deploy.pipes.changed) {
            output.showResourceChange(`${name}.pipe`, "changed");
          }
          for (const name of deploy.pipes.deleted) {
            output.showResourceChange(`${name}.pipe`, "deleted");
          }
        }
      }

      // Verify showResourceChange was called (simple format like dev)
      expect(showResourceChangeSpy).toHaveBeenCalledWith("page_views.datasource", "created");
      expect(showResourceChangeSpy).toHaveBeenCalledWith("top_pages.pipe", "created");

      // Verify showChangesTable was NOT called (table format)
      expect(showChangesTableSpy).not.toHaveBeenCalled();

      // Verify the actual console output format
      const allCalls = consoleLogSpy.mock.calls.map((c: unknown[]) => c[0]).join("\n");
      expect(allCalls).toContain("✓ page_views.datasource created");
      expect(allCalls).toContain("✓ top_pages.pipe created");

      // Verify table format is NOT used
      expect(allCalls).not.toContain("┌");
      expect(allCalls).not.toContain("┐");
      expect(allCalls).not.toContain("Changes to be deployed");

      showResourceChangeSpy.mockRestore();
      showChangesTableSpy.mockRestore();
    });

    it("outputs changed and deleted resources in simple format", async () => {
      server.use(
        http.get(`${BASE_URL}/v0/environments/:name`, () => {
          return HttpResponse.json({
            id: "branch-feature_test_branch",
            name: "feature_test_branch",
            token: "p.branch-token-feature_test_branch",
            created_at: new Date().toISOString(),
          });
        }),
        http.post(`${BASE_URL}/v1/build`, () => {
          return HttpResponse.json({
            result: "success",
            build: {
              id: "build-e2e-456",
              new_pipe_names: [],
              new_datasource_names: [],
              changed_pipe_names: ["top_pages"],
              changed_datasource_names: ["page_views"],
              deleted_pipe_names: ["old_pipe"],
              deleted_datasource_names: ["old_datasource"],
            },
          });
        })
      );

      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
      });

      const { output } = await import("../src/cli/output.js");
      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(true);

      // Simulate CLI output
      const { deploy } = buildResult;
      if (deploy && deploy.result !== "no_changes") {
        if (deploy.datasources) {
          for (const name of deploy.datasources.created) {
            output.showResourceChange(`${name}.datasource`, "created");
          }
          for (const name of deploy.datasources.changed) {
            output.showResourceChange(`${name}.datasource`, "changed");
          }
          for (const name of deploy.datasources.deleted) {
            output.showResourceChange(`${name}.datasource`, "deleted");
          }
        }
        if (deploy.pipes) {
          for (const name of deploy.pipes.created) {
            output.showResourceChange(`${name}.pipe`, "created");
          }
          for (const name of deploy.pipes.changed) {
            output.showResourceChange(`${name}.pipe`, "changed");
          }
          for (const name of deploy.pipes.deleted) {
            output.showResourceChange(`${name}.pipe`, "deleted");
          }
        }
      }

      const allCalls = consoleLogSpy.mock.calls.map((c: unknown[]) => c[0]).join("\n");

      // Verify changed resources
      expect(allCalls).toContain("✓ page_views.datasource changed");
      expect(allCalls).toContain("✓ top_pages.pipe changed");

      // Verify deleted resources
      expect(allCalls).toContain("✓ old_datasource.datasource deleted");
      expect(allCalls).toContain("✓ old_pipe.pipe deleted");

      // Verify no table format
      expect(allCalls).not.toContain("┌");
    });
  });
});
