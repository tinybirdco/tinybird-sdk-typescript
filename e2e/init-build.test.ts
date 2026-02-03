/**
 * E2E tests for the init + build happy path
 *
 * Tests the full flow of:
 * 1. Creating a new blank project
 * 2. Running init to scaffold Tinybird files
 * 3. Running build to deploy to Tinybird (mocked)
 */

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
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
        clientPath: "tinybird",
      });

      // Verify init succeeded
      expect(initResult.success).toBe(true);
      expect(initResult.devMode).toBe("branch");
      expect(initResult.clientPath).toBe("tinybird");

      // Verify files created
      expect(initResult.created).toContain("tinybird.json");
      expect(initResult.created).toContain("tinybird/datasources.ts");
      expect(initResult.created).toContain("tinybird/endpoints.ts");
      expect(initResult.created).toContain("tinybird/client.ts");

      // Verify files exist on disk
      expect(fs.existsSync(path.join(tempDir, "tinybird.json"))).toBe(true);
      expect(fs.existsSync(path.join(tempDir, "tinybird/datasources.ts"))).toBe(true);
      expect(fs.existsSync(path.join(tempDir, "tinybird/endpoints.ts"))).toBe(true);
      expect(fs.existsSync(path.join(tempDir, "tinybird/client.ts"))).toBe(true);

      // Verify tinybird.json content
      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      expect(config.include).toEqual([
        "tinybird/datasources.ts",
        "tinybird/endpoints.ts",
      ]);
      expect(config.token).toBe("${TINYBIRD_TOKEN}");
      expect(config.baseUrl).toBe("https://api.tinybird.co");
      expect(config.devMode).toBe("branch");
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
        clientPath: "tinybird",
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

    it("generated TypeScript files are valid", async () => {
      // Run init
      await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "tinybird",
      });

      // Read generated files
      const datasourcesContent = fs.readFileSync(
        path.join(tempDir, "tinybird/datasources.ts"),
        "utf-8"
      );
      const endpointsContent = fs.readFileSync(
        path.join(tempDir, "tinybird/endpoints.ts"),
        "utf-8"
      );
      const clientContent = fs.readFileSync(
        path.join(tempDir, "tinybird/client.ts"),
        "utf-8"
      );

      // Verify datasources.ts has expected exports
      expect(datasourcesContent).toContain("export const pageViews");
      expect(datasourcesContent).toContain("defineDatasource");
      expect(datasourcesContent).toContain("export type PageViewsRow");

      // Verify endpoints.ts has expected exports
      expect(endpointsContent).toContain("export const topPages");
      expect(endpointsContent).toContain("defineEndpoint");
      expect(endpointsContent).toContain("export type TopPagesParams");
      expect(endpointsContent).toContain("export type TopPagesOutput");

      // Verify client.ts imports and exports correctly
      expect(clientContent).toContain("createTinybirdClient");
      expect(clientContent).toContain("export const tinybird");
      expect(clientContent).toContain('from "./datasources"');
      expect(clientContent).toContain('from "./endpoints"');
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
        clientPath: "tinybird",
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
        clientPath: "tinybird",
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
    it("init creates files in src/tinybird when src exists", async () => {
      // Create src folder
      fs.mkdirSync(path.join(tempDir, "src"));

      // Run init
      const initResult = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "src/tinybird",
      });

      expect(initResult.success).toBe(true);
      expect(initResult.created).toContain("src/tinybird/datasources.ts");
      expect(initResult.created).toContain("src/tinybird/endpoints.ts");
      expect(initResult.created).toContain("src/tinybird/client.ts");

      // Verify config has correct paths
      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      expect(config.include).toEqual([
        "src/tinybird/datasources.ts",
        "src/tinybird/endpoints.ts",
      ]);
    });

    it("init + build works with src/tinybird structure", async () => {
      // Create src folder
      fs.mkdirSync(path.join(tempDir, "src"));

      // Run init
      const initResult = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "src/tinybird",
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
        clientPath: "tinybird",
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
        clientPath: "tinybird",
      });

      // Run build
      const buildResult = await runBuild({ cwd: tempDir });

      expect(buildResult.success).toBe(true);
      expect(branchCreateCalled).toBe(false);
    });
  });
});
