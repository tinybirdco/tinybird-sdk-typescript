import {
  describe,
  it,
  expect,
  beforeAll,
  beforeEach,
  afterEach,
  afterAll,
} from "vitest";
import { runInit } from "../src/cli/commands/init.js";
import { runBuild } from "../src/cli/commands/build.js";
import { listDatasources, listPipesV1 } from "../src/api/resources.js";
import { getBranch, deleteBranch, BranchApiError } from "../src/api/branches.js";
import { createClient } from "../src/client/base.js";
import {
  getLiveE2EConfigFromEnv,
  createRunScopedName,
  type LiveE2EConfig,
} from "./cloud-workspace.js";
import {
  ensureDistBuild,
  createTempProjectDir,
  cleanupTempProjectDir,
  setConfigBaseUrl,
} from "./test-project.js";

const liveConfig = getLiveE2EConfigFromEnv();
const describeLive = liveConfig ? describe : describe.skip;

function toTinybirdDateTime(value: Date): string {
  return value.toISOString().slice(0, 19).replace("T", " ");
}

async function waitForPipeData(
  branchToken: string,
  baseUrl: string
): Promise<Array<Record<string, unknown>>> {
  const client = createClient({
    baseUrl,
    token: branchToken,
  });

  for (let attempt = 0; attempt < 10; attempt++) {
    const result = await client.query<Record<string, unknown>>("top_pages", {
      start_date: toTinybirdDateTime(new Date(Date.now() - 60_000)),
      end_date: toTinybirdDateTime(new Date(Date.now() + 60_000)),
      limit: 10,
    });

    if (result.data.length > 0) {
      return result.data as Array<Record<string, unknown>>;
    }

    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }

  return [];
}

describeLive("E2E Live: build", () => {
  const config = liveConfig as LiveE2EConfig;

  let tempDir = "";
  let originalEnv: NodeJS.ProcessEnv;

  const workspaceToken = liveConfig?.workspaceToken ?? "";
  const runName = createRunScopedName("sdk_build");
  let tinybirdBranchName = "";

  beforeAll(() => {
    ensureDistBuild();
  });

  beforeEach(() => {
    tempDir = createTempProjectDir();
    originalEnv = { ...process.env };
    // The SDK prefers GITHUB_HEAD_REF (set on pull_request runs) over GITHUB_REF_NAME.
    delete process.env.GITHUB_HEAD_REF;
    process.env.GITHUB_REF_NAME = `live-build/${runName}`;
    process.env.TINYBIRD_TOKEN = workspaceToken;
  });

  afterEach(() => {
    process.env = originalEnv;
    cleanupTempProjectDir(tempDir);
  });

  afterAll(async () => {
    if (tinybirdBranchName) {
      try {
        await deleteBranch(
          { baseUrl: config.baseUrl, token: workspaceToken },
          tinybirdBranchName
        );
      } catch (error) {
        if (!(error instanceof BranchApiError && error.status === 404)) {
          throw error;
        }
      }
    }
  });

  it("builds and deploys resources, then serves queries from the deployed endpoint", async () => {
    const initResult = await runInit({
      cwd: tempDir,
      skipLogin: true,
      devMode: "branch",
      clientPath: "lib/tinybird.ts",
    });
    expect(initResult.success).toBe(true);

    setConfigBaseUrl(tempDir, config.baseUrl);

    const buildResult = await runBuild({ cwd: tempDir });
    expect(buildResult.success).toBe(true);
    expect(buildResult.deploy?.success).toBe(true);
    expect(buildResult.branchInfo?.tinybirdBranch).toBeTruthy();

    tinybirdBranchName = buildResult.branchInfo?.tinybirdBranch ?? "";

    const branch = await getBranch(
      { baseUrl: config.baseUrl, token: workspaceToken },
      tinybirdBranchName
    );
    expect(branch.token).toBeTruthy();

    const branchToken = branch.token as string;

    const datasources = await listDatasources({
      baseUrl: config.baseUrl,
      token: branchToken,
    });
    expect(datasources).toContain("page_views");

    const pipes = await listPipesV1({
      baseUrl: config.baseUrl,
      token: branchToken,
    });
    expect(pipes).toContain("top_pages");

    const client = createClient({
      baseUrl: config.baseUrl,
      token: branchToken,
    });

    const pathname = `/live-e2e/${runName}`;
    await client.ingest("page_views", {
      timestamp: toTinybirdDateTime(new Date()),
      session_id: "live_test_session",
      pathname,
      referrer: null,
    });

    const rows = await waitForPipeData(branchToken, config.baseUrl);
    expect(rows.map((row) => row.pathname)).toContain(pathname);
  });
});
