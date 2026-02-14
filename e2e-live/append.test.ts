import {
  describe,
  it,
  expect,
  beforeAll,
  beforeEach,
  afterEach,
  afterAll,
} from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import { runInit } from "../src/cli/commands/init.js";
import { runBuild } from "../src/cli/commands/build.js";
import { getBranch, deleteBranch, BranchApiError } from "../src/api/branches.js";
import { createClient } from "../src/client/base.js";
import {
  getLiveE2EConfigFromEnv,
  assertWorkspaceAdminToken,
  createWorkspaceWithToken,
  deleteWorkspace,
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

async function waitForTopPagesRows(
  branchToken: string,
  baseUrl: string
): Promise<Array<Record<string, unknown>>> {
  const client = createClient({ baseUrl, token: branchToken });

  for (let attempt = 0; attempt < 10; attempt++) {
    const result = await client.query<Record<string, unknown>>("top_pages", {
      start_date: toTinybirdDateTime(new Date(Date.now() - 300_000)),
      end_date: toTinybirdDateTime(new Date(Date.now() + 300_000)),
      limit: 20,
    });

    if (result.data.length > 0) {
      return result.data;
    }

    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }

  return [];
}

describeLive("E2E Live: append", () => {
  const config = liveConfig as LiveE2EConfig;

  let tempDir = "";
  let originalEnv: NodeJS.ProcessEnv;

  let workspaceId = "";
  let workspaceName = "";
  let workspaceToken = "";
  let branchName = "";
  let branchToken = "";

  beforeAll(async () => {
    ensureDistBuild();
    await assertWorkspaceAdminToken(config);

    const workspace = await createWorkspaceWithToken(config, "sdk_append");
    workspaceId = workspace.id;
    workspaceName = workspace.name;
    workspaceToken = workspace.token;
  });

  beforeEach(async () => {
    tempDir = createTempProjectDir();
    originalEnv = { ...process.env };
    process.env.GITHUB_REF_NAME = `live-append/${workspaceName}`;
    process.env.TINYBIRD_TOKEN = workspaceToken;

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

    branchName = buildResult.branchInfo?.tinybirdBranch ?? "";
    expect(branchName).toBeTruthy();

    const branch = await getBranch(
      { baseUrl: config.baseUrl, token: workspaceToken },
      branchName
    );
    branchToken = branch.token ?? "";
    expect(branchToken).toBeTruthy();
  });

  afterEach(async () => {
    if (branchName) {
      try {
        await deleteBranch(
          { baseUrl: config.baseUrl, token: workspaceToken },
          branchName
        );
      } catch (error) {
        if (!(error instanceof BranchApiError && error.status === 404)) {
          throw error;
        }
      }
    }

    branchName = "";
    branchToken = "";
    process.env = originalEnv;
    cleanupTempProjectDir(tempDir);
  });

  afterAll(async () => {
    if (workspaceId && workspaceName) {
      await deleteWorkspace(config, workspaceId, workspaceName);
    }
  });

  it("appends rows from a local CSV file and serves them via endpoint query", async () => {
    const csvPath = path.join(tempDir, "page_views.csv");
    const timestamp = toTinybirdDateTime(new Date());
    const pathname = "/append-live-e2e";

    fs.writeFileSync(
      csvPath,
      `timestamp,session_id,pathname,referrer\n${timestamp},session_append,${pathname},\n`
    );

    const client = createClient({
      baseUrl: config.baseUrl,
      token: branchToken,
    });

    const appendResult = await client.datasources.append("page_views", {
      file: csvPath,
    });

    expect(appendResult).toBeDefined();
    expect(
      typeof appendResult.successful_rows === "number" ||
        typeof appendResult.import_id === "string"
    ).toBe(true);

    const rows = await waitForTopPagesRows(branchToken, config.baseUrl);
    const pathnames = rows.map((row) => row.pathname);
    expect(pathnames).toContain(pathname);
  });

  it("returns an API error when appending to a non-existent datasource", async () => {
    const csvPath = path.join(tempDir, "missing.csv");
    fs.writeFileSync(
      csvPath,
      `timestamp,session_id,pathname,referrer\n${toTinybirdDateTime(new Date())},session_missing,/x,\n`
    );

    const client = createClient({
      baseUrl: config.baseUrl,
      token: branchToken,
    });

    await expect(
      client.datasources.append("does_not_exist_live", { file: csvPath })
    ).rejects.toThrow();
  });
});
