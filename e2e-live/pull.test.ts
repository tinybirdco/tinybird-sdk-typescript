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
import { runDeploy } from "../src/cli/commands/deploy.js";
import { runPull } from "../src/cli/commands/pull.js";
import {
  listDatasources,
  getDatasourceFile,
  listPipesV1,
  getPipeFile,
  listConnectors,
  getConnectorFile,
} from "../src/api/resources.js";
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

function normalizeDatafileContent(content: string): string {
  return content.replace(/\r\n/g, "\n").trimEnd();
}

async function fetchProductionDatafiles(
  baseUrl: string,
  token: string
): Promise<Map<string, string>> {
  const apiConfig = { baseUrl, token };

  const [datasourceNames, pipeNames, connectorNames] = await Promise.all([
    listDatasources(apiConfig),
    listPipesV1(apiConfig),
    listConnectors(apiConfig),
  ]);

  const [datasourceFiles, pipeFiles, connectorFiles] = await Promise.all([
    Promise.all(
      datasourceNames.map(async (name) => [
        `${name}.datasource`,
        await getDatasourceFile(apiConfig, name),
      ] as const)
    ),
    Promise.all(
      pipeNames.map(async (name) => [
        `${name}.pipe`,
        await getPipeFile(apiConfig, name),
      ] as const)
    ),
    Promise.all(
      connectorNames.map(async (name) => [
        `${name}.connection`,
        await getConnectorFile(apiConfig, name),
      ] as const)
    ),
  ]);

  return new Map([...datasourceFiles, ...pipeFiles, ...connectorFiles]);
}

describeLive("E2E Live: pull", () => {
  const config = liveConfig as LiveE2EConfig;

  let tempDir = "";
  let originalEnv: NodeJS.ProcessEnv;

  let workspaceId = "";
  let workspaceName = "";
  let workspaceToken = "";

  beforeAll(async () => {
    ensureDistBuild();
    await assertWorkspaceAdminToken(config);

    const workspace = await createWorkspaceWithToken(config, "sdk_pull");
    workspaceId = workspace.id;
    workspaceName = workspace.name;
    workspaceToken = workspace.token;
  });

  beforeEach(() => {
    tempDir = createTempProjectDir();
    originalEnv = { ...process.env };

    // Force main branch detection so deploy runs against production workspace.
    process.env.GITHUB_REF_NAME = "main";
    process.env.TINYBIRD_TOKEN = workspaceToken;
  });

  afterEach(() => {
    process.env = originalEnv;
    cleanupTempProjectDir(tempDir);
  });

  afterAll(async () => {
    if (workspaceId && workspaceName) {
      await deleteWorkspace(config, workspaceId, workspaceName);
    }
  });

  it("deploys to production and pulls matching datasource/pipe files", async () => {
    const initResult = await runInit({
      cwd: tempDir,
      skipLogin: true,
      devMode: "branch",
      clientPath: "lib/tinybird.ts",
    });
    expect(initResult.success).toBe(true);

    setConfigBaseUrl(tempDir, config.baseUrl);

    const deployResult = await runDeploy({ cwd: tempDir });
    expect(deployResult.success).toBe(true);
    expect(deployResult.deploy?.success).toBe(true);

    const pullResult = await runPull({
      cwd: tempDir,
      outputDir: "pulled",
    });
    expect(pullResult.success).toBe(true);
    expect(pullResult.files?.length).toBeGreaterThan(0);

    const pulledDir = path.join(tempDir, "pulled");
    const productionDatafiles = await fetchProductionDatafiles(
      config.baseUrl,
      workspaceToken
    );

    const expectedFilenames = Array.from(productionDatafiles.keys()).sort();
    const pulledFilenames = (pullResult.files ?? [])
      .map((file) => file.filename)
      .sort();

    expect(pulledFilenames).toEqual(expectedFilenames);

    for (const [filename, expectedContent] of productionDatafiles.entries()) {
      const pulledPath = path.join(pulledDir, filename);
      expect(fs.existsSync(pulledPath)).toBe(true);

      const pulledContent = fs.readFileSync(pulledPath, "utf-8");
      expect(normalizeDatafileContent(pulledContent)).toBe(
        normalizeDatafileContent(expectedContent)
      );
    }

    expect(pullResult.stats?.total).toBe(expectedFilenames.length);
    expect(pulledFilenames).toContain("page_views.datasource");
    expect(pulledFilenames).toContain("top_pages.pipe");
  });
});
