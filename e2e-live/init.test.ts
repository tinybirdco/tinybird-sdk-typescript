import {
  describe,
  it,
  expect,
  beforeAll,
  beforeEach,
  afterEach,
} from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import { runInit } from "../src/cli/commands/init.js";
import { getWorkspace } from "../src/api/workspaces.js";
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

describeLive("E2E Live: init", () => {
  const config = liveConfig as LiveE2EConfig;

  let tempDir = "";
  let originalEnv: NodeJS.ProcessEnv;

  const workspaceToken = liveConfig?.workspaceToken ?? "";
  const runName = createRunScopedName("sdk_init");

  beforeAll(() => {
    ensureDistBuild();
  });

  beforeEach(() => {
    tempDir = createTempProjectDir();
    originalEnv = { ...process.env };
    // The SDK prefers GITHUB_HEAD_REF (set on pull_request runs) over GITHUB_REF_NAME.
    delete process.env.GITHUB_HEAD_REF;
    process.env.GITHUB_REF_NAME = `live-init/${runName}`;
    process.env.TINYBIRD_TOKEN = workspaceToken;
  });

  afterEach(() => {
    process.env = originalEnv;
    cleanupTempProjectDir(tempDir);
  });

  it("initializes a project against a real workspace token", async () => {
    const workspaceInfo = await getWorkspace({
      baseUrl: config.baseUrl,
      token: workspaceToken,
    });
    expect(workspaceInfo.id).toBeTruthy();

    const initResult = await runInit({
      cwd: tempDir,
      skipLogin: true,
      devMode: "branch",
      clientPath: "lib/tinybird.ts",
    });

    expect(initResult.success).toBe(true);
    expect(initResult.created).toContain("tinybird.config.json");
    expect(initResult.created).toContain("lib/tinybird.ts");

    setConfigBaseUrl(tempDir, config.baseUrl);

    const configContents = fs.readFileSync(
      path.join(tempDir, "tinybird.config.json"),
      "utf-8"
    );
    expect(configContents).toContain("${TINYBIRD_TOKEN}");
    expect(configContents).toContain(`"${config.baseUrl}"`);

    const generatedClient = fs.readFileSync(
      path.join(tempDir, "lib/tinybird.ts"),
      "utf-8"
    );
    expect(generatedClient).toContain("defineDatasource");
    expect(generatedClient).toContain("defineEndpoint");
    expect(generatedClient).toContain("new Tinybird");
  });
});
