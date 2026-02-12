import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { runLogin } from "./login.js";

// Mock the auth module to avoid browser login
vi.mock("../auth.js", () => ({
  browserLogin: vi.fn(),
}));

// Mock the region-selector module to avoid interactive prompts
vi.mock("../region-selector.js", () => ({
  getApiHostWithRegionSelection: vi.fn(),
}));

// Import the mocked functions
import { browserLogin } from "../auth.js";
import { getApiHostWithRegionSelection } from "../region-selector.js";

const mockedBrowserLogin = vi.mocked(browserLogin);
const mockedGetApiHost = vi.mocked(getApiHostWithRegionSelection);

describe("Login Command", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-login-test-"));
    vi.clearAllMocks();
    // Default mock for region selection - returns a default apiHost
    mockedGetApiHost.mockResolvedValue({
      apiHost: "https://api.tinybird.co",
      fromConfig: false,
    });
  });

  afterEach(() => {
    try {
      fs.rmSync(tempDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  describe("config file requirement", () => {
    it("fails when no tinybird.json exists", async () => {
      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(false);
      expect(result.error).toContain("No tinybird config found");
      expect(result.error).toContain("npx tinybird init");
    });

    it("succeeds when tinybird.json exists", async () => {
      // Create a tinybird.json config
      const config = { include: ["lib/tinybird.ts"], token: "${TINYBIRD_TOKEN}" };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "test-token",
        baseUrl: "https://api.tinybird.co",
        workspaceName: "test-workspace",
        userEmail: "user@example.com",
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(true);
      expect(result.workspaceName).toBe("test-workspace");
      expect(result.userEmail).toBe("user@example.com");
      expect(result.baseUrl).toBe("https://api.tinybird.co");
    });

    it("finds tinybird.json in parent directory (monorepo support)", async () => {
      // Create nested directory structure
      const nestedDir = path.join(tempDir, "packages", "app");
      fs.mkdirSync(nestedDir, { recursive: true });

      // Create tinybird.json in root
      const config = { include: ["lib/tinybird.ts"], token: "${TINYBIRD_TOKEN}" };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "test-token",
        baseUrl: "https://api.tinybird.co",
        workspaceName: "test-workspace",
        userEmail: "user@example.com",
      });

      const result = await runLogin({ cwd: nestedDir });

      expect(result.success).toBe(true);
    });
  });

  describe("authentication flow", () => {
    beforeEach(() => {
      // Create a tinybird.json config
      const config = { include: ["lib/tinybird.ts"], token: "${TINYBIRD_TOKEN}" };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );
    });

    it("returns error when browser login fails", async () => {
      mockedBrowserLogin.mockResolvedValue({
        success: false,
        error: "Authentication timed out",
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(false);
      expect(result.error).toBe("Authentication timed out");
    });

    it("returns error when token is missing from auth result", async () => {
      mockedBrowserLogin.mockResolvedValue({
        success: true,
        // No token provided
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(false);
      expect(result.error).toBe("Login failed");
    });

    it("passes apiHost option to browserLogin", async () => {
      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "test-token",
        baseUrl: "https://api.us-east.tinybird.co",
        workspaceName: "test-workspace",
      });

      await runLogin({ cwd: tempDir, apiHost: "https://api.us-east.tinybird.co" });

      expect(mockedBrowserLogin).toHaveBeenCalledWith({
        apiHost: "https://api.us-east.tinybird.co",
      });
    });
  });

  describe("token storage", () => {
    beforeEach(() => {
      // Create a tinybird.json config
      const config = { include: ["lib/tinybird.ts"], token: "${TINYBIRD_TOKEN}" };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );
    });

    it("creates .env.local with token", async () => {
      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "new-token-123",
        baseUrl: "https://api.tinybird.co",
        workspaceName: "test-workspace",
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(true);

      const envContent = fs.readFileSync(
        path.join(tempDir, ".env.local"),
        "utf-8"
      );
      expect(envContent).toContain("TINYBIRD_TOKEN=new-token-123");
    });

    it("updates existing token in .env.local", async () => {
      // Create existing .env.local with old token
      fs.writeFileSync(
        path.join(tempDir, ".env.local"),
        "TINYBIRD_TOKEN=old-token\nOTHER_VAR=value\n"
      );

      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "new-token-456",
        baseUrl: "https://api.tinybird.co",
        workspaceName: "test-workspace",
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(true);

      const envContent = fs.readFileSync(
        path.join(tempDir, ".env.local"),
        "utf-8"
      );
      expect(envContent).toContain("TINYBIRD_TOKEN=new-token-456");
      expect(envContent).toContain("OTHER_VAR=value");
      expect(envContent).not.toContain("old-token");
    });

    it("appends token to existing .env.local without TINYBIRD_TOKEN", async () => {
      // Create existing .env.local without token
      fs.writeFileSync(
        path.join(tempDir, ".env.local"),
        "OTHER_VAR=value\n"
      );

      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "new-token-789",
        baseUrl: "https://api.tinybird.co",
        workspaceName: "test-workspace",
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(true);

      const envContent = fs.readFileSync(
        path.join(tempDir, ".env.local"),
        "utf-8"
      );
      expect(envContent).toContain("OTHER_VAR=value");
      expect(envContent).toContain("TINYBIRD_TOKEN=new-token-789");
    });

    it("saves .env.local in same directory as tinybird.json (monorepo)", async () => {
      // Create nested directory structure
      const nestedDir = path.join(tempDir, "packages", "app");
      fs.mkdirSync(nestedDir, { recursive: true });

      // Move tinybird.json to root
      fs.unlinkSync(path.join(tempDir, "tinybird.json"));
      const config = { include: ["lib/tinybird.ts"], token: "${TINYBIRD_TOKEN}" };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "test-token",
        baseUrl: "https://api.tinybird.co",
        workspaceName: "test-workspace",
      });

      const result = await runLogin({ cwd: nestedDir });

      expect(result.success).toBe(true);

      // .env.local should be in the root (same as tinybird.json), not in nested dir
      expect(fs.existsSync(path.join(tempDir, ".env.local"))).toBe(true);
      expect(fs.existsSync(path.join(nestedDir, ".env.local"))).toBe(false);
    });
  });

  describe("baseUrl update", () => {
    beforeEach(() => {
      // Create a tinybird.json config
      const config = {
        include: ["lib/tinybird.ts"],
        token: "${TINYBIRD_TOKEN}",
        baseUrl: "https://api.tinybird.co",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config, null, 2)
      );
    });

    it("updates baseUrl in tinybird.json when region changes", async () => {
      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "test-token",
        baseUrl: "https://api.us-east.tinybird.co",
        workspaceName: "test-workspace",
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(true);

      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      expect(config.baseUrl).toBe("https://api.us-east.tinybird.co");
    });

    it("does not modify tinybird.json when baseUrl is not provided", async () => {
      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "test-token",
        // No baseUrl
        workspaceName: "test-workspace",
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(true);

      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      // Original baseUrl preserved
      expect(config.baseUrl).toBe("https://api.tinybird.co");
    });
  });

  describe("return values", () => {
    beforeEach(() => {
      const config = { include: ["lib/tinybird.ts"], token: "${TINYBIRD_TOKEN}" };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );
    });

    it("returns workspace name from auth result", async () => {
      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "test-token",
        workspaceName: "my-workspace",
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(true);
      expect(result.workspaceName).toBe("my-workspace");
    });

    it("returns user email from auth result", async () => {
      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "test-token",
        userEmail: "developer@example.com",
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(true);
      expect(result.userEmail).toBe("developer@example.com");
    });

    it("returns baseUrl from auth result", async () => {
      mockedBrowserLogin.mockResolvedValue({
        success: true,
        token: "test-token",
        baseUrl: "https://api.eu-central.tinybird.co",
      });

      const result = await runLogin({ cwd: tempDir });

      expect(result.success).toBe(true);
      expect(result.baseUrl).toBe("https://api.eu-central.tinybird.co");
    });
  });
});
