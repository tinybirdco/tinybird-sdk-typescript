import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { detectPackageManagerRunCmd } from "./package-manager.js";

describe("detectPackageManagerRunCmd", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-manager-test-"));
  });

  afterEach(() => {
    try {
      fs.rmSync(tempDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  describe("lockfile detection", () => {
    it("detects pnpm from pnpm-lock.yaml", () => {
      fs.writeFileSync(path.join(tempDir, "pnpm-lock.yaml"), "");
      expect(detectPackageManagerRunCmd(tempDir)).toBe("pnpm");
    });

    it("detects yarn from yarn.lock", () => {
      fs.writeFileSync(path.join(tempDir, "yarn.lock"), "");
      expect(detectPackageManagerRunCmd(tempDir)).toBe("yarn");
    });

    it("detects bun from bun.lockb", () => {
      fs.writeFileSync(path.join(tempDir, "bun.lockb"), "");
      expect(detectPackageManagerRunCmd(tempDir)).toBe("bun run");
    });

    it("detects npm from package-lock.json", () => {
      fs.writeFileSync(path.join(tempDir, "package-lock.json"), "{}");
      expect(detectPackageManagerRunCmd(tempDir)).toBe("npm run");
    });

    it("prioritizes pnpm lockfile over others", () => {
      fs.writeFileSync(path.join(tempDir, "pnpm-lock.yaml"), "");
      fs.writeFileSync(path.join(tempDir, "yarn.lock"), "");
      fs.writeFileSync(path.join(tempDir, "package-lock.json"), "{}");
      expect(detectPackageManagerRunCmd(tempDir)).toBe("pnpm");
    });

    it("prioritizes yarn lockfile over npm", () => {
      fs.writeFileSync(path.join(tempDir, "yarn.lock"), "");
      fs.writeFileSync(path.join(tempDir, "package-lock.json"), "{}");
      expect(detectPackageManagerRunCmd(tempDir)).toBe("yarn");
    });
  });

  describe("packageManager field detection", () => {
    it("detects pnpm from packageManager field", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: "pnpm@9.0.0" })
      );
      expect(detectPackageManagerRunCmd(tempDir)).toBe("pnpm");
    });

    it("detects yarn from packageManager field", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: "yarn@4.0.0" })
      );
      expect(detectPackageManagerRunCmd(tempDir)).toBe("yarn");
    });

    it("detects bun from packageManager field", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: "bun@1.0.0" })
      );
      expect(detectPackageManagerRunCmd(tempDir)).toBe("bun run");
    });

    it("prioritizes lockfile over packageManager field", () => {
      fs.writeFileSync(path.join(tempDir, "yarn.lock"), "");
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: "pnpm@9.0.0" })
      );
      expect(detectPackageManagerRunCmd(tempDir)).toBe("yarn");
    });
  });

  describe("default behavior", () => {
    it("defaults to npm run when no indicators found", () => {
      expect(detectPackageManagerRunCmd(tempDir)).toBe("npm run");
    });

    it("defaults to npm run when package.json has no packageManager field", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ name: "test-project" })
      );
      expect(detectPackageManagerRunCmd(tempDir)).toBe("npm run");
    });

    it("defaults to npm run when package.json is invalid JSON", () => {
      fs.writeFileSync(path.join(tempDir, "package.json"), "not json");
      expect(detectPackageManagerRunCmd(tempDir)).toBe("npm run");
    });

    it("defaults to npm run when packageManager is not a string", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: 123 })
      );
      expect(detectPackageManagerRunCmd(tempDir)).toBe("npm run");
    });
  });
});
