import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import {
  detectPackageManager,
  detectPackageManagerInstallCmd,
  detectPackageManagerRunCmd,
  getPackageManagerAddCmd,
  getPackageManagerInstallCmd,
  getPackageManagerRunCmd,
  hasTinybirdSdkDependency,
} from "./package-manager.js";

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

describe("detectPackageManager", () => {
  describe("lockfile detection", () => {
    it("detects pnpm from pnpm-lock.yaml", () => {
      fs.writeFileSync(path.join(tempDir, "pnpm-lock.yaml"), "");
      expect(detectPackageManager(tempDir)).toBe("pnpm");
    });

    it("detects yarn from yarn.lock", () => {
      fs.writeFileSync(path.join(tempDir, "yarn.lock"), "");
      expect(detectPackageManager(tempDir)).toBe("yarn");
    });

    it("detects bun from bun.lockb", () => {
      fs.writeFileSync(path.join(tempDir, "bun.lockb"), "");
      expect(detectPackageManager(tempDir)).toBe("bun");
    });

    it("detects npm from package-lock.json", () => {
      fs.writeFileSync(path.join(tempDir, "package-lock.json"), "{}");
      expect(detectPackageManager(tempDir)).toBe("npm");
    });

    it("prioritizes pnpm lockfile over others", () => {
      fs.writeFileSync(path.join(tempDir, "pnpm-lock.yaml"), "");
      fs.writeFileSync(path.join(tempDir, "yarn.lock"), "");
      fs.writeFileSync(path.join(tempDir, "package-lock.json"), "{}");
      expect(detectPackageManager(tempDir)).toBe("pnpm");
    });

    it("prioritizes yarn lockfile over npm", () => {
      fs.writeFileSync(path.join(tempDir, "yarn.lock"), "");
      fs.writeFileSync(path.join(tempDir, "package-lock.json"), "{}");
      expect(detectPackageManager(tempDir)).toBe("yarn");
    });
  });

  describe("packageManager field detection", () => {
    it("detects pnpm from packageManager field", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: "pnpm@9.0.0" })
      );
      expect(detectPackageManager(tempDir)).toBe("pnpm");
    });

    it("detects yarn from packageManager field", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: "yarn@4.0.0" })
      );
      expect(detectPackageManager(tempDir)).toBe("yarn");
    });

    it("detects bun from packageManager field", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: "bun@1.0.0" })
      );
      expect(detectPackageManager(tempDir)).toBe("bun");
    });

    it("detects npm from packageManager field", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: "npm@10.0.0" })
      );
      expect(detectPackageManager(tempDir)).toBe("npm");
    });

    it("prioritizes lockfile over packageManager field", () => {
      fs.writeFileSync(path.join(tempDir, "yarn.lock"), "");
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: "pnpm@9.0.0" })
      );
      expect(detectPackageManager(tempDir)).toBe("yarn");
    });
  });

  describe("monorepo detection", () => {
    it("detects pnpm from workspace root when inside a package", () => {
      const repoRoot = path.join(tempDir, "repo");
      const packageDir = path.join(repoRoot, "packages", "app");
      fs.mkdirSync(packageDir, { recursive: true });
      fs.writeFileSync(path.join(repoRoot, "pnpm-workspace.yaml"), "packages:\n  - packages/*\n");
      fs.writeFileSync(path.join(repoRoot, "pnpm-lock.yaml"), "");
      expect(detectPackageManager(packageDir)).toBe("pnpm");
    });

    it("detects pnpm from workspace config even without a lockfile", () => {
      const repoRoot = path.join(tempDir, "repo");
      const packageDir = path.join(repoRoot, "packages", "app");
      fs.mkdirSync(packageDir, { recursive: true });
      fs.writeFileSync(path.join(repoRoot, "pnpm-workspace.yaml"), "packages:\n  - packages/*\n");
      expect(detectPackageManager(packageDir)).toBe("pnpm");
    });

    it("prefers the nearest lockfile in nested packages", () => {
      const repoRoot = path.join(tempDir, "repo");
      const packageDir = path.join(repoRoot, "packages", "app");
      fs.mkdirSync(packageDir, { recursive: true });
      fs.writeFileSync(path.join(repoRoot, "pnpm-lock.yaml"), "");
      fs.writeFileSync(path.join(packageDir, "yarn.lock"), "");
      expect(detectPackageManager(packageDir)).toBe("yarn");
    });
  });

  describe("default behavior", () => {
    it("defaults to npm when no indicators found", () => {
      expect(detectPackageManager(tempDir)).toBe("npm");
    });

    it("defaults to npm when package.json has no packageManager field", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ name: "test-project" })
      );
      expect(detectPackageManager(tempDir)).toBe("npm");
    });

    it("defaults to npm when package.json is invalid JSON", () => {
      fs.writeFileSync(path.join(tempDir, "package.json"), "not json");
      expect(detectPackageManager(tempDir)).toBe("npm");
    });

    it("defaults to npm when packageManager is not a string", () => {
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify({ packageManager: 123 })
      );
      expect(detectPackageManager(tempDir)).toBe("npm");
    });
  });
});

describe("getPackageManagerRunCmd", () => {
  it("maps npm to npm run", () => {
    expect(getPackageManagerRunCmd("npm")).toBe("npm run");
  });

  it("maps pnpm to pnpm run", () => {
    expect(getPackageManagerRunCmd("pnpm")).toBe("pnpm run");
  });

  it("maps yarn to yarn", () => {
    expect(getPackageManagerRunCmd("yarn")).toBe("yarn");
  });

  it("maps bun to bun run", () => {
    expect(getPackageManagerRunCmd("bun")).toBe("bun run");
  });
});

describe("getPackageManagerInstallCmd", () => {
  it("maps npm to npm install", () => {
    expect(getPackageManagerInstallCmd("npm")).toBe("npm install");
  });

  it("maps pnpm to pnpm install", () => {
    expect(getPackageManagerInstallCmd("pnpm")).toBe("pnpm install");
  });

  it("maps yarn to yarn install", () => {
    expect(getPackageManagerInstallCmd("yarn")).toBe("yarn install");
  });

  it("maps bun to bun install", () => {
    expect(getPackageManagerInstallCmd("bun")).toBe("bun install");
  });
});

describe("getPackageManagerAddCmd", () => {
  it("maps npm to npm install", () => {
    expect(getPackageManagerAddCmd("npm")).toBe("npm install");
  });

  it("maps pnpm to pnpm add", () => {
    expect(getPackageManagerAddCmd("pnpm")).toBe("pnpm add");
  });

  it("maps yarn to yarn add", () => {
    expect(getPackageManagerAddCmd("yarn")).toBe("yarn add");
  });

  it("maps bun to bun add", () => {
    expect(getPackageManagerAddCmd("bun")).toBe("bun add");
  });
});

describe("detectPackageManagerInstallCmd", () => {
  it("detects install command based on lockfile", () => {
    fs.writeFileSync(path.join(tempDir, "yarn.lock"), "");
    expect(detectPackageManagerInstallCmd(tempDir)).toBe("yarn install");
  });
});

describe("hasTinybirdSdkDependency", () => {
  it("returns true when sdk is in dependencies", () => {
    fs.writeFileSync(
      path.join(tempDir, "package.json"),
      JSON.stringify({ dependencies: { "@tinybirdco/sdk": "^1.0.0" } })
    );
    expect(hasTinybirdSdkDependency(tempDir)).toBe(true);
  });

  it("returns true when sdk is in devDependencies", () => {
    fs.writeFileSync(
      path.join(tempDir, "package.json"),
      JSON.stringify({ devDependencies: { "@tinybirdco/sdk": "^1.0.0" } })
    );
    expect(hasTinybirdSdkDependency(tempDir)).toBe(true);
  });

  it("returns false when package.json is missing or invalid", () => {
    expect(hasTinybirdSdkDependency(tempDir)).toBe(false);
    fs.writeFileSync(path.join(tempDir, "package.json"), "not json");
    expect(hasTinybirdSdkDependency(tempDir)).toBe(false);
  });

  it("checks the nearest package.json", () => {
    const repoRoot = path.join(tempDir, "repo");
    const packageDir = path.join(repoRoot, "packages", "app");
    fs.mkdirSync(packageDir, { recursive: true });
    fs.writeFileSync(
      path.join(repoRoot, "package.json"),
      JSON.stringify({ dependencies: { "@tinybirdco/sdk": "^1.0.0" } })
    );
    fs.writeFileSync(
      path.join(packageDir, "package.json"),
      JSON.stringify({ dependencies: { other: "^1.0.0" } })
    );
    expect(hasTinybirdSdkDependency(packageDir)).toBe(false);
  });
});

describe("detectPackageManagerRunCmd", () => {
  it("uses the package manager detection and mapping", () => {
    fs.writeFileSync(path.join(tempDir, "pnpm-lock.yaml"), "");
    expect(detectPackageManagerRunCmd(tempDir)).toBe("pnpm run");
  });
});
