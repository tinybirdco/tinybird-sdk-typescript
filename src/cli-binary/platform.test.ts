import { describe, expect, it } from "vitest";

import { executableName, getHostPlatform, normalizeArch, normalizeOs } from "./platform.js";

describe("normalizeOs", () => {
  it("maps known platform strings", () => {
    expect(normalizeOs("Linux")).toBe("linux");
    expect(normalizeOs("linux")).toBe("linux");
    expect(normalizeOs("Darwin")).toBe("darwin");
    expect(normalizeOs("darwin")).toBe("darwin");
    expect(normalizeOs("Windows")).toBe("windows");
    expect(normalizeOs("win32")).toBe("windows");
  });

  it("passes through unknown platforms unchanged (lowercased)", () => {
    expect(normalizeOs("SunOS")).toBe("sunos");
  });
});

describe("normalizeArch", () => {
  it("maps known architecture strings", () => {
    expect(normalizeArch("x86_64")).toBe("x64");
    expect(normalizeArch("amd64")).toBe("x64");
    expect(normalizeArch("x64")).toBe("x64");
    expect(normalizeArch("arm64")).toBe("arm64");
    expect(normalizeArch("aarch64")).toBe("arm64");
  });

  it("passes through unknown architectures unchanged (lowercased)", () => {
    expect(normalizeArch("ppc64le")).toBe("ppc64le");
  });
});

describe("getHostPlatform", () => {
  it("reflects the real process.platform/process.arch, normalized", () => {
    const host = getHostPlatform();
    expect(host.os).toBe(normalizeOs(process.platform));
    expect(host.arch).toBe(normalizeArch(process.arch));
  });
});

describe("executableName", () => {
  it("appends .exe only for windows", () => {
    expect(executableName("windows")).toBe("tb.exe");
    expect(executableName("linux")).toBe("tb");
    expect(executableName("darwin")).toBe("tb");
  });
});
