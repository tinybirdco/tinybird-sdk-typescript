import { describe, expect, it, vi } from "vitest";

import { DEFAULT_CLI_BINARIES_BASE_URL, fetchManifest, findArtifactForHost, type CliManifest } from "./manifest.js";

function sampleManifest(): CliManifest {
  return {
    version: "1.2.3",
    generated_at: "2026-01-01T00:00:00Z",
    artifacts: [
      {
        id: "darwin-arm64",
        os: "darwin",
        arch: "arm64",
        bundle_mode: "onefile",
        executable_relative_path: "tb",
        filename: "tb-1.2.3-darwin-arm64.tar.gz",
        sha256: "abc123",
        size_bytes: 42,
        relative_path: "packages/tb-1.2.3-darwin-arm64.tar.gz",
        url: "https://tinybird.co/cli/1.2.3/packages/tb-1.2.3-darwin-arm64.tar.gz",
      },
      {
        id: "windows-x64",
        os: "windows",
        arch: "x64",
        bundle_mode: "onefile",
        executable_relative_path: "tb.exe",
        filename: "tb-1.2.3-windows-x64.tar.gz",
        sha256: "def456",
        size_bytes: 43,
        relative_path: "packages/tb-1.2.3-windows-x64.tar.gz",
      },
    ],
  };
}

describe("fetchManifest", () => {
  it("fetches from the default base URL and latest version by default", async () => {
    const fetchFn = vi.fn(async (url: string) => {
      expect(url).toBe(`${DEFAULT_CLI_BINARIES_BASE_URL}/latest/manifest.json`);
      return new Response(JSON.stringify(sampleManifest()), { status: 200 });
    });

    const manifest = await fetchManifest({ fetchFn: fetchFn as unknown as typeof fetch });
    expect(manifest.version).toBe("1.2.3");
    expect(manifest.artifacts).toHaveLength(2);
  });

  it("respects an explicit baseUrl and version, stripping a trailing slash", async () => {
    const fetchFn = vi.fn(async (url: string) => {
      expect(url).toBe("https://example.com/cli/9.9.9/manifest.json");
      return new Response(JSON.stringify(sampleManifest()), { status: 200 });
    });

    await fetchManifest({
      baseUrl: "https://example.com/cli/",
      version: "9.9.9",
      fetchFn: fetchFn as unknown as typeof fetch,
    });
  });

  it("throws a clear error on a non-OK response", async () => {
    const fetchFn = vi.fn(async () => new Response("nope", { status: 404, statusText: "Not Found" }));

    await expect(fetchManifest({ fetchFn: fetchFn as unknown as typeof fetch })).rejects.toThrow(/404/);
  });

  it("throws on a malformed manifest body", async () => {
    const fetchFn = vi.fn(async () => new Response(JSON.stringify({ not: "a manifest" }), { status: 200 }));

    await expect(fetchManifest({ fetchFn: fetchFn as unknown as typeof fetch })).rejects.toThrow(/Invalid/);
  });
});

describe("findArtifactForHost", () => {
  it("finds the matching artifact by os/arch", () => {
    const manifest = sampleManifest();
    const artifact = findArtifactForHost(manifest, { os: "darwin", arch: "arm64" });
    expect(artifact?.id).toBe("darwin-arm64");
  });

  it("returns undefined when no artifact matches", () => {
    const manifest = sampleManifest();
    expect(findArtifactForHost(manifest, { os: "darwin", arch: "x64" })).toBeUndefined();
  });
});
