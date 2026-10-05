import { createHash } from "node:crypto";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gzipSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  ChecksumMismatchError,
  ensureBinaryDownloaded,
  extractSingleFileTarGz,
  getCacheDir,
} from "./download.js";
import type { ManifestArtifact } from "./manifest.js";

/**
 * Builds a minimal valid single-entry ustar archive (gzip'd), equivalent to
 * what Python's `tarfile.open(dest, "w:gz")` + one `tar.add(path, arcname)`
 * call produces (see package_artifact() in tinybird/tb/modules/binary.py).
 */
function buildTarGz(entryName: string, content: Buffer): Buffer {
  const header = Buffer.alloc(512);
  header.write(entryName, 0, "ascii");
  header.write("0000755\0", 100, "ascii"); // mode
  header.write("0000000\0", 108, "ascii"); // uid
  header.write("0000000\0", 116, "ascii"); // gid
  header.write(`${content.length.toString(8).padStart(11, "0")}\0`, 124, "ascii"); // size
  header.write("00000000000\0", 136, "ascii"); // mtime
  header.write("        ", 148, "ascii"); // checksum placeholder (8 spaces)
  header.write("0", 156, "ascii"); // typeflag: regular file
  header.write("ustar\0", 257, "ascii"); // magic
  header.write("00", 263, "ascii"); // version

  let checksum = 0;
  for (const byte of header) checksum += byte;
  header.write(`${checksum.toString(8).padStart(6, "0")}\0 `, 148, "ascii");

  const contentBlockSize = Math.ceil(content.length / 512) * 512;
  const contentBlock = Buffer.alloc(contentBlockSize);
  content.copy(contentBlock);

  const tar = Buffer.concat([header, contentBlock, Buffer.alloc(1024)]); // two zero blocks at EOF
  return gzipSync(tar);
}

describe("extractSingleFileTarGz", () => {
  it("extracts the single file's content", () => {
    const content = Buffer.from("fake-binary-content");
    const tarGz = buildTarGz("tb", content);
    expect(extractSingleFileTarGz(tarGz)).toEqual(content);
  });

  it("works for content spanning multiple 512-byte blocks", () => {
    const content = Buffer.from("x".repeat(512 * 2 + 37));
    const tarGz = buildTarGz("tb", content);
    expect(extractSingleFileTarGz(tarGz)).toEqual(content);
  });

  it("throws on a truncated archive", () => {
    expect(() => extractSingleFileTarGz(gzipSync(Buffer.alloc(10)))).toThrow(/shorter than one header block/);
  });
});

describe("getCacheDir", () => {
  const originalCacheDir = process.env.TB_CLI_CACHE_DIR;
  const originalXdg = process.env.XDG_CACHE_HOME;

  afterEach(() => {
    if (originalCacheDir === undefined) delete process.env.TB_CLI_CACHE_DIR;
    else process.env.TB_CLI_CACHE_DIR = originalCacheDir;
    if (originalXdg === undefined) delete process.env.XDG_CACHE_HOME;
    else process.env.XDG_CACHE_HOME = originalXdg;
  });

  it("honors TB_CLI_CACHE_DIR when set", () => {
    process.env.TB_CLI_CACHE_DIR = "/tmp/custom-cache";
    expect(getCacheDir()).toBe("/tmp/custom-cache");
  });

  it("falls back to XDG_CACHE_HOME/tinybird-cli", () => {
    delete process.env.TB_CLI_CACHE_DIR;
    process.env.XDG_CACHE_HOME = "/tmp/xdg-cache";
    expect(getCacheDir()).toBe(join("/tmp/xdg-cache", "tinybird-cli"));
  });
});

describe("ensureBinaryDownloaded", () => {
  let cacheDir: string;

  beforeEach(async () => {
    cacheDir = await mkdtemp(join(tmpdir(), "tb-cli-cache-test-"));
  });

  afterEach(async () => {
    await rm(cacheDir, { recursive: true, force: true });
  });

  function artifactFor(content: Buffer): ManifestArtifact {
    return {
      id: "linux-x64",
      os: "linux",
      arch: "x64",
      bundle_mode: "onefile",
      executable_relative_path: "tb",
      filename: "tb-1.0.0-linux-x64.tar.gz",
      sha256: createHash("sha256").update(buildTarGz("tb", content)).digest("hex"),
      size_bytes: content.length,
      relative_path: "packages/tb-1.0.0-linux-x64.tar.gz",
      url: "https://example.com/tb-1.0.0-linux-x64.tar.gz",
    };
  }

  it("downloads, verifies, extracts, and caches the binary", async () => {
    const content = Buffer.from("fake-binary");
    const artifact = artifactFor(content);
    const tarGz = buildTarGz("tb", content);

    const fetchFn = async () => new Response(new Uint8Array(tarGz), { status: 200 });

    const path = await ensureBinaryDownloaded({
      version: "1.0.0",
      artifact,
      cacheDir,
      fetchFn: fetchFn as unknown as typeof fetch,
    });

    expect(path).toBe(join(cacheDir, "1.0.0", "tb"));
    expect(await readFile(path)).toEqual(content);
  });

  it("reuses the cached binary on a second call without fetching again", async () => {
    const content = Buffer.from("fake-binary");
    const artifact = artifactFor(content);
    const tarGz = buildTarGz("tb", content);

    let fetchCount = 0;
    const fetchFn = async () => {
      fetchCount++;
      return new Response(new Uint8Array(tarGz), { status: 200 });
    };

    const options = { version: "1.0.0", artifact, cacheDir, fetchFn: fetchFn as unknown as typeof fetch };
    await ensureBinaryDownloaded(options);
    await ensureBinaryDownloaded(options);

    expect(fetchCount).toBe(1);
  });

  it("throws ChecksumMismatchError when the downloaded tarball doesn't match", async () => {
    const artifact = artifactFor(Buffer.from("expected"));
    const fetchFn = async () => new Response(new Uint8Array(buildTarGz("tb", Buffer.from("tampered"))), { status: 200 });

    await expect(
      ensureBinaryDownloaded({ version: "1.0.0", artifact, cacheDir, fetchFn: fetchFn as unknown as typeof fetch })
    ).rejects.toThrow(ChecksumMismatchError);
  });

  it("throws a clear error when the artifact has no download URL", async () => {
    const artifact = { ...artifactFor(Buffer.from("x")), url: undefined };

    await expect(ensureBinaryDownloaded({ version: "1.0.0", artifact, cacheDir })).rejects.toThrow(
      /TB_CLI_BINARY_PATH/
    );
  });

  it("throws a clear error on a non-OK download response", async () => {
    const artifact = artifactFor(Buffer.from("x"));
    const fetchFn = async () => new Response("nope", { status: 500, statusText: "Internal Server Error" });

    await expect(
      ensureBinaryDownloaded({ version: "1.0.0", artifact, cacheDir, fetchFn: fetchFn as unknown as typeof fetch })
    ).rejects.toThrow(/500/);
  });
});
