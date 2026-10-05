/**
 * Downloads, verifies, and caches the `tb` CLI binary tarball described by a
 * manifest artifact (see manifest.ts). Each tarball is a gzip'd ustar archive
 * containing exactly one regular file (the `tb`/`tb.exe` executable) — see
 * `package_artifact()` in tinybird/tb/modules/binary.py (MR !23765), which
 * builds it with Python's `tarfile.open(destination, "w:gz")` plus a single
 * `tar.add(artifact_path, arcname=artifact_path.name)` call.
 */

import { createHash } from "node:crypto";
import { gunzipSync } from "node:zlib";
import { chmod, mkdir, rename, writeFile } from "node:fs/promises";
import { access } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

import type { ManifestArtifact } from "./manifest.js";

export class ChecksumMismatchError extends Error {}

function sha256Hex(buffer: Buffer): string {
  return createHash("sha256").update(buffer).digest("hex");
}

/**
 * Reads a single-entry ustar archive (after gunzip) and returns that entry's
 * content. Not a general-purpose tar reader: it only reads the first header
 * block, which is all these always-single-file archives contain.
 */
export function extractSingleFileTarGz(tarGzBuffer: Buffer): Buffer {
  const tarBuffer = gunzipSync(tarGzBuffer);

  if (tarBuffer.length < 512) {
    throw new Error("Invalid tar archive: shorter than one header block");
  }

  // ustar header: size is a 12-byte octal ASCII field at offset 124.
  const sizeField = tarBuffer
    .subarray(124, 124 + 12)
    .toString("ascii")
    .replace(/\0.*$/, "")
    .trim();
  const size = Number.parseInt(sizeField, 8);
  if (!Number.isFinite(size) || size < 0) {
    throw new Error(`Invalid tar header: could not read file size (raw field: ${JSON.stringify(sizeField)})`);
  }

  const content = tarBuffer.subarray(512, 512 + size);
  if (content.length !== size) {
    throw new Error(
      `Invalid tar archive: expected ${size} bytes of content, found ${content.length}`
    );
  }

  return content;
}

export function getCacheDir(): string {
  if (process.env.TB_CLI_CACHE_DIR) {
    return process.env.TB_CLI_CACHE_DIR;
  }
  const xdgCacheHome = process.env.XDG_CACHE_HOME;
  const base = xdgCacheHome && xdgCacheHome.length > 0 ? xdgCacheHome : join(homedir(), ".cache");
  return join(base, "tinybird-cli");
}

async function isExecutableFile(path: string): Promise<boolean> {
  try {
    await access(path, fsConstants.X_OK);
    return true;
  } catch {
    return false;
  }
}

export interface EnsureBinaryDownloadedOptions {
  /** Resolved manifest version (not "latest" — the concrete version string). */
  version: string;
  artifact: ManifestArtifact;
  fetchFn?: typeof fetch;
  cacheDir?: string;
}

/**
 * Ensures the binary described by `artifact` is present in the local cache,
 * downloading and verifying it if necessary, and returns its path.
 */
export async function ensureBinaryDownloaded(options: EnsureBinaryDownloadedOptions): Promise<string> {
  const { version, artifact } = options;
  const fetchFn = options.fetchFn ?? fetch;
  const cacheDir = options.cacheDir ?? getCacheDir();
  const versionDir = join(cacheDir, version);
  const finalPath = join(versionDir, artifact.executable_relative_path);

  if (await isExecutableFile(finalPath)) {
    return finalPath;
  }

  if (!artifact.url) {
    throw new Error(
      `Manifest artifact for ${artifact.os}-${artifact.arch} has no download URL ` +
        `(manifest was generated without --base-url). Set TB_CLI_BINARY_PATH to use a local binary instead.`
    );
  }

  const response = await fetchFn(artifact.url);
  if (!response.ok) {
    throw new Error(
      `Failed to download Tinybird CLI binary from ${artifact.url}: ${response.status} ${response.statusText}`
    );
  }

  const tarGzBuffer = Buffer.from(await response.arrayBuffer());

  const actualSha256 = sha256Hex(tarGzBuffer);
  if (actualSha256 !== artifact.sha256) {
    throw new ChecksumMismatchError(
      `Checksum mismatch for Tinybird CLI binary ${artifact.filename}: ` +
        `expected ${artifact.sha256}, got ${actualSha256}`
    );
  }

  const executable = extractSingleFileTarGz(tarGzBuffer);

  await mkdir(versionDir, { recursive: true });
  // Download to a unique temp name and rename into place so concurrent
  // processes racing to populate the same cache dir never see a partial file.
  const tempPath = `${finalPath}.download-${process.pid}-${Date.now()}`;
  await writeFile(tempPath, executable);
  await chmod(tempPath, 0o755);
  await rename(tempPath, finalPath);

  return finalPath;
}
