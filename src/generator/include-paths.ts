import * as fs from "node:fs";
import * as path from "node:path";

export interface ResolvedIncludeFile {
  sourcePath: string;
  absolutePath: string;
}

const GLOB_SEGMENT_REGEX = /[*?[]/;
const IGNORED_DIRECTORIES = new Set([".git", "node_modules"]);
const SEGMENT_REGEX_CACHE = new Map<string, RegExp>();

function hasGlobPattern(value: string): boolean {
  return GLOB_SEGMENT_REGEX.test(value);
}

function normalizePath(value: string): string {
  return value.replace(/\\/g, "/");
}

function splitAbsolutePath(filePath: string): { root: string; segments: string[] } {
  const absolutePath = path.resolve(filePath);
  const root = path.parse(absolutePath).root;
  const relative = path.relative(root, absolutePath);

  return {
    root: normalizePath(root),
    segments: normalizePath(relative).split("/").filter(Boolean),
  };
}

function segmentMatcher(segment: string): RegExp {
  const cached = SEGMENT_REGEX_CACHE.get(segment);
  if (cached) {
    return cached;
  }

  const escaped = segment
    .replace(/[.+^${}()|\\]/g, "\\$&")
    .replace(/\*/g, "[^/]*")
    .replace(/\?/g, "[^/]");

  const matcher = new RegExp(`^${escaped}$`);
  SEGMENT_REGEX_CACHE.set(segment, matcher);
  return matcher;
}

function matchSegment(patternSegment: string, valueSegment: string): boolean {
  if (!hasGlobPattern(patternSegment)) {
    return patternSegment === valueSegment;
  }

  return segmentMatcher(patternSegment).test(valueSegment);
}

function matchGlobSegments(
  patternSegments: string[],
  pathSegments: string[],
  patternIndex: number,
  pathIndex: number,
  memo: Map<string, boolean>
): boolean {
  const key = `${patternIndex}:${pathIndex}`;
  const cached = memo.get(key);
  if (cached !== undefined) {
    return cached;
  }

  if (patternIndex === patternSegments.length) {
    const matches = pathIndex === pathSegments.length;
    memo.set(key, matches);
    return matches;
  }

  const patternSegment = patternSegments[patternIndex];
  let matches = false;

  if (patternSegment === "**") {
    matches = matchGlobSegments(patternSegments, pathSegments, patternIndex + 1, pathIndex, memo);

    if (!matches && pathIndex < pathSegments.length) {
      matches = matchGlobSegments(patternSegments, pathSegments, patternIndex, pathIndex + 1, memo);
    }
  } else if (
    pathIndex < pathSegments.length &&
    matchSegment(patternSegment, pathSegments[pathIndex])
  ) {
    matches = matchGlobSegments(patternSegments, pathSegments, patternIndex + 1, pathIndex + 1, memo);
  }

  memo.set(key, matches);
  return matches;
}

function matchGlobPath(absolutePattern: string, absolutePath: string): boolean {
  const patternParts = splitAbsolutePath(absolutePattern);
  const pathParts = splitAbsolutePath(absolutePath);

  if (patternParts.root.toLowerCase() !== pathParts.root.toLowerCase()) {
    return false;
  }

  return matchGlobSegments(
    patternParts.segments,
    pathParts.segments,
    0,
    0,
    new Map<string, boolean>()
  );
}

function getGlobRootDirectory(absolutePattern: string): string {
  const { root, segments } = splitAbsolutePath(absolutePattern);
  const firstGlobIndex = segments.findIndex((segment) => hasGlobPattern(segment));
  const baseSegments =
    firstGlobIndex === -1 ? segments : segments.slice(0, firstGlobIndex);

  return path.join(root, ...baseSegments);
}

function collectFilesRecursive(directory: string, result: string[]): void {
  const entries = fs.readdirSync(directory, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(directory, entry.name);

    if (entry.isDirectory()) {
      if (IGNORED_DIRECTORIES.has(entry.name)) {
        continue;
      }

      collectFilesRecursive(fullPath, result);
      continue;
    }

    if (entry.isFile()) {
      result.push(fullPath);
    }
  }
}

function expandGlobPattern(absolutePattern: string): string[] {
  const rootDirectory = getGlobRootDirectory(absolutePattern);

  if (!fs.existsSync(rootDirectory)) {
    return [];
  }

  if (!fs.statSync(rootDirectory).isDirectory()) {
    return [];
  }

  const files: string[] = [];
  collectFilesRecursive(rootDirectory, files);

  return files
    .filter((filePath) => matchGlobPath(absolutePattern, filePath))
    .sort((a, b) => a.localeCompare(b));
}

export function resolveIncludeFiles(
  includePaths: string[],
  cwd: string
): ResolvedIncludeFile[] {
  const resolved: ResolvedIncludeFile[] = [];
  const seen = new Set<string>();

  for (const includePath of includePaths) {
    const absoluteIncludePath = path.isAbsolute(includePath)
      ? includePath
      : path.resolve(cwd, includePath);

    if (hasGlobPattern(includePath)) {
      const matchedFiles = expandGlobPattern(absoluteIncludePath);

      if (matchedFiles.length === 0) {
        throw new Error(`Include pattern matched no files: ${includePath}`);
      }

      for (const matchedFile of matchedFiles) {
        if (seen.has(matchedFile)) {
          continue;
        }

        seen.add(matchedFile);
        resolved.push({
          sourcePath: path.isAbsolute(includePath)
            ? matchedFile
            : path.relative(cwd, matchedFile),
          absolutePath: matchedFile,
        });
      }
      continue;
    }

    if (!fs.existsSync(absoluteIncludePath)) {
      throw new Error(`Include file not found: ${absoluteIncludePath}`);
    }

    if (seen.has(absoluteIncludePath)) {
      continue;
    }

    seen.add(absoluteIncludePath);
    resolved.push({
      sourcePath: includePath,
      absolutePath: absoluteIncludePath,
    });
  }

  return resolved;
}

export function getIncludeWatchDirectories(
  includePaths: string[],
  cwd: string
): string[] {
  const watchDirs = new Set<string>();

  for (const includePath of includePaths) {
    const absoluteIncludePath = path.isAbsolute(includePath)
      ? includePath
      : path.resolve(cwd, includePath);

    if (hasGlobPattern(includePath)) {
      watchDirs.add(getGlobRootDirectory(absoluteIncludePath));
      continue;
    }

    watchDirs.add(path.dirname(absoluteIncludePath));
  }

  return Array.from(watchDirs);
}
