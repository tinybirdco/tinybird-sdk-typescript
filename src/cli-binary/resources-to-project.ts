/**
 * Materializes in-memory GeneratedResources to a temp directory laid out as
 * a `tb` project (datasources/*.datasource, pipes/*.pipe, connections/*.connection),
 * so the binary can be invoked against it. Mirrors `_write_virtual_resources`
 * in tinybird/tb/modules/ts_project.py, which does the same thing in the
 * opposite direction (tb -> this SDK's generator -> virtual project).
 */

import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { GeneratedResources } from "../generator/index.js";

export interface MaterializedProject {
  dir: string;
  cleanup: () => Promise<void>;
}

const LAYOUTS: Array<{ key: keyof GeneratedResources; folder: string; suffix: string }> = [
  { key: "datasources", folder: "datasources", suffix: ".datasource" },
  { key: "pipes", folder: "pipes", suffix: ".pipe" },
  { key: "connections", folder: "connections", suffix: ".connection" },
];

export async function materializeResourcesToTempProject(
  resources: GeneratedResources
): Promise<MaterializedProject> {
  const dir = await mkdtemp(join(tmpdir(), "tinybird-cli-project-"));

  for (const { key, folder, suffix } of LAYOUTS) {
    const entries = resources[key] ?? [];
    if (entries.length === 0) continue;

    const targetDir = join(dir, folder);
    await mkdir(targetDir, { recursive: true });
    for (const entry of entries) {
      await writeFile(join(targetDir, `${entry.name}${suffix}`), entry.content, "utf-8");
    }
  }

  return {
    dir,
    cleanup: () => rm(dir, { recursive: true, force: true }),
  };
}
