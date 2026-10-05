import { readFile, readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import { materializeResourcesToTempProject } from "./resources-to-project.js";
import type { GeneratedResources } from "../generator/index.js";

describe("materializeResourcesToTempProject", () => {
  const cleanups: Array<() => Promise<void>> = [];

  afterEach(async () => {
    await Promise.all(cleanups.map((cleanup) => cleanup()));
    cleanups.length = 0;
  });

  it("writes datasources, pipes, and connections to their respective folders", async () => {
    const resources: GeneratedResources = {
      datasources: [{ name: "events", content: "SCHEMA > timestamp DateTime" }],
      pipes: [{ name: "top_events", content: "NODE main\nSQL > SELECT 1" }],
      connections: [{ name: "my_kafka", content: "TYPE kafka" }],
    };

    const project = await materializeResourcesToTempProject(resources);
    cleanups.push(project.cleanup);

    expect(await readFile(join(project.dir, "datasources", "events.datasource"), "utf-8")).toBe(
      "SCHEMA > timestamp DateTime"
    );
    expect(await readFile(join(project.dir, "pipes", "top_events.pipe"), "utf-8")).toBe(
      "NODE main\nSQL > SELECT 1"
    );
    expect(await readFile(join(project.dir, "connections", "my_kafka.connection"), "utf-8")).toBe("TYPE kafka");
  });

  it("skips creating folders for empty resource kinds", async () => {
    const resources: GeneratedResources = {
      datasources: [{ name: "events", content: "x" }],
      pipes: [],
      connections: [],
    };

    const project = await materializeResourcesToTempProject(resources);
    cleanups.push(project.cleanup);

    const entries = await readdir(project.dir);
    expect(entries.sort()).toEqual(["datasources"]);
  });

  it("cleanup removes the temp directory", async () => {
    const project = await materializeResourcesToTempProject({ datasources: [], pipes: [], connections: [] });
    await project.cleanup();

    await expect(stat(project.dir)).rejects.toThrow();
  });
});
