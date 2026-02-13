import type { ParsedResource, ResourceFile } from "./types.js";
import { parseDatasourceFile } from "./parse-datasource.js";
import { parsePipeFile } from "./parse-pipe.js";
import { parseConnectionFile } from "./parse-connection.js";

export function parseResourceFile(resource: ResourceFile): ParsedResource {
  switch (resource.kind) {
    case "datasource":
      return parseDatasourceFile(resource);
    case "pipe":
      return parsePipeFile(resource);
    case "connection":
      return parseConnectionFile(resource);
    default: {
      const exhaustive: never = resource.kind;
      throw new Error(`Unsupported resource kind: ${String(exhaustive)}`);
    }
  }
}

