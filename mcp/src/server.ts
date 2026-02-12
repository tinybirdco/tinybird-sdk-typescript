/**
 * Tinybird DevTools MCP Server
 * Provides tools for LLMs to interact with Tinybird during development
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { loadConfigAsync } from "./config.js";
import { registerExecuteQueryTool } from "./tools/execute-query.js";
import { registerListKafkaTopicsTool } from "./tools/list-kafka-topics.js";
import { registerPreviewKafkaTopicTool } from "./tools/preview-kafka-topic.js";
import { registerListResourcesTool } from "./tools/list-resources.js";
import { registerGetResourceTool } from "./tools/get-resource.js";
import { registerListBranchesTool } from "./tools/list-branches.js";
import { registerLoginTool } from "./tools/login.js";
import { registerBuildTool } from "./tools/build.js";
import { registerInfoTool } from "./tools/info.js";
import { registerOpenDashboardTool } from "./tools/open-dashboard.js";
import { registerQueryLogsTool } from "./tools/query-logs.js";
import pkg from "../package.json" with { type: "json" }

/**
 * Create and configure the MCP server
 *
 * Loads configuration from tinybird.config.* or tinybird.json and registers all available tools.
 *
 * @returns Configured MCP server instance
 */
export async function createMcpServer(): Promise<McpServer> {
  const server = new McpServer({
    name: "tinybird-devtools-mcp",
    version: pkg.version,
  });

  // Load config from tinybird.config.* or tinybird.json
  const config = await loadConfigAsync();

  // Register tools
  registerExecuteQueryTool(server, config);
  registerListResourcesTool(server, config);
  registerGetResourceTool(server, config);
  registerListBranchesTool(server, config);
  registerListKafkaTopicsTool(server, config);
  registerPreviewKafkaTopicTool(server, config);

  // SDK command tools
  registerLoginTool(server, config);
  registerBuildTool(server, config);
  registerInfoTool(server, config);
  registerOpenDashboardTool(server, config);

  // Observability tools
  registerQueryLogsTool(server, config);

  return server;
}
