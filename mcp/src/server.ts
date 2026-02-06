/**
 * Tinybird DevTools MCP Server
 * Provides tools for LLMs to interact with Tinybird during development
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { loadConfig } from "./config.js";
import { registerExecuteQueryTool } from "./tools/execute-query.js";
import { registerListConnectionsTool } from "./tools/list-connections.js";
import { registerListKafkaTopicsTool } from "./tools/list-kafka-topics.js";
import { registerPreviewKafkaTopicTool } from "./tools/preview-kafka-topic.js";
import { registerListResourcesTool } from "./tools/list-resources.js";

/**
 * Create and configure the MCP server
 *
 * Loads configuration from tinybird.json and registers all available tools.
 *
 * @returns Configured MCP server instance
 */
export function createMcpServer(): McpServer {
  const server = new McpServer({
    name: "tinybird-devtools",
    version: "0.0.1",
  });

  // Load config from tinybird.json
  const config = loadConfig();

  // Register tools
  registerExecuteQueryTool(server, config);
  registerListConnectionsTool(server, config);
  registerListKafkaTopicsTool(server, config);
  registerPreviewKafkaTopicTool(server, config);
  registerListResourcesTool(server, config);

  return server;
}
