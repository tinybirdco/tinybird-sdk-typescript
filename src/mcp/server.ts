/**
 * Tinybird DevTools MCP Server
 * Provides tools for LLMs to interact with Tinybird during development
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { loadConfig } from "../cli/config.js";
import { registerExecuteQueryTool } from "./tools/execute-query.js";

/**
 * Create and configure the MCP server
 *
 * Loads configuration from tinybird.json and registers all available tools.
 *
 * @returns Configured MCP server instance
 */
export function createMcpServer(): McpServer {
  const server = new McpServer({
    name: "tinybird-devtools-mcp",
    version: "0.0.3",
  });

  // Load config from tinybird.json
  const config = loadConfig();

  // Register tools
  registerExecuteQueryTool(server, config);

  return server;
}
