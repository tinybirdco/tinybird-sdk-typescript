#!/usr/bin/env node
/**
 * Tinybird DevTools MCP Server Entry Point
 *
 * Runs using stdio transport for MCP communication.
 * Load .env files before starting the server.
 */

import { config } from "dotenv";

// Load .env files in priority order
config({ path: ".env.local" });
config({ path: ".env" });

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createMcpServer } from "./server.js";

async function main() {
  const server = await createMcpServer();
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch(console.error);
