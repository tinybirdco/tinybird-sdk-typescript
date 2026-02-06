#!/usr/bin/env node
/**
 * Tinybird DevTools MCP Server Entry Point
 *
 * Runs as an HTTP server using StreamableHTTP transport for MCP communication.
 * Load .env files before starting the server.
 */

import { config } from "dotenv";

// Load .env files in priority order
config({ path: ".env.local" });
config({ path: ".env" });

import { createServer } from "node:http";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createMcpServer } from "./server.js";

const PORT = Number(process.env.MCP_PORT) || 8000;

async function main() {
  const mcpServer = createMcpServer();

  // Create a map to store transports by session ID
  const transports = new Map<string, StreamableHTTPServerTransport>();

  const httpServer = createServer(async (req, res) => {
    // Handle CORS preflight
    if (req.method === "OPTIONS") {
      res.writeHead(204, {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, mcp-session-id",
      });
      res.end();
      return;
    }

    // Add CORS headers to all responses
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, mcp-session-id");

    const sessionId = req.headers["mcp-session-id"] as string | undefined;

    if (req.method === "POST") {
      // Check for existing session
      let transport = sessionId ? transports.get(sessionId) : undefined;

      if (!transport) {
        // Create new transport for new session
        transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => crypto.randomUUID(),
          onsessioninitialized: (id) => {
            transports.set(id, transport!);
          },
        });

        // Connect to MCP server
        await mcpServer.connect(transport);
      }

      // Handle the request
      await transport.handleRequest(req, res);
    } else if (req.method === "GET") {
      // SSE endpoint for server-to-client notifications
      if (sessionId && transports.has(sessionId)) {
        const transport = transports.get(sessionId)!;
        await transport.handleRequest(req, res);
      } else {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Missing or invalid session ID" }));
      }
    } else if (req.method === "DELETE") {
      // Session termination
      if (sessionId && transports.has(sessionId)) {
        const transport = transports.get(sessionId)!;
        await transport.handleRequest(req, res);
        transports.delete(sessionId);
      } else {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Missing or invalid session ID" }));
      }
    } else {
      res.writeHead(405, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Method not allowed" }));
    }
  });

  httpServer.listen(PORT, () => {
    console.log(`Tinybird DevTools MCP server running on http://localhost:${PORT}`);
  });
}

main().catch(console.error);
