/**
 * Browser-based authentication for Tinybird CLI
 *
 * Implements OAuth flow via local HTTP server callback
 */

import * as http from "node:http";
import * as crypto from "node:crypto";
import { spawn } from "node:child_process";
import { platform } from "node:os";
import { URL } from "node:url";

/**
 * Port for the local OAuth callback server
 */
export const AUTH_SERVER_PORT = 49160;

/**
 * Default auth host (Tinybird cloud)
 */
export const DEFAULT_AUTH_HOST = "https://cloud.tinybird.co";

/**
 * Default API host (EU region)
 */
export const DEFAULT_API_HOST = "https://api.tinybird.co";

/**
 * Maximum time to wait for authentication (in seconds)
 */
export const SERVER_MAX_WAIT_TIME = 180;

/**
 * Get the auth host from environment or use default
 */
export function getAuthHost(): string {
  return process.env.TINYBIRD_AUTH_HOST ?? DEFAULT_AUTH_HOST;
}

/**
 * Result of a login attempt
 */
export interface AuthResult {
  success: boolean;
  token?: string;
  baseUrl?: string;
  workspaceName?: string;
  userEmail?: string;
  error?: string;
}

/**
 * Options for the browser login flow
 */
export interface LoginOptions {
  /** Override the default auth host */
  authHost?: string;
  /** Override the API host (region) */
  apiHost?: string;
}

/**
 * Token response from Tinybird auth API
 */
interface TokenResponse {
  workspace_token: string;
  user_token: string;
  api_host: string;
  workspace_name?: string;
  user_email?: string;
}

/**
 * Generate a cryptographically strong random state for OAuth
 */
function generateState(): string {
  return crypto.randomBytes(32).toString("hex");
}

/**
 * Generate the HTML callback page served by the local server
 *
 * This page extracts the code and state from the query string and POSTs them back to the server
 */
function getCallbackHtml(authHost: string, expectedState: string): string {
  return `<!DOCTYPE html>
<html>
<head>
  <style>
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background: #f5f5f5;
      display: flex;
      align-items: center;
      justify-content: center;
      height: 100vh;
      margin: 0;
    }
    .container {
      text-align: center;
      padding: 2rem;
    }
    .spinner {
      border: 3px solid #e0e0e0;
      border-top: 3px solid #333;
      border-radius: 50%;
      width: 30px;
      height: 30px;
      animation: spin 1s linear infinite;
      margin: 0 auto 1rem;
    }
    @keyframes spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="spinner"></div>
    <p>Completing authentication...</p>
  </div>
  <script>
    const searchParams = new URLSearchParams(window.location.search);
    const code = searchParams.get('code');
    const state = searchParams.get('state');
    const workspace = searchParams.get('workspace');
    const region = searchParams.get('region');
    const provider = searchParams.get('provider');
    const host = "${authHost}";
    const expectedState = "${expectedState}";

    if (!code) {
      document.querySelector('.container').innerHTML = '<p>Missing authentication code. Please try again.</p>';
    } else if (state !== expectedState) {
      document.querySelector('.container').innerHTML = '<p>Invalid state parameter. This may be a security issue. Please try again.</p>';
    } else {
      fetch('/?code=' + encodeURIComponent(code) + '&state=' + encodeURIComponent(state), { method: 'POST' })
        .then(() => {
          if (provider && region && workspace) {
            window.location.href = host + "/" + provider + "/" + region + "/cli-login?workspace=" + workspace;
          } else {
            document.querySelector('.container').innerHTML = '<p>Authentication successful! You can close this tab.</p>';
          }
        })
        .catch(() => {
          document.querySelector('.container').innerHTML = '<p>Authentication failed. Please try again.</p>';
        });
    }
  </script>
</body>
</html>`;
}

/**
 * Start a local HTTP server to receive the OAuth callback
 *
 * @param onCode - Callback invoked when auth code is received with valid state
 * @param authHost - Auth host for redirect URL in HTML
 * @param expectedState - The expected OAuth state parameter for validation
 * @returns Promise that resolves to the server instance
 */
function startAuthServer(
  onCode: (code: string) => void,
  authHost: string,
  expectedState: string
): Promise<http.Server> {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      const url = new URL(req.url ?? "/", `http://localhost:${AUTH_SERVER_PORT}`);

      if (req.method === "GET") {
        // Serve the callback HTML page
        res.writeHead(200, { "Content-Type": "text/html" });
        res.end(getCallbackHtml(authHost, expectedState));
      } else if (req.method === "POST") {
        // Receive the auth code and validate state
        const code = url.searchParams.get("code");
        const state = url.searchParams.get("state");

        if (!code) {
          res.writeHead(400);
          res.end("Missing code parameter");
          return;
        }

        if (state !== expectedState) {
          res.writeHead(400);
          res.end("Invalid state parameter");
          return;
        }

        onCode(code);
        res.writeHead(200);
        res.end();
      } else {
        res.writeHead(405);
        res.end("Method not allowed");
      }
    });

    server.on("error", (err) => {
      reject(new Error(`Failed to start auth server: ${err.message}`));
    });

    // Bind to localhost only for security (prevents network access)
    server.listen(AUTH_SERVER_PORT, "127.0.0.1", () => {
      resolve(server);
    });
  });
}

/**
 * Open a URL in the user's default browser
 *
 * Cross-platform support for macOS, Linux, and Windows
 *
 * @param url - URL to open
 * @returns Promise that resolves to true if browser was opened successfully
 */
export async function openBrowser(url: string): Promise<boolean> {
  const os = platform();

  let command: string;
  let args: string[];

  switch (os) {
    case "darwin":
      command = "open";
      args = [url];
      break;
    case "win32":
      command = "cmd";
      args = ["/c", "start", "", url];
      break;
    default:
      // Linux and others
      command = "xdg-open";
      args = [url];
      break;
  }

  return new Promise((resolve) => {
    try {
      const child = spawn(command, args, {
        detached: true,
        stdio: "ignore",
      });

      child.unref();

      child.on("error", () => {
        resolve(false);
      });

      // Give it a moment to potentially fail
      setTimeout(() => resolve(true), 500);
    } catch {
      resolve(false);
    }
  });
}

/**
 * Exchange an authorization code for tokens
 *
 * @param code - Authorization code from OAuth callback
 * @param authHost - Auth host URL
 * @returns Promise that resolves to token response
 */
export async function exchangeCodeForTokens(
  code: string,
  authHost: string
): Promise<TokenResponse> {
  const url = new URL("/api/cli-login", authHost);
  url.searchParams.set("code", code);

  const response = await fetch(url.toString());

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `Token exchange failed: ${response.status} ${response.statusText}\n${body}`
    );
  }

  return (await response.json()) as TokenResponse;
}

/**
 * Perform browser-based login flow
 *
 * 1. Starts a local HTTP server for OAuth callback
 * 2. Opens the user's browser to the auth URL
 * 3. Waits for the callback with auth code
 * 4. Exchanges the code for tokens
 *
 * @param options - Login options
 * @returns Promise that resolves to auth result
 */
export async function browserLogin(
  options: LoginOptions = {}
): Promise<AuthResult> {
  const authHost = options.authHost ?? getAuthHost();
  const apiHost = options.apiHost ?? DEFAULT_API_HOST;

  // Generate a cryptographically strong state for CSRF protection
  const state = generateState();

  let server: http.Server | null = null;
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  try {
    // Start the server first
    const serverPromise = new Promise<{ server: http.Server; code: string }>((resolve, reject) => {
      // Set up timeout
      timeoutId = setTimeout(() => {
        reject(new Error("Authentication timed out after 180 seconds"));
      }, SERVER_MAX_WAIT_TIME * 1000);

      startAuthServer(
        (code) => {
          if (timeoutId) clearTimeout(timeoutId);
          if (server) {
            resolve({ server, code });
          }
        },
        authHost,
        state
      )
        .then((srv) => {
          server = srv;
        })
        .catch(reject);
    });

    // Wait for server to start
    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    // Build auth URL with state parameter
    const authUrl = new URL("/api/cli-login", authHost);
    authUrl.searchParams.set("apiHost", apiHost);
    authUrl.searchParams.set("state", state);

    console.log("Opening browser for authentication...");

    // Open browser
    await openBrowser(authUrl.toString());

    console.log("\nIf the browser doesn't open, please visit:");
    console.log(authUrl.toString());

    // Wait for auth code
    const { code } = await serverPromise;

    // Exchange code for tokens
    console.log("\nExchanging code for tokens...");
    const tokens = await exchangeCodeForTokens(code, authHost);

    return {
      success: true,
      token: tokens.workspace_token,
      baseUrl: tokens.api_host,
      workspaceName: tokens.workspace_name,
      userEmail: tokens.user_email,
    };
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
    };
  } finally {
    // Clean up
    if (timeoutId) clearTimeout(timeoutId);
    if (server) {
      (server as http.Server).close();
    }
  }
}
