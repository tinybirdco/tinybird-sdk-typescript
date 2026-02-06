/**
 * Console output utilities with color support
 * Provides consistent formatting similar to the Python CLI
 */

// ANSI color codes
const colors = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  red: "\x1b[91m",
  green: "\x1b[92m",
  yellow: "\x1b[38;5;208m",
  blue: "\x1b[94m",
  gray: "\x1b[90m",
} as const;

// Check if colors should be disabled
const noColor = process.env.NO_COLOR !== undefined || !process.stdout.isTTY;

function colorize(text: string, color: keyof typeof colors): string {
  if (noColor) return text;
  return `${colors[color]}${text}${colors.reset}`;
}

/**
 * Output a success message (green)
 */
export function success(message: string): void {
  console.log(colorize(message, "green"));
}

/**
 * Output an error message (red)
 */
export function error(message: string): void {
  console.error(colorize(message, "red"));
}

/**
 * Output a warning message (yellow/orange)
 */
export function warning(message: string): void {
  console.log(colorize(message, "yellow"));
}

/**
 * Output an info message (default color)
 */
export function info(message: string): void {
  console.log(message);
}

/**
 * Output a highlighted message (blue)
 */
export function highlight(message: string): void {
  console.log(colorize(message, "blue"));
}

/**
 * Output a gray message (dimmed)
 */
export function gray(message: string): void {
  console.log(colorize(message, "gray"));
}

/**
 * Output a bold message
 */
export function bold(message: string): void {
  console.log(colorize(message, "bold"));
}

/**
 * Format a timestamp for console output
 */
export function formatTime(): string {
  return new Date().toLocaleTimeString("en-US", { hour12: false });
}

/**
 * Format duration in human-readable format
 */
export function formatDuration(ms: number): string {
  if (ms < 1000) {
    return `${ms}ms`;
  }
  return `${(ms / 1000).toFixed(1)}s`;
}

/**
 * Show a resource change (checkmark + path + status)
 */
export function showResourceChange(
  path: string,
  status: "created" | "changed" | "deleted"
): void {
  console.log(`✓ ${path} ${status}`);
}

/**
 * Show a warning for a resource
 */
export function showResourceWarning(
  level: string,
  resource: string,
  message: string
): void {
  warning(`△ ${level}: ${resource}: ${message}`);
}

/**
 * Show build errors in formatted style
 */
export function showBuildErrors(errors: Array<{ filename?: string; error: string }>): void {
  for (const err of errors) {
    if (err.filename) {
      error(`${err.filename}`);
      // Indent the error message
      const lines = err.error.split("\n");
      for (const line of lines) {
        error(`  ${line}`);
      }
    } else {
      error(err.error);
    }
    console.log(); // Empty line between errors
  }
}

/**
 * Show final build success message
 */
export function showBuildSuccess(durationMs: number, isRebuild = false): void {
  const prefix = isRebuild ? "Rebuild" : "Build";
  success(`\n✓ ${prefix} completed in ${formatDuration(durationMs)}`);
}

/**
 * Show final build failure message
 */
export function showBuildFailure(isRebuild = false): void {
  const prefix = isRebuild ? "Rebuild" : "Build";
  error(`\n✗ ${prefix} failed`);
}

/**
 * Show no changes message
 */
export function showNoChanges(): void {
  warning("△ Not deploying. No changes.");
}

/**
 * Show waiting for deployment message
 */
export function showWaitingForDeployment(): void {
  info("» Waiting for deployment to be ready...");
}

/**
 * Show deployment ready message
 */
export function showDeploymentReady(): void {
  success("✓ Deployment is ready");
}

/**
 * Show deployment live message
 */
export function showDeploymentLive(deploymentId: string): void {
  success(`✓ Deployment #${deploymentId} is live!`);
}

/**
 * Show validating deployment message
 */
export function showValidatingDeployment(): void {
  info("» Validating deployment...");
}

/**
 * Show final deploy success message
 */
export function showDeploySuccess(durationMs: number): void {
  success(`\n✓ Deploy completed in ${formatDuration(durationMs)}`);
}

/**
 * Show final deploy failure message
 */
export function showDeployFailure(): void {
  error(`\n✗ Deploy failed`);
}

/**
 * Output object containing all output functions
 */
export const output = {
  success,
  error,
  warning,
  info,
  highlight,
  gray,
  bold,
  formatTime,
  formatDuration,
  showResourceChange,
  showResourceWarning,
  showBuildErrors,
  showBuildSuccess,
  showBuildFailure,
  showNoChanges,
  showWaitingForDeployment,
  showDeploymentReady,
  showDeploymentLive,
  showValidatingDeployment,
  showDeploySuccess,
  showDeployFailure,
};
