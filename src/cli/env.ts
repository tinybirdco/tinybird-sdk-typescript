/**
 * Environment file utilities for managing .env.local
 */

import * as fs from "fs";
import * as path from "path";

/**
 * Result of saving a token to .env.local
 */
export interface SaveTokenResult {
  /** Whether the file was newly created */
  created: boolean;
}

/**
 * Save the TINYBIRD_TOKEN to .env.local
 *
 * Handles creating, appending, or updating the token in the file.
 * Ensures proper newline handling when appending.
 *
 * @param directory - Directory where .env.local should be saved
 * @param token - The token value to save
 * @returns Result indicating if the file was created
 */
export function saveTinybirdToken(
  directory: string,
  token: string
): SaveTokenResult {
  const envLocalPath = path.join(directory, ".env.local");
  const envContent = `TINYBIRD_TOKEN=${token}\n`;

  if (fs.existsSync(envLocalPath)) {
    const existingContent = fs.readFileSync(envLocalPath, "utf-8");

    if (existingContent.includes("TINYBIRD_TOKEN=")) {
      // Replace existing token
      const updatedContent = existingContent.replace(
        /TINYBIRD_TOKEN=.*/,
        `TINYBIRD_TOKEN=${token}`
      );
      fs.writeFileSync(envLocalPath, updatedContent);
    } else {
      // Append token, ensuring proper newline
      const needsNewline = existingContent.length > 0 && !existingContent.endsWith("\n");
      fs.appendFileSync(envLocalPath, (needsNewline ? "\n" : "") + envContent);
    }

    return { created: false };
  } else {
    // Create new file
    fs.writeFileSync(envLocalPath, envContent);
    return { created: true };
  }
}
