/** @type {import("@tinybirdco/sdk").TinybirdConfig} */
export default {
  include: [
    "src/tinybird/datasources.ts",
    "src/tinybird/pipes.ts",
    "src/tinybird/endpoints.ts",
    "src/tinybird/mvs.ts",
    "src/tinybird/copies.ts",
    "src/tinybird/dummy.datasource",
  ],
  token: process.env.TINYBIRD_TOKEN,
  baseUrl: "https://api.tinybird.co",
};
