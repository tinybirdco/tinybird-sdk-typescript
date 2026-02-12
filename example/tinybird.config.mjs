/** @type {import("@tinybirdco/sdk").TinybirdConfig} */
const tinybirdConfig = {
  include: [
    "src/tinybird/datasources.ts",
    "src/tinybird/pipes.ts",
    "src/tinybird/endpoints.ts",
    "src/tinybird/mvs.ts",
    "src/tinybird/copies.ts",
    "src/tinybird/dummy.datasource",
  ],
  token: process.env.TINYBIRD_TOKEN,
  baseUrl: "https://api.europe-west2.gcp.tinybird.co",
  devMode: "branch",
};

export default tinybirdConfig;
