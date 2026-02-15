import { describe, expect, it } from "vitest";
import * as sdk from "./index.js";

describe("root public exports", () => {
  it("does not expose legacy project-client aliases", () => {
    expect("createTinybirdClient" in sdk).toBe(false);
  });

  it("keeps documented project constructors", () => {
    expect(typeof sdk.defineProject).toBe("function");
    expect(typeof sdk.Tinybird).toBe("function");
  });
});
