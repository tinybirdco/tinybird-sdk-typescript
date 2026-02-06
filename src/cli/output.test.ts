/**
 * Tests for CLI output utilities
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  formatDuration,
  showResourceChange,
  showChangesTable,
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
} from "./output.js";

describe("output utilities", () => {
  let consoleLogSpy: ReturnType<typeof vi.spyOn>;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    consoleLogSpy.mockRestore();
    consoleErrorSpy.mockRestore();
  });

  describe("formatDuration", () => {
    it("formats milliseconds for durations under 1 second", () => {
      expect(formatDuration(500)).toBe("500ms");
      expect(formatDuration(0)).toBe("0ms");
      expect(formatDuration(999)).toBe("999ms");
    });

    it("formats seconds for durations 1 second or more", () => {
      expect(formatDuration(1000)).toBe("1.0s");
      expect(formatDuration(1500)).toBe("1.5s");
      expect(formatDuration(2345)).toBe("2.3s");
      expect(formatDuration(10000)).toBe("10.0s");
    });
  });

  describe("showResourceChange", () => {
    it("shows created resource", () => {
      showResourceChange("events.datasource", "created");
      expect(consoleLogSpy).toHaveBeenCalledWith("✓ events.datasource created");
    });

    it("shows changed resource", () => {
      showResourceChange("top_pages.pipe", "changed");
      expect(consoleLogSpy).toHaveBeenCalledWith("✓ top_pages.pipe changed");
    });

    it("shows deleted resource", () => {
      showResourceChange("old_data.datasource", "deleted");
      expect(consoleLogSpy).toHaveBeenCalledWith("✓ old_data.datasource deleted");
    });
  });

  describe("showChangesTable", () => {
    it("shows no changes message when empty", () => {
      showChangesTable([]);
      expect(consoleLogSpy).toHaveBeenCalled();
      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain("No changes to be deployed");
    });

    it("shows table with changes", () => {
      showChangesTable([
        { status: "new", name: "events", type: "datasource" },
        { status: "modified", name: "top_pages", type: "pipe" },
        { status: "deleted", name: "old_data", type: "datasource" },
      ]);

      // Check that table header and data were logged
      const allCalls = consoleLogSpy.mock.calls.map((c: unknown[]) => c[0]).join("\n");
      expect(allCalls).toContain("Changes to be deployed");
      expect(allCalls).toContain("status");
      expect(allCalls).toContain("name");
      expect(allCalls).toContain("type");
      expect(allCalls).toContain("new");
      expect(allCalls).toContain("events");
      expect(allCalls).toContain("datasource");
      expect(allCalls).toContain("modified");
      expect(allCalls).toContain("top_pages");
      expect(allCalls).toContain("pipe");
      expect(allCalls).toContain("deleted");
      expect(allCalls).toContain("old_data");
    });

    it("shows table borders", () => {
      showChangesTable([{ status: "new", name: "test", type: "pipe" }]);

      const allCalls = consoleLogSpy.mock.calls.map((c: unknown[]) => c[0]).join("\n");
      expect(allCalls).toContain("┌");
      expect(allCalls).toContain("┐");
      expect(allCalls).toContain("├");
      expect(allCalls).toContain("┤");
      expect(allCalls).toContain("└");
      expect(allCalls).toContain("┘");
    });
  });

  describe("showBuildErrors", () => {
    it("shows errors with filename", () => {
      showBuildErrors([
        { filename: "events.datasource", error: "Invalid column type" },
      ]);
      expect(consoleErrorSpy).toHaveBeenCalledWith("events.datasource");
      expect(consoleErrorSpy).toHaveBeenCalledWith("  Invalid column type");
    });

    it("shows errors without filename", () => {
      showBuildErrors([{ error: "General build error" }]);
      expect(consoleErrorSpy).toHaveBeenCalledWith("General build error");
    });

    it("shows multiple errors", () => {
      showBuildErrors([
        { filename: "a.datasource", error: "Error A" },
        { filename: "b.pipe", error: "Error B" },
      ]);
      expect(consoleErrorSpy).toHaveBeenCalledWith("a.datasource");
      expect(consoleErrorSpy).toHaveBeenCalledWith("  Error A");
      expect(consoleErrorSpy).toHaveBeenCalledWith("b.pipe");
      expect(consoleErrorSpy).toHaveBeenCalledWith("  Error B");
    });

    it("handles multi-line errors", () => {
      showBuildErrors([
        { filename: "test.pipe", error: "Line 1\nLine 2\nLine 3" },
      ]);
      expect(consoleErrorSpy).toHaveBeenCalledWith("test.pipe");
      expect(consoleErrorSpy).toHaveBeenCalledWith("  Line 1");
      expect(consoleErrorSpy).toHaveBeenCalledWith("  Line 2");
      expect(consoleErrorSpy).toHaveBeenCalledWith("  Line 3");
    });
  });

  describe("showBuildSuccess", () => {
    it("shows build success with duration in ms", () => {
      showBuildSuccess(500);
      expect(consoleLogSpy).toHaveBeenCalled();
      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain("✓");
      expect(call).toContain("Build completed in 500ms");
    });

    it("shows build success with duration in seconds", () => {
      showBuildSuccess(2500);
      expect(consoleLogSpy).toHaveBeenCalled();
      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain("Build completed in 2.5s");
    });

    it("shows rebuild success when isRebuild is true", () => {
      showBuildSuccess(1000, true);
      expect(consoleLogSpy).toHaveBeenCalled();
      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain("Rebuild completed in 1.0s");
    });
  });

  describe("showBuildFailure", () => {
    it("shows build failure", () => {
      showBuildFailure();
      expect(consoleErrorSpy).toHaveBeenCalled();
      const call = consoleErrorSpy.mock.calls[0][0];
      expect(call).toContain("✗");
      expect(call).toContain("Build failed");
    });

    it("shows rebuild failure when isRebuild is true", () => {
      showBuildFailure(true);
      expect(consoleErrorSpy).toHaveBeenCalled();
      const call = consoleErrorSpy.mock.calls[0][0];
      expect(call).toContain("Rebuild failed");
    });
  });

  describe("showNoChanges", () => {
    it("shows no changes message", () => {
      showNoChanges();
      expect(consoleLogSpy).toHaveBeenCalled();
      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain("△");
      expect(call).toContain("Not deploying. No changes.");
    });
  });

  describe("showWaitingForDeployment", () => {
    it("shows waiting for deployment message", () => {
      showWaitingForDeployment();
      expect(consoleLogSpy).toHaveBeenCalledWith("» Waiting for deployment to be ready...");
    });
  });

  describe("showDeploymentReady", () => {
    it("shows deployment ready message", () => {
      showDeploymentReady();
      expect(consoleLogSpy).toHaveBeenCalled();
      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain("✓");
      expect(call).toContain("Deployment is ready");
    });
  });

  describe("showDeploymentLive", () => {
    it("shows deployment live message with ID", () => {
      showDeploymentLive("abc123");
      expect(consoleLogSpy).toHaveBeenCalled();
      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain("✓");
      expect(call).toContain("Deployment #abc123 is live!");
    });
  });

  describe("showValidatingDeployment", () => {
    it("shows validating deployment message", () => {
      showValidatingDeployment();
      expect(consoleLogSpy).toHaveBeenCalledWith("» Validating deployment...");
    });
  });

  describe("showDeploySuccess", () => {
    it("shows deploy success with duration in ms", () => {
      showDeploySuccess(500);
      expect(consoleLogSpy).toHaveBeenCalled();
      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain("✓");
      expect(call).toContain("Deploy completed in 500ms");
    });

    it("shows deploy success with duration in seconds", () => {
      showDeploySuccess(2500);
      expect(consoleLogSpy).toHaveBeenCalled();
      const call = consoleLogSpy.mock.calls[0][0];
      expect(call).toContain("Deploy completed in 2.5s");
    });
  });

  describe("showDeployFailure", () => {
    it("shows deploy failure", () => {
      showDeployFailure();
      expect(consoleErrorSpy).toHaveBeenCalled();
      const call = consoleErrorSpy.mock.calls[0][0];
      expect(call).toContain("✗");
      expect(call).toContain("Deploy failed");
    });
  });
});
