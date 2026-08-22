import { describe, expect, it } from "vitest";

import { formatCountdown, modelConnectionState, scanButtonText } from "./App";

const baseStatus = {
  state: "idle",
  task_id: null,
  phase: "completed",
  current: 2,
  total: 2,
  started_at: "2026-08-22T00:00:00Z",
  last_completed_at: "2026-08-22T00:01:00Z",
  next_scan_at: "2026-08-22T00:11:00Z",
  last_result: null,
  last_error: null,
  interval_seconds: 600,
  server_time: "2026-08-22T00:01:00Z",
};

describe("scan status presentation", () => {
  it("formats a completion-anchored countdown", () => {
    expect(formatCountdown(600)).toBe("10分00秒");
    expect(scanButtonText(baseStatus, Date.parse("2026-08-22T00:01:01Z"))).toBe(
      "距离下一次扫描 09分59秒",
    );
  });

  it("keeps every active state labeled as scanning", () => {
    expect(scanButtonText({ ...baseStatus, state: "queued" }, Date.now())).toBe("扫描中");
    expect(scanButtonText({
      ...baseStatus, state: "running", phase: "extracting", current: 4, total: 12,
    }, Date.now())).toBe("扫描中 · 事件归纳 4/12");
    expect(scanButtonText({ ...baseStatus, state: "retrying" }, Date.now())).toBe(
      "扫描中 · 正在重试",
    );
  });
});

describe("Ollama model availability", () => {
  it("reports checking before health data arrives", () => {
    expect(modelConnectionState(null, "qwen2.5:3b")).toBe("checking");
  });

  it("reports every model offline when Ollama is unreachable", () => {
    expect(modelConnectionState({ ollama: false, models: [] }, "qwen2.5:7b")).toBe("offline");
  });

  it("distinguishes installed and missing models", () => {
    const health = {
      ollama: true,
      models: ["qwen2.5:3b", "qwen2.5:7b"],
    };

    expect(modelConnectionState(health, "qwen2.5:3b")).toBe("available");
    expect(modelConnectionState(health, "qwen2.5-coder:7b")).toBe("missing");
  });

  it("matches Ollama model names case-insensitively", () => {
    expect(modelConnectionState(
      { ollama: true, models: ["QWEN2.5-CODER:7B"] },
      "qwen2.5-coder:7b",
    )).toBe("available");
  });
});
