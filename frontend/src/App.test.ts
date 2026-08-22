import { describe, expect, it } from "vitest";

import { analysisPendingText, formatCountdown, modelConnectionState, scanButtonText } from "./App";

const baseStatus = {
  state: "idle",
  task_id: null,
  phase: "completed",
  paused_from_phase: null,
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
    expect(scanButtonText({ ...baseStatus, state: "queued" }, Date.now())).toBe(
      "暂停扫描 · 排队中",
    );
    expect(scanButtonText({
      ...baseStatus, state: "running", phase: "extracting", current: 4, total: 12,
    }, Date.now())).toBe("暂停 · 事件归纳 4/12");
    expect(scanButtonText({ ...baseStatus, state: "retrying" }, Date.now())).toBe(
      "暂停扫描 · 正在重试",
    );
  });

  it("offers resume with preserved progress while paused", () => {
    expect(scanButtonText({
      ...baseStatus,
      state: "paused",
      phase: "paused",
      paused_from_phase: "extracting",
      current: 4,
      total: 12,
    }, Date.now())).toBe("继续扫描 · 已暂停 4/12");
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

describe("analysis mapping states", () => {
  it("explains active and failed 7B mapping without implying deep research started", () => {
    expect(analysisPendingText("mapping_queued")).toContain("正在识别并验证");
    expect(analysisPendingText("mapping_failed")).toContain("未生成或猜测证券代码");
  });

  it("keeps genuinely unmapped events explicit", () => {
    expect(analysisPendingText("unmapped")).toBe("该新闻尚未映射到可研究标的。");
  });
});
