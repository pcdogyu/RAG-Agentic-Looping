import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import {
  analysisPendingText,
  createInitialHealthTracking,
  formatCountdown,
  normalizeTheme,
  scanButtonText,
  updateHealthTracking,
} from "./App";
import BuildFooter, { buildInfo } from "./BuildFooter";
import ModelLogsPage, {
  buildModelLogQuery,
  fidelityLabel,
  isModelLogsHash,
} from "./ModelLogs";

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
  it("starts in checking and turns red on the third unreachable poll", () => {
    let tracking = createInitialHealthTracking();
    expect(tracking.ollama.state).toBe("checking");

    tracking = updateHealthTracking(tracking, null);
    expect(tracking.ollama).toEqual({ failures: 1, state: "checking" });
    expect(tracking.models["qwen2.5:3b"].state).toBe("checking");

    tracking = updateHealthTracking(tracking, { ollama: false, models: [] });
    expect(tracking.ollama).toEqual({ failures: 2, state: "checking" });

    tracking = updateHealthTracking(tracking, null);
    expect(tracking.ollama).toEqual({ failures: 3, state: "offline" });
    expect(tracking.models["qwen2.5:3b"].state).toBe("offline");
  });

  it("recovers every available connection immediately after three failures", () => {
    let tracking = createInitialHealthTracking();
    for (let attempt = 0; attempt < 3; attempt += 1) {
      tracking = updateHealthTracking(tracking, null);
    }

    tracking = updateHealthTracking(tracking, {
      ollama: true,
      models: ["qwen2.5:3b", "qwen2.5:7b", "qwen2.5:14b", "qwen2.5-coder:7b"],
    });

    expect(tracking.ollama).toEqual({ failures: 0, state: "available" });
    expect(tracking.models["qwen2.5:7b"]).toEqual({ failures: 0, state: "available" });
    expect(tracking.models["qwen2.5:14b"]).toEqual({ failures: 0, state: "available" });
  });

  it("tracks missing models independently from Ollama and installed models", () => {
    const health = {
      ollama: true,
      models: ["qwen2.5:3b", "qwen2.5:7b"],
    };
    let tracking = createInitialHealthTracking();
    for (let attempt = 0; attempt < 3; attempt += 1) {
      tracking = updateHealthTracking(tracking, health);
    }

    expect(tracking.ollama.state).toBe("available");
    expect(tracking.models["qwen2.5:3b"].state).toBe("available");
    expect(tracking.models["qwen2.5:14b"]).toEqual({ failures: 3, state: "missing" });
    expect(tracking.models["qwen2.5-coder:7b"]).toEqual({ failures: 3, state: "missing" });
  });

  it("matches Ollama model names case-insensitively", () => {
    const tracking = updateHealthTracking(
      createInitialHealthTracking(),
      { ollama: true, models: ["QWEN2.5:14B"] },
    );
    expect(tracking.models["qwen2.5:14b"].state).toBe("available");
  });
});

describe("theme selection", () => {
  it("defaults missing or invalid stored values to dark", () => {
    expect(normalizeTheme(null)).toBe("dark");
    expect(normalizeTheme("system")).toBe("dark");
  });

  it("restores either persisted theme", () => {
    expect(normalizeTheme("dark")).toBe("dark");
    expect(normalizeTheme("light")).toBe("light");
  });
});

describe("build footer", () => {
  it("renders the author and Git build coordinates", () => {
    const markup = renderToStaticMarkup(createElement(BuildFooter));
    expect(markup).toContain("Code by");
    expect(markup).toContain("mailto:Yuhao@jiansutech.com");
    expect(markup).toContain(buildInfo.commitTime);
    expect(markup).toContain(buildInfo.branch);
    expect(markup).toContain(buildInfo.commitId.slice(0, 12));
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

describe("model log navigation and filters", () => {
  it("recognizes only the dedicated model log hash", () => {
    expect(isModelLogsHash("#/model-logs")).toBe(true);
    expect(isModelLogsHash("#model-logs")).toBe(false);
    expect(isModelLogsHash("")).toBe(false);
  });

  it("builds stable API filters with an ISO time boundary", () => {
    const query = buildModelLogQuery({
      range: "7d",
      model: "qwen2.5:7b",
      provider: "ollama",
      operation: "report_drafting",
      status: "completed",
      language: "zh",
      fidelity: "exact",
    }, Date.parse("2026-08-23T00:00:00Z"));
    expect(query.get("start")).toBe("2026-08-16T00:00:00.000Z");
    expect(query.get("model")).toBe("qwen2.5:7b");
    expect(query.get("fidelity")).toBe("exact");
  });

  it("labels reconstructed history and renders the full-screen shell", () => {
    expect(fidelityLabel("reconstructed")).toBe("历史重建");
    const markup = renderToStaticMarkup(createElement(ModelLogsPage, {
      apiBase: "http://localhost:8000",
      onBack: () => undefined,
    }));
    expect(markup).toContain("模型日志");
    expect(markup).toContain("返回主看板");
    expect(markup).toContain("模型日志筛选");
    expect(markup).toContain("正在读取模型日志");
  });
});
