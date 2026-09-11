import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import ModelPromptsPage from "./ModelPrompts";

describe("model prompts page", () => {
  it("explains live prompt editing and starts with a server-backed loading state", () => {
    const markup = renderToStaticMarkup(createElement(ModelPromptsPage, { apiBase: "" }));
    expect(markup).toContain("模型提示词");
    expect(markup).toContain("保存后由 Worker 从数据库即时读取，无需重启");
    expect(markup).toContain("正在读取模型提示词");
  });
});
