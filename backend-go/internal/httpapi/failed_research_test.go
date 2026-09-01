package httpapi

import "testing"

func TestFailedResearchErrorUsesActionableModelFailureMessages(t *testing.T) {
	tests := []struct {
		name         string
		payload      map[string]any
		auditedError string
		want         any
	}{
		{
			name:         "audit restores inference slot timeout hidden by generic code",
			payload:      map[string]any{"retryable_reason": "model_LlmError"},
			auditedError: "LlmError: timed out waiting for the research inference slot",
			want:         "研究模型排队超时：等待可用研究实例超过限制，可重新执行。",
		},
		{
			name:    "legacy read timeout",
			payload: map[string]any{"error": "研究依赖不可用：model_ReadTimeout", "retryable_reason": "model_ReadTimeout"},
			want:    "研究模型响应超时：本次生成超过时限，可重新执行。",
		},
		{
			name:    "connection details do not expose internal endpoint",
			payload: map[string]any{"error": `*url.Error: Post "http://host.docker.internal:11439/api/chat": dial tcp: connect: connection refused`},
			want:    "研究模型实例暂不可用：连接失败，可重新执行。",
		},
		{
			name:    "generic legacy llm error",
			payload: map[string]any{"error": "研究依赖不可用：model_LlmError", "retryable_reason": "model_LlmError"},
			want:    "研究模型调用未完成：排队超时或实例暂不可用，可重新执行。",
		},
		{
			name:    "non model failure remains available",
			payload: map[string]any{"error": "evidence source unavailable", "retryable_reason": "source_unavailable"},
			want:    "evidence source unavailable",
		},
		{
			name:    "missing failure detail",
			payload: map[string]any{},
			want:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := failedResearchError(test.payload, test.auditedError); got != test.want {
				t.Fatalf("failedResearchError()=%v want=%v", got, test.want)
			}
		})
	}
}
