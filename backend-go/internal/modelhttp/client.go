// Package modelhttp adapts the internal Ollama wire format to OpenAI-compatible
// gateways when a configured model base URL ends in /v1.
package modelhttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Do preserves native Ollama requests and converts /v1 model requests only.
func Do(client *http.Client, request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	chat := strings.HasSuffix(path, "/v1/api/chat")
	models := strings.HasSuffix(path, "/v1/api/tags") || strings.HasSuffix(path, "/v1/api/ps")
	if !chat && !models {
		return client.Do(request)
	}
	converted := request.Clone(request.Context())
	if chat {
		var input map[string]any
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			return nil, fmt.Errorf("decode model request: %w", err)
		}
		_ = request.Body.Close()
		output := map[string]any{"model": input["model"], "messages": input["messages"], "stream": false}
		if options, ok := input["options"].(map[string]any); ok {
			output["temperature"] = options["temperature"]
			output["max_tokens"] = options["num_predict"]
		}
		if schema, ok := input["format"].(map[string]any); ok {
			output["response_format"] = map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "result", "schema": schema}}
		} else if input["format"] == "json" {
			output["response_format"] = map[string]any{"type": "json_object"}
		}
		think, _ := input["think"].(bool)
		output["chat_template_kwargs"] = map[string]any{"enable_thinking": think}
		body, err := json.Marshal(output)
		if err != nil {
			return nil, err
		}
		converted.URL.Path = strings.TrimSuffix(path, "/api/chat") + "/chat/completions"
		converted.Body = io.NopCloser(bytes.NewReader(body))
		converted.ContentLength = int64(len(body))
		converted.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	} else {
		converted.URL.Path = path[:strings.LastIndex(path, "/api/")] + "/models"
	}
	response, err := client.Do(converted)
	if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		return response, err
	}
	defer response.Body.Close()
	var output any
	if chat {
		var payload struct {
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage struct {
				Prompt     int `json:"prompt_tokens"`
				Completion int `json:"completion_tokens"`
			} `json:"usage"`
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&payload); err != nil {
			return nil, err
		}
		if len(payload.Choices) == 0 {
			return nil, fmt.Errorf("model gateway returned no choices")
		}
		output = map[string]any{"message": map[string]any{"content": payload.Choices[0].Message.Content}, "done": true, "done_reason": payload.Choices[0].FinishReason, "prompt_eval_count": payload.Usage.Prompt, "eval_count": payload.Usage.Completion}
	} else {
		var payload struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
			return nil, err
		}
		items := make([]map[string]string, 0, len(payload.Data))
		for _, model := range payload.Data {
			items = append(items, map[string]string{"name": model.ID})
		}
		output = map[string]any{"models": items}
	}
	body, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	response.Header.Del("Content-Length")
	return response, nil
}
