package jobs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const celeryPrioritySeparator = "\x06\x16"

type celeryMessage struct {
	Body            string         `json:"body"`
	ContentEncoding string         `json:"content-encoding"`
	ContentType     string         `json:"content-type"`
	Headers         map[string]any `json:"headers"`
	Properties      map[string]any `json:"properties"`
}

func publishCelery(ctx context.Context, client *redis.Client, task, queue, taskID string, args []any, kwargs map[string]any, priority int) error {
	if taskID == "" {
		taskID = uuid.NewString()
	}
	if args == nil {
		args = []any{}
	}
	if kwargs == nil {
		kwargs = map[string]any{}
	}
	body, err := json.Marshal([]any{args, kwargs, map[string]any{"callbacks": nil, "errbacks": nil, "chain": nil, "chord": nil}})
	if err != nil {
		return err
	}
	message := celeryMessage{
		Body: base64.StdEncoding.EncodeToString(body), ContentEncoding: "utf-8", ContentType: "application/json",
		Headers: map[string]any{
			"lang": "py", "task": task, "id": taskID, "shadow": nil, "eta": nil, "expires": nil,
			"group": nil, "group_index": nil, "retries": 0, "timelimit": []any{nil, nil},
			"root_id": taskID, "parent_id": nil, "argsrepr": fmt.Sprint(args), "kwargsrepr": fmt.Sprint(kwargs),
			"origin": "go-worker", "ignore_result": false, "replaced_task_nesting": 0,
			"stamped_headers": nil, "stamps": map[string]any{},
		},
		Properties: map[string]any{
			"correlation_id": taskID, "reply_to": uuid.NewString(), "delivery_mode": 2,
			"delivery_info": map[string]any{"exchange": "", "routing_key": queue},
			"priority":      priority, "body_encoding": "base64", "delivery_tag": uuid.NewString(),
		},
	}
	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return client.LPush(ctx, celeryQueueKey(queue, priority), encoded).Err()
}

func celeryQueueKey(queue string, priority int) string {
	step := 0
	for _, candidate := range []int{3, 6, 9} {
		if priority >= candidate {
			step = candidate
		}
	}
	if step == 0 {
		return queue
	}
	return queue + celeryPrioritySeparator + strconv.Itoa(step)
}
