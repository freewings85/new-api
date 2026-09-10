package bodylog

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRawOrString(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want string
	}{
		{"valid object", []byte(`{"a":1}`), `{"a":1}`},
		{"valid array", []byte(`[1,2]`), `[1,2]`},
		{"invalid json becomes string", []byte(`not json`), `"not json"`},
		{"empty becomes null", nil, `null`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, string(RawOrString(tt.in)))
		})
	}
}

func TestRelativeFilePath(t *testing.T) {
	ts := time.Date(2026, 9, 9, 17, 4, 50, 0, time.FixedZone("CST", 8*3600))
	assert.Equal(t, "42/2026-09-09.jsonl", RelativeFilePath(42, ts))
}

func TestRecordLine(t *testing.T) {
	ts := time.Date(2026, 9, 9, 17, 4, 50, 0, time.FixedZone("CST", 8*3600))
	rec := Record{
		Time: ts, RequestID: "req-1", UserID: 42, Username: "u", TokenName: "tk",
		Model: "m-out", UpstreamModel: "m-up", ChannelID: 7, Stream: true, Status: "ok",
		PromptTokens: 3, CompletionTokens: 5, UseTimeMs: 120,
		Input: RawOrString([]byte(`{"messages":[]}`)), Output: RawOrString([]byte(`{"text":"hi"}`)),
	}
	line, err := rec.Line()
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(string(line), "\n"))
	require.Equal(t, 1, strings.Count(string(line), "\n"), "exactly one line")

	var back map[string]any
	require.NoError(t, json.Unmarshal(line, &back))
	assert.Equal(t, "req-1", back["request_id"])
	assert.Equal(t, "2026-09-09T17:04:50+08:00", back["time"])
	assert.Equal(t, map[string]any{"text": "hi"}, back["output"])
	assert.Equal(t, map[string]any{"messages": []any{}}, back["input"])
	_, hasErr := back["error"]
	assert.False(t, hasErr, "error omitted when empty")

	keys := strings.Index(string(line), `"time"`) < strings.Index(string(line), `"request_id"`) &&
		strings.Index(string(line), `"request_id"`) < strings.Index(string(line), `"input"`) &&
		strings.Index(string(line), `"input"`) < strings.Index(string(line), `"output"`)
	assert.True(t, keys, "field order fixed: time, request_id ... input, output")
}
