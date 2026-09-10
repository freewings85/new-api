// Package bodylog writes complete request/response bodies to per-user,
// per-day rolling JSONL files. It is independent from the host application.
//
// This package deliberately calls encoding/json directly: it is a
// self-contained encoder that must not import the host's common package, so
// AGENTS.md's "all JSON goes through common.Marshal" rule (which governs
// business code in the root module) does not apply here.
package bodylog

import (
	"encoding/json"
	"fmt"
	"time"
)

// Record is one request/response pair written as a single JSON line.
type Record struct {
	Time             time.Time       `json:"time"`
	RequestID        string          `json:"request_id"`
	UserID           int             `json:"user_id"`
	Username         string          `json:"username"`
	TokenName        string          `json:"token_name"`
	Model            string          `json:"model"`
	UpstreamModel    string          `json:"upstream_model"`
	ChannelID        int             `json:"channel_id"`
	Stream           bool            `json:"stream"`
	Status           string          `json:"status"`
	Error            string          `json:"error,omitempty"`
	PromptTokens     int             `json:"prompt_tokens"`
	CompletionTokens int             `json:"completion_tokens"`
	UseTimeMs        int64           `json:"use_time_ms"`
	Input            json.RawMessage `json:"input"`
	// InputSkipped names why the request body was not captured: the media type
	// for a non-JSON request, or "disk-cached body" for a body the host spilled
	// to disk. Bodies are never truncated; they are either captured whole or
	// skipped with a reason.
	InputSkipped string `json:"input_skipped,omitempty"`
	// InputSize is the byte size of a skipped body when it is known.
	InputSize int64 `json:"input_size,omitempty"`
	// InputMissing marks a body that should have been captured but could not be read.
	InputMissing  bool            `json:"input_missing,omitempty"`
	Output        json.RawMessage `json:"output"`
	OutputMissing bool            `json:"output_missing,omitempty"`
}

// Config controls where and how records are written.
type Config struct {
	Dir           string
	MaxSizeBytes  int64
	RetentionDays int
	QueueSize     int
	// MaxQueueBytes caps the input+output bytes held in the queue at once.
	// Zero selects defaultMaxQueueBytes.
	MaxQueueBytes int64
}

const (
	StatusOK    = "ok"
	StatusError = "error"
	dateLayout  = "2006-01-02"
)

// RawOrString embeds b verbatim when it is valid JSON, otherwise as a JSON string.
// Empty input becomes JSON null.
func RawOrString(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}
	if json.Valid(b) {
		return json.RawMessage(b)
	}
	quoted, err := json.Marshal(string(b))
	if err != nil {
		return json.RawMessage("null")
	}
	return json.RawMessage(quoted)
}

// RelativeFilePath returns "<userID>/<YYYY-MM-DD>.jsonl" relative to Config.Dir.
func RelativeFilePath(userID int, t time.Time) string {
	return fmt.Sprintf("%d/%s.jsonl", userID, t.Format(dateLayout))
}

// Line serializes the record as one JSON line terminated by '\n'.
func (r Record) Line() ([]byte, error) {
	if len(r.Input) == 0 {
		r.Input = json.RawMessage("null")
	}
	if len(r.Output) == 0 {
		r.Output = json.RawMessage("null")
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
