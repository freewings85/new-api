package service

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/bodylog"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newBodyLogContext(t *testing.T, body string) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(body))
	c.Set(common.KeyRequestBody, []byte(body))
	c.Set("username", "alice")
	c.Set("token_name", "tk")
	return c
}

func TestBuildRequestBodyRecordNonStream(t *testing.T) {
	c := newBodyLogContext(t, `{"model":"m-out","messages":[{"role":"user","content":"hi"}]}`)
	start := time.Date(2026, 9, 9, 17, 0, 0, 0, time.UTC)
	info := &relaycommon.RelayInfo{
		RequestId: "req-1", UserId: 42, TokenId: 9,
		OriginModelName: "m-out", IsStream: false, StartTime: start,
		ResponseBody: []byte(`{"choices":[{"message":{"content":"hello"}}]}`),
		ChannelMeta:  &relaycommon.ChannelMeta{ChannelId: 3, UpstreamModelName: "m-up"},
	}
	rec := buildRequestBodyRecord(c, info, RequestBodyLogParams{Status: bodylog.StatusOK, PromptTokens: 5, CompletionTokens: 2}, start.Add(1500*time.Millisecond))

	assert.Equal(t, "req-1", rec.RequestID)
	assert.Equal(t, 42, rec.UserID)
	assert.Equal(t, "alice", rec.Username)
	assert.Equal(t, "tk", rec.TokenName)
	assert.Equal(t, "m-out", rec.Model)
	assert.Equal(t, "m-up", rec.UpstreamModel)
	assert.Equal(t, 3, rec.ChannelID)
	assert.False(t, rec.Stream)
	assert.Equal(t, "ok", rec.Status)
	assert.Equal(t, 5, rec.PromptTokens)
	assert.Equal(t, 2, rec.CompletionTokens)
	assert.Equal(t, int64(1500), rec.UseTimeMs)
	assert.JSONEq(t, `{"model":"m-out","messages":[{"role":"user","content":"hi"}]}`, string(rec.Input))
	assert.JSONEq(t, `{"choices":[{"message":{"content":"hello"}}]}`, string(rec.Output))
	assert.False(t, rec.OutputMissing)
}

func TestBuildRequestBodyRecordStreamUsesText(t *testing.T) {
	c := newBodyLogContext(t, `{"model":"m","stream":true,"messages":[]}`)
	info := &relaycommon.RelayInfo{RequestId: "req-2", UserId: 1, IsStream: true, StartTime: time.Now(), ResponseText: "streamed answer"}
	rec := buildRequestBodyRecord(c, info, RequestBodyLogParams{Status: bodylog.StatusOK}, time.Now())
	assert.True(t, rec.Stream)
	assert.JSONEq(t, `{"text":"streamed answer"}`, string(rec.Output))
	assert.False(t, rec.OutputMissing)
}

func TestBuildRequestBodyRecordMissingOutput(t *testing.T) {
	c := newBodyLogContext(t, `{"model":"m"}`)
	info := &relaycommon.RelayInfo{RequestId: "req-3", UserId: 1, StartTime: time.Now()}
	rec := buildRequestBodyRecord(c, info, RequestBodyLogParams{Status: bodylog.StatusOK}, time.Now())
	assert.Equal(t, "null", string(rec.Output))
	assert.True(t, rec.OutputMissing)
}

func TestBuildRequestBodyRecordError(t *testing.T) {
	c := newBodyLogContext(t, `not json at all`)
	info := &relaycommon.RelayInfo{RequestId: "req-4", UserId: 1, StartTime: time.Now(), ResponseText: "partial"}
	rec := buildRequestBodyRecord(c, info, RequestBodyLogParams{Status: bodylog.StatusError, Error: "upstream 500"}, time.Now())
	assert.Equal(t, "error", rec.Status)
	assert.Equal(t, "upstream 500", rec.Error)
	assert.Equal(t, `"not json at all"`, string(rec.Input))
	assert.Equal(t, "null", string(rec.Output), "error records never carry output")
}

// TestBuildRequestBodyRecordContentTypeGate covers the capture gate: JSON bodies
// are embedded verbatim (bodies are never truncated), everything else is named
// rather than captured, so a multipart upload cannot land in the log as a
// multi-megabyte JSON string.
func TestBuildRequestBodyRecordContentTypeGate(t *testing.T) {
	const body = `{"model":"m"}`
	cases := []struct {
		name        string
		contentType string
		wantInput   string
		wantSkipped string
	}{
		{"absent content type is treated as json", "", body, ""},
		{"json", "application/json", body, ""},
		{"json with charset", "application/json; charset=utf-8", body, ""},
		{"uppercase json", "Application/JSON", body, ""},
		{"json with a malformed parameter list", "application/json; charset", body, ""},
		{"multipart upload", "multipart/form-data; boundary=xyz", "null", "multipart/form-data"},
		{"form encoded", "application/x-www-form-urlencoded", "null", "application/x-www-form-urlencoded"},
		{"audio upload", "audio/mpeg", "null", "audio/mpeg"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newBodyLogContext(t, body)
			if tc.contentType != "" {
				c.Request.Header.Set("Content-Type", tc.contentType)
			}
			c.Request.ContentLength = 4096
			info := &relaycommon.RelayInfo{RequestId: "req", UserId: 1, StartTime: time.Now()}
			rec := buildRequestBodyRecord(c, info, RequestBodyLogParams{Status: bodylog.StatusOK}, time.Now())

			assert.Equal(t, tc.wantInput, string(rec.Input))
			assert.Equal(t, tc.wantSkipped, rec.InputSkipped)
			assert.False(t, rec.InputMissing)
			if tc.wantSkipped == "" {
				assert.Zero(t, rec.InputSize)
			} else {
				assert.Equal(t, int64(4096), rec.InputSize, "a skipped body still reports its size")
			}
		})
	}
}

func TestBuildRequestBodyRecordSkipsDiskCachedBody(t *testing.T) {
	previous := common.GetDiskCacheConfig()
	common.SetDiskCacheConfig(common.DiskCacheConfig{Enabled: true, ThresholdMB: 0, MaxSizeMB: 1024, Path: t.TempDir()})
	t.Cleanup(func() { common.SetDiskCacheConfig(previous) })

	payload := []byte(`{"model":"m","messages":[]}`)
	storage, err := common.CreateBodyStorage(payload)
	require.NoError(t, err)
	t.Cleanup(func() { _ = storage.Close() })
	require.True(t, storage.IsDisk(), "test needs a disk-backed body")

	c := newBodyLogContext(t, string(payload))
	c.Set(common.KeyBodyStorage, storage)
	info := &relaycommon.RelayInfo{RequestId: "req-disk", UserId: 1, StartTime: time.Now()}
	rec := buildRequestBodyRecord(c, info, RequestBodyLogParams{Status: bodylog.StatusOK}, time.Now())

	assert.Equal(t, "null", string(rec.Input), "a body the host kept off the heap is not read back in to log it")
	assert.Equal(t, "disk-cached body", rec.InputSkipped)
	assert.Equal(t, int64(len(payload)), rec.InputSize)
	assert.False(t, rec.InputMissing)
}

type failingBody struct{}

func (failingBody) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (failingBody) Close() error             { return nil }

func TestBuildRequestBodyRecordMarksUnreadableInput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Request.ContentLength = 0
	c.Request.Body = failingBody{}

	info := &relaycommon.RelayInfo{RequestId: "req-5", UserId: 1, StartTime: time.Now()}
	rec := buildRequestBodyRecord(c, info, RequestBodyLogParams{Status: bodylog.StatusOK}, time.Now())

	assert.True(t, rec.InputMissing, "a body that should have been captured but could not be read is marked, not silently null")
	assert.Equal(t, "null", string(rec.Input))
	assert.Empty(t, rec.InputSkipped)
}

func TestRequestBodyLogFile(t *testing.T) {
	ts := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	assert.Equal(t, "42/2026-09-09.jsonl", RequestBodyLogFile(42, ts))
}

func TestRecordRequestBodyLogNoopWhenDisabled(t *testing.T) {
	require.False(t, RequestBodyLogEnabled())
	c := newBodyLogContext(t, `{}`)
	RecordRequestBodyLog(c, &relaycommon.RelayInfo{UserId: 1, StartTime: time.Now()}, RequestBodyLogParams{Status: bodylog.StatusOK}) // must not panic
}
