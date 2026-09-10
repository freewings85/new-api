package service

import (
	"context"
	"fmt"
	"mime"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/bodylog"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/gin-gonic/gin"
)

var requestBodyLogWriter *bodylog.Writer

// InitRequestBodyLog reads REQUEST_LOG_* and starts the writer.
// REQUEST_LOG_DIR=off disables the feature; an unwritable directory disables it with an error log.
func InitRequestBodyLog() {
	dir := strings.TrimSpace(common.GetEnvOrDefaultString("REQUEST_LOG_DIR", "data/requests"))
	if dir == "" || strings.EqualFold(dir, "off") {
		common.SysLog("request body log disabled (REQUEST_LOG_DIR=off)")
		return
	}
	// A non-positive size, queue length or byte budget means "unset": keep the
	// default instead of failing the writer and disabling the whole feature.
	maxSizeMB := common.GetEnvOrDefault("REQUEST_LOG_MAX_SIZE_MB", 256)
	if maxSizeMB <= 0 {
		maxSizeMB = 256
	}
	queueSize := common.GetEnvOrDefault("REQUEST_LOG_QUEUE_SIZE", 10000)
	if queueSize <= 0 {
		queueSize = 10000
	}
	queueBytesMB := common.GetEnvOrDefault("REQUEST_LOG_QUEUE_BYTES_MB", 256)
	if queueBytesMB <= 0 {
		queueBytesMB = 256
	}
	cfg := bodylog.Config{
		Dir:           dir,
		MaxSizeBytes:  int64(maxSizeMB) << 20,
		RetentionDays: common.GetEnvOrDefault("REQUEST_LOG_RETENTION_DAYS", 30),
		QueueSize:     queueSize,
		MaxQueueBytes: int64(queueBytesMB) << 20,
	}
	w, err := bodylog.New(cfg, time.Now, func(format string, args ...any) {
		common.SysError(fmt.Sprintf(format, args...))
	})
	if err != nil {
		common.SysError("request body log disabled: " + err.Error())
		return
	}
	requestBodyLogWriter = w
	// The writer's own retention sweep only fires on a 24h ticker, so a process that
	// restarts more often than daily would never run cleanup; run it once at startup too.
	go w.Cleanup(time.Now())
	common.SysLog(fmt.Sprintf("request body log enabled: dir=%s max_size=%dMB retention=%dd queue=%d queue_bytes=%dMB",
		dir, cfg.MaxSizeBytes>>20, cfg.RetentionDays, cfg.QueueSize, cfg.MaxQueueBytes>>20))
}

// CloseRequestBodyLog drains the queue and waits for pending compressions,
// bounded by ctx (the same shutdown deadline the HTTP server got). Call it
// after the HTTP server has stopped. On a deadline it logs and returns; the
// remaining work is abandoned with the process.
func CloseRequestBodyLog(ctx context.Context) {
	if requestBodyLogWriter == nil {
		return
	}
	if err := requestBodyLogWriter.Shutdown(ctx); err != nil {
		common.SysError("request body log shutdown incomplete: " + err.Error())
	}
}

func RequestBodyLogEnabled() bool { return requestBodyLogWriter != nil }

// RequestBodyLogFile is the relative file a record written at t for userID lands in.
func RequestBodyLogFile(userID int, t time.Time) string {
	return bodylog.RelativeFilePath(userID, t)
}

type RequestBodyLogParams struct {
	Status           string
	Error            string
	PromptTokens     int
	CompletionTokens int
	// At is the record timestamp. Callers that already derived the file name
	// with RequestBodyLogFile pass the same instant here, so the recorded
	// body_file and the file the record lands in cannot disagree across
	// midnight. Zero means "now".
	At time.Time
}

// RecordRequestBodyLog enqueues the request/response pair for the current relay.
// It never blocks and never affects the response.
func RecordRequestBodyLog(c *gin.Context, relayInfo *relaycommon.RelayInfo, params RequestBodyLogParams) {
	if requestBodyLogWriter == nil || relayInfo == nil || relayInfo.UserId == 0 {
		return
	}
	at := params.At
	if at.IsZero() {
		at = time.Now()
	}
	requestBodyLogWriter.Enqueue(buildRequestBodyRecord(c, relayInfo, params, at))
}

// buildRequestBodyRecord assembles the record purely from its inputs so it can be tested
// without a running writer.
func buildRequestBodyRecord(c *gin.Context, relayInfo *relaycommon.RelayInfo, params RequestBodyLogParams, now time.Time) bodylog.Record {
	// Bodies are never truncated: a request body is either captured whole or
	// skipped with a reason. Only JSON is captured, so multipart uploads and
	// audio payloads are named rather than embedded as a giant JSON string.
	var (
		input        []byte
		inputSkipped string
		inputSize    int64
		inputMissing bool
	)
	mediaType := "application/json" // many clients post JSON with no Content-Type
	if c.Request != nil {
		if ct := strings.TrimSpace(c.Request.Header.Get("Content-Type")); ct != "" {
			parsed, _, err := mime.ParseMediaType(ct)
			if err != nil {
				// A malformed parameter list still names its media type.
				parsed, _, _ = strings.Cut(ct, ";")
			}
			mediaType = strings.ToLower(strings.TrimSpace(parsed))
		}
	}
	if mediaType != "application/json" {
		inputSkipped = mediaType
		if c.Request != nil && c.Request.ContentLength > 0 {
			inputSize = c.Request.ContentLength
		}
	} else if body, err := common.GetRequestBody(c); err != nil {
		inputMissing = true
		common.SysError("request body log: read request body: " + err.Error())
	} else if bs, ok := body.(common.BodyStorage); !ok {
		inputMissing = true
		common.SysError("request body log: request body storage has unexpected type")
	} else if bs.IsDisk() {
		// The host spilled this body to its disk cache precisely to keep it out
		// of RAM; Bytes() would read all of it back in just to log it.
		inputSkipped = "disk-cached body"
		inputSize = bs.Size()
	} else if b, err := bs.Bytes(); err != nil {
		inputMissing = true
		common.SysError("request body log: read request body: " + err.Error())
	} else {
		input = b
	}

	output := bodylog.RawOrString(nil)
	outputMissing := true
	switch {
	case params.Status != bodylog.StatusOK:
		// Error records never carry a response body; that's expected, not a missing output.
		outputMissing = false
	case relayInfo.IsStream && relayInfo.ResponseText != "":
		if b, err := common.Marshal(map[string]string{"text": relayInfo.ResponseText}); err == nil {
			output = bodylog.RawOrString(b)
			outputMissing = false
		}
	case !relayInfo.IsStream && len(relayInfo.ResponseBody) > 0:
		output = bodylog.RawOrString(relayInfo.ResponseBody)
		outputMissing = false
	}

	var useTimeMs int64
	if !relayInfo.StartTime.IsZero() {
		useTimeMs = now.Sub(relayInfo.StartTime).Milliseconds()
	}
	return bodylog.Record{
		Time:             now,
		RequestID:        relayInfo.RequestId,
		UserID:           relayInfo.UserId,
		Username:         c.GetString("username"),
		TokenName:        c.GetString("token_name"),
		Model:            relayInfo.OriginModelName,
		UpstreamModel:    relayInfo.GetUpstreamModelName(),
		ChannelID:        relayInfo.GetChannelID(),
		Stream:           relayInfo.IsStream,
		Status:           params.Status,
		Error:            params.Error,
		PromptTokens:     params.PromptTokens,
		CompletionTokens: params.CompletionTokens,
		UseTimeMs:        useTimeMs,
		Input:            bodylog.RawOrString(input),
		InputSkipped:     inputSkipped,
		InputSize:        inputSize,
		InputMissing:     inputMissing,
		Output:           output,
		OutputMissing:    outputMissing,
	}
}
