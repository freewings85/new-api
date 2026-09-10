# 请求正文日志 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把每次中转请求的完整输入输出按用户分目录、按天和大小切分写到本地 JSONL 文件，供按 request_id 查找。

**Architecture:** 新包 `pkg/bodylog` 提供一个异步、单消费者的滚动写入器（队列 → 每用户 appender → 按天/大小轮转 → gzip → 定期清理），与业务无关、可单测。`service/body_log.go` 把 gin 上下文和 `RelayInfo` 组装成记录并投递；`relay_info` 新增两个响应字段，由 OpenAI Chat 的流式/非流式 handler 填充；在文本计费结算和错误日志两处挂钩。

**Tech Stack:** Go 1.25+（标准库 `os`、`compress/gzip`、`encoding/json` 仅允许在 `pkg/bodylog` 内作为编码实现；根模块其它代码走 `common.Marshal`），gin，testify。

**Spec:** `specs/2026-09-09-request-body-log-design.md`

## Global Constraints

- Go 版本以 `go.mod` 为准（1.25.1）；使用 `any`、`for i := range n`、`strings.Cut` 等现代写法。
- 根模块业务代码 JSON 序列化必须用 `common.Marshal` / `common.Unmarshal`；`pkg/bodylog` 是独立编码实现，允许直接用 `encoding/json`，但不得 import 根模块 `common`。
- 新测试用 `github.com/stretchr/testify/require`（致命）和 `assert`（非致命）；表驱动、确定性、不用 sleep 做同步（用 `Flush()` 或 channel 等待）。
- 不新增只有一个调用方的包级 helper；单次使用的逻辑内联。
- 环境变量：`REQUEST_LOG_DIR`（默认 `data/requests`，`off` 关闭）、`REQUEST_LOG_MAX_SIZE_MB`（256）、`REQUEST_LOG_RETENTION_DAYS`（30）、`REQUEST_LOG_QUEUE_SIZE`（10000）。
- 写路径任何错误不得影响请求响应与计费；队列满丢弃并计数。
- 文件名不含节点名；每个节点写自己的本地目录。
- 每个任务结束提交一次，提交信息末尾带：
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_018eDAyuMBUXRW4w3Eqg394r
  ```
- 每次提交前运行 `gofmt -l` 涉及的目录并保证为空，`GOWORK=off go vet ./pkg/bodylog/ ./service/ ./relay/... ./controller/` 无错误。

---

## 文件结构

| 文件 | 职责 |
|---|---|
| `pkg/bodylog/record.go` | `Record` 结构体、`Config`、`FilePathFor()` 路径推导、行序列化 |
| `pkg/bodylog/appender.go` | 单个用户目录下的文件句柄：打开/续写、写一行、按天/大小轮转、触发压缩 |
| `pkg/bodylog/writer.go` | `Writer`：队列、消费 goroutine、appender 表、空闲回收、清理任务、`Close()` |
| `pkg/bodylog/*_test.go` | 单元测试 |
| `service/body_log.go` | 全局 `Writer` 初始化、`RecordRequestBodyLog()` 组装记录 |
| `service/body_log_test.go` | 组装逻辑测试 |
| `relay/common/relay_info.go` | 新增 `ResponseBody []byte`、`ResponseText string` |
| `relay/channel/openai/relay-openai.go` | 两处赋值 |
| `service/text_quota.go` | 成功挂钩 + `body_file` |
| `controller/relay.go` | 失败挂钩 |
| `main.go` | 初始化与关闭 |
| `.env.example`、`docker-compose.local.yml`、`deploy/aliyun/docker-compose.yml`、`deploy/aliyun/.env.example` | 配置与挂卷 |

---

### Task 1: 记录结构与路径推导

**Files:**
- Create: `pkg/bodylog/record.go`
- Test: `pkg/bodylog/record_test.go`

**Interfaces:**
- Produces:
  ```go
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
      Status           string          `json:"status"`            // "ok" | "error"
      Error            string          `json:"error,omitempty"`
      PromptTokens     int             `json:"prompt_tokens"`
      CompletionTokens int             `json:"completion_tokens"`
      UseTimeMs        int64           `json:"use_time_ms"`
      Input            json.RawMessage `json:"input"`
      Output           json.RawMessage `json:"output"`
      OutputMissing    bool            `json:"output_missing,omitempty"`
  }
  type Config struct {
      Dir           string
      MaxSizeBytes  int64
      RetentionDays int
      QueueSize     int
  }
  func RawOrString(b []byte) json.RawMessage   // 合法 JSON 原样返回，否则编码为 JSON 字符串；空输入返回 "null"
  func RelativeFilePath(userID int, t time.Time) string  // "<userID>/2006-01-02.jsonl"
  func (r Record) Line() ([]byte, error)      // 单行 JSON + '\n'
  ```

- [ ] **Step 1: 写失败的测试**

```go
// pkg/bodylog/record_test.go
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
```

- [ ] **Step 2: 运行确认失败**

Run: `cd /mnt/e/Documents/github/new-api && GOWORK=off go test ./pkg/bodylog/ -run 'TestRawOrString|TestRelativeFilePath|TestRecordLine' -v`
Expected: FAIL，`undefined: RawOrString` 等编译错误。

- [ ] **Step 3: 最小实现**

```go
// pkg/bodylog/record.go
// Package bodylog writes complete request/response bodies to per-user,
// per-day rolling JSONL files. It is independent from the host application.
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
	Output           json.RawMessage `json:"output"`
	OutputMissing    bool            `json:"output_missing,omitempty"`
}

// Config controls where and how records are written.
type Config struct {
	Dir           string
	MaxSizeBytes  int64
	RetentionDays int
	QueueSize     int
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
```

- [ ] **Step 4: 运行确认通过**

Run: `GOWORK=off go test ./pkg/bodylog/ -run 'TestRawOrString|TestRelativeFilePath|TestRecordLine' -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
gofmt -l pkg/bodylog && git add pkg/bodylog/record.go pkg/bodylog/record_test.go && git commit -m "feat(bodylog): 记录结构、路径推导与单行序列化

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018eDAyuMBUXRW4w3Eqg394r"
```

---

### Task 2: 单用户 appender：续写、按大小轮转、按天轮转、压缩

**Files:**
- Create: `pkg/bodylog/appender.go`
- Test: `pkg/bodylog/appender_test.go`

**Interfaces:**
- Consumes: `RelativeFilePath`, `dateLayout`（Task 1）
- Produces:
  ```go
  type appender struct { ... }
  func openAppender(dir string, userID int, now time.Time, maxSize int64, compress func(path string)) (*appender, error)
  func (a *appender) write(line []byte, now time.Time) error   // 先按需轮转（日期变化），写入，再按大小轮转
  func (a *appender) close() error
  func (a *appender) lastWrite() time.Time
  func gzipFile(path string) error   // path -> path+".gz"，成功后删除 path
  func nextRotatedName(dir string, base string) string  // "<base>.<n>.jsonl"，n = 已存在最大序号 + 1
  ```
  轮转规则：
  - 日期变化：关闭 `<date>.jsonl`，交给 `compress` 压成 `<date>.jsonl.gz`，打开新日期文件。
  - 大小超限：关闭，改名为 `<date>.<n>.jsonl`，交给 `compress`，重新打开 `<date>.jsonl`。
  - `compress` 由 `Writer` 注入（生产中异步，测试中同步），appender 不关心它何时完成。

- [ ] **Step 1: 写失败的测试**

```go
// pkg/bodylog/appender_test.go
package bodylog

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var day1 = time.Date(2026, 9, 9, 10, 0, 0, 0, time.UTC)
var day2 = day1.Add(24 * time.Hour)

func readGz(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	zr, err := gzip.NewReader(f)
	require.NoError(t, err)
	b, err := io.ReadAll(zr)
	require.NoError(t, err)
	return string(b)
}

func syncGzip(t *testing.T) func(string) {
	return func(p string) { require.NoError(t, gzipFile(p)) }
}

func TestAppenderAppendsAndResumes(t *testing.T) {
	dir := t.TempDir()
	a, err := openAppender(dir, 1, day1, 1<<20, syncGzip(t))
	require.NoError(t, err)
	require.NoError(t, a.write([]byte("l1\n"), day1))
	require.NoError(t, a.close())

	// reopen same day: must append, not truncate, and continue size accounting
	a, err = openAppender(dir, 1, day1, 1<<20, syncGzip(t))
	require.NoError(t, err)
	require.NoError(t, a.write([]byte("l2\n"), day1))
	require.NoError(t, a.close())

	got, err := os.ReadFile(filepath.Join(dir, "1", "2026-09-09.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, "l1\nl2\n", string(got))
}

func TestAppenderRotatesBySize(t *testing.T) {
	dir := t.TempDir()
	a, err := openAppender(dir, 1, day1, 8, syncGzip(t)) // 8 bytes cap
	require.NoError(t, err)
	require.NoError(t, a.write([]byte("aaaa\n"), day1)) // 5 bytes, below cap
	require.NoError(t, a.write([]byte("bbbb\n"), day1)) // 10 bytes total -> rotate after write
	require.NoError(t, a.write([]byte("cc\n"), day1))   // goes to fresh file
	require.NoError(t, a.close())

	assert.Equal(t, "aaaa\nbbbb\n", readGz(t, filepath.Join(dir, "1", "2026-09-09.1.jsonl.gz")))
	cur, err := os.ReadFile(filepath.Join(dir, "1", "2026-09-09.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, "cc\n", string(cur))
}

func TestAppenderRotatedNameSkipsExisting(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "1"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "1", "2026-09-09.1.jsonl.gz"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "1", "2026-09-09.3.jsonl"), []byte("x"), 0o644))
	assert.Equal(t, "2026-09-09.4.jsonl", nextRotatedName(filepath.Join(dir, "1"), "2026-09-09"))
}

func TestAppenderRotatesByDay(t *testing.T) {
	dir := t.TempDir()
	a, err := openAppender(dir, 1, day1, 1<<20, syncGzip(t))
	require.NoError(t, err)
	require.NoError(t, a.write([]byte("d1\n"), day1))
	require.NoError(t, a.write([]byte("d2\n"), day2))
	require.NoError(t, a.close())

	assert.Equal(t, "d1\n", readGz(t, filepath.Join(dir, "1", "2026-09-09.jsonl.gz")))
	_, err = os.Stat(filepath.Join(dir, "1", "2026-09-09.jsonl"))
	assert.True(t, os.IsNotExist(err), "uncompressed day-1 file removed after gzip")
	cur, err := os.ReadFile(filepath.Join(dir, "1", "2026-09-10.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, "d2\n", string(cur))
}

func TestGzipFileKeepsSourceOnFailure(t *testing.T) {
	err := gzipFile(filepath.Join(t.TempDir(), "missing.jsonl"))
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "missing.jsonl"))
}
```

- [ ] **Step 2: 运行确认失败**

Run: `GOWORK=off go test ./pkg/bodylog/ -run 'TestAppender|TestGzipFile' -v`
Expected: FAIL，`undefined: openAppender`。

- [ ] **Step 3: 最小实现**

```go
// pkg/bodylog/appender.go
package bodylog

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// appender owns the currently open JSONL file of one user.
// It is only ever used from the Writer's single consumer goroutine.
type appender struct {
	userDir  string
	date     string
	file     *os.File
	size     int64
	maxSize  int64
	last     time.Time
	compress func(path string)
}

func openAppender(dir string, userID int, now time.Time, maxSize int64, compress func(path string)) (*appender, error) {
	a := &appender{
		userDir:  filepath.Join(dir, strconv.Itoa(userID)),
		maxSize:  maxSize,
		compress: compress,
	}
	if err := a.open(now); err != nil {
		return nil, err
	}
	return a, nil
}

// open creates the user directory if needed and opens <date>.jsonl for append,
// resuming the size counter from the existing file.
func (a *appender) open(now time.Time) error {
	if err := os.MkdirAll(a.userDir, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("bodylog: mkdir %s: %w", a.userDir, err)
	}
	a.date = now.Format(dateLayout)
	path := filepath.Join(a.userDir, a.date+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("bodylog: open %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("bodylog: stat %s: %w", path, err)
	}
	a.file = f
	a.size = st.Size()
	a.last = now
	return nil
}

func (a *appender) currentPath() string {
	return filepath.Join(a.userDir, a.date+".jsonl")
}

// write appends one line. A day change rotates before writing; exceeding the
// size cap rotates after writing, so a line is never split across files.
func (a *appender) write(line []byte, now time.Time) error {
	if now.Format(dateLayout) != a.date {
		closed := a.currentPath()
		if err := a.file.Close(); err != nil {
			return err
		}
		a.compress(closed)
		if err := a.open(now); err != nil {
			return err
		}
	}
	n, err := a.file.Write(line)
	a.size += int64(n)
	a.last = now
	if err != nil {
		return fmt.Errorf("bodylog: write %s: %w", a.currentPath(), err)
	}
	if a.size >= a.maxSize {
		return a.rotateBySize(now)
	}
	return nil
}

func (a *appender) rotateBySize(now time.Time) error {
	cur := a.currentPath()
	if err := a.file.Close(); err != nil {
		return err
	}
	rotated := filepath.Join(a.userDir, nextRotatedName(a.userDir, a.date))
	if err := os.Rename(cur, rotated); err != nil {
		return fmt.Errorf("bodylog: rename %s: %w", cur, err)
	}
	a.compress(rotated)
	return a.open(now)
}

func (a *appender) close() error {
	if a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	return err
}

func (a *appender) lastWrite() time.Time { return a.last }

// nextRotatedName returns "<base>.<n>.jsonl" where n is one more than the
// highest existing index for base in dir (matching both .jsonl and .jsonl.gz).
func nextRotatedName(dir string, base string) string {
	entries, _ := os.ReadDir(dir)
	maxIdx := 0
	prefix := base + "."
	for _, e := range entries {
		name := strings.TrimSuffix(strings.TrimSuffix(e.Name(), ".gz"), ".jsonl")
		rest, ok := strings.CutPrefix(name, prefix)
		if !ok {
			continue
		}
		if n, err := strconv.Atoi(rest); err == nil && n > maxIdx {
			maxIdx = n
		}
	}
	return fmt.Sprintf("%s.%d.jsonl", base, maxIdx+1)
}

// gzipFile compresses path into path+".gz" and removes path on success.
// On failure the source file is left untouched.
func gzipFile(path string) error {
	src, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("bodylog: gzip open %s: %w", path, err)
	}
	defer src.Close()
	tmp := path + ".gz.tmp"
	dst, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("bodylog: gzip create %s: %w", tmp, err)
	}
	zw := gzip.NewWriter(dst)
	if _, err := io.Copy(zw, src); err != nil {
		zw.Close()
		dst.Close()
		os.Remove(tmp)
		return fmt.Errorf("bodylog: gzip copy %s: %w", path, err)
	}
	if err := zw.Close(); err != nil {
		dst.Close()
		os.Remove(tmp)
		return err
	}
	if err := dst.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path+".gz"); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Remove(path)
}
```

- [ ] **Step 4: 运行确认通过**

Run: `GOWORK=off go test ./pkg/bodylog/ -v`
Expected: PASS（Task 1 的测试也仍通过）

- [ ] **Step 5: 提交**

```bash
gofmt -l pkg/bodylog && git add pkg/bodylog/appender.go pkg/bodylog/appender_test.go && git commit -m "feat(bodylog): 单用户 appender，按天与大小轮转并 gzip

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018eDAyuMBUXRW4w3Eqg394r"
```

---

### Task 3: Writer：队列、单消费者、空闲回收、清理、Close

**Files:**
- Create: `pkg/bodylog/writer.go`
- Test: `pkg/bodylog/writer_test.go`

**Interfaces:**
- Consumes: `Record`, `Config`, `openAppender`, `gzipFile`（Task 1、2）
- Produces:
  ```go
  type Writer struct { ... }
  // New 创建目录、校验可写、启动消费 goroutine、空闲回收 ticker 和清理 ticker。
  // now 用于测试注入时间，生产传 time.Now。
  func New(cfg Config, now func() time.Time, logf func(format string, args ...any)) (*Writer, error)
  func (w *Writer) Enqueue(rec Record) bool       // 非阻塞；队列满返回 false 并计数
  func (w *Writer) Dropped() uint64
  func (w *Writer) Flush()                        // 阻塞直到当前队列中的记录全部写完（测试与关停用）
  func (w *Writer) Cleanup(now time.Time) int     // 删除超过 RetentionDays 的文件，返回删除数；由 ticker 每 24h 调用，也可手动调用
  func (w *Writer) Close() error                  // 关闭队列，写完剩余，关闭所有句柄，停止 ticker
  ```
  内部常量：空闲关闭 5 分钟，最大打开句柄 1024，空闲扫描每 1 分钟。

- [ ] **Step 1: 写失败的测试**

```go
// pkg/bodylog/writer_test.go
package bodylog

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestWriter(t *testing.T, queue int) (*Writer, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := New(Config{Dir: dir, MaxSizeBytes: 1 << 20, RetentionDays: 30, QueueSize: queue},
		func() time.Time { return day1 }, t.Logf)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w, dir
}

func mkRecord(userID int, id string) Record {
	return Record{Time: day1, RequestID: id, UserID: userID, Status: StatusOK,
		Input: RawOrString([]byte(`{"q":"` + id + `"}`)), Output: RawOrString([]byte(`{"a":1}`))}
}

func TestWriterConcurrentEnqueueProducesIntactLines(t *testing.T) {
	w, dir := newTestWriter(t, 10000)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				require.True(t, w.Enqueue(mkRecord(7, "g"+string(rune('a'+g))+"-"+time.Duration(i).String())))
			}
		}(g)
	}
	wg.Wait()
	w.Flush()

	f, err := os.Open(filepath.Join(dir, "7", "2026-09-09.jsonl"))
	require.NoError(t, err)
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	count := 0
	for sc.Scan() {
		var m map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &m), "line %d must be valid JSON", count)
		assert.Equal(t, float64(7), m["user_id"])
		count++
	}
	require.NoError(t, sc.Err())
	assert.Equal(t, 1600, count)
	assert.Equal(t, uint64(0), w.Dropped())
}

func TestWriterDropsWhenQueueFull(t *testing.T) {
	dir := t.TempDir()
	// Block the consumer by never starting it: use a queue of 2 and a writer whose
	// consumer is paused via a gate.
	w, err := newPaused(Config{Dir: dir, MaxSizeBytes: 1 << 20, RetentionDays: 30, QueueSize: 2},
		func() time.Time { return day1 }, t.Logf)
	require.NoError(t, err)
	defer w.Close()

	assert.True(t, w.Enqueue(mkRecord(1, "a")))
	assert.True(t, w.Enqueue(mkRecord(1, "b")))
	assert.False(t, w.Enqueue(mkRecord(1, "c")), "third must be dropped, not block")
	assert.Equal(t, uint64(1), w.Dropped())
	w.resume()
	w.Flush()
}

func TestWriterSeparatesUsers(t *testing.T) {
	w, dir := newTestWriter(t, 100)
	require.True(t, w.Enqueue(mkRecord(1, "x")))
	require.True(t, w.Enqueue(mkRecord(2, "y")))
	w.Flush()
	_, err := os.Stat(filepath.Join(dir, "1", "2026-09-09.jsonl"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "2", "2026-09-09.jsonl"))
	require.NoError(t, err)
}

func TestWriterIdleCloseThenResume(t *testing.T) {
	w, dir := newTestWriter(t, 100)
	require.True(t, w.Enqueue(mkRecord(1, "first")))
	w.Flush()
	w.closeIdle(day1.Add(10 * time.Minute)) // idle > 5min -> closed
	assert.Equal(t, 0, w.openAppenders())
	require.True(t, w.Enqueue(mkRecord(1, "second")))
	w.Flush()
	b, err := os.ReadFile(filepath.Join(dir, "1", "2026-09-09.jsonl"))
	require.NoError(t, err)
	assert.Contains(t, string(b), `"request_id":"first"`)
	assert.Contains(t, string(b), `"request_id":"second"`)
}

func TestWriterCleanupDeletesExpired(t *testing.T) {
	w, dir := newTestWriter(t, 100)
	old := filepath.Join(dir, "5")
	require.NoError(t, os.MkdirAll(old, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(old, "2026-07-01.jsonl.gz"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(old, "2026-09-01.2.jsonl.gz"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(old, "2026-09-09.jsonl"), []byte("x"), 0o644))

	deleted := w.Cleanup(day1) // retention 30d -> cutoff 2026-08-10
	assert.Equal(t, 1, deleted)
	_, err := os.Stat(filepath.Join(old, "2026-07-01.jsonl.gz"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(old, "2026-09-01.2.jsonl.gz"))
	assert.NoError(t, err)
}

func TestWriterCleanupRemovesEmptyUserDir(t *testing.T) {
	w, dir := newTestWriter(t, 100)
	old := filepath.Join(dir, "9")
	require.NoError(t, os.MkdirAll(old, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(old, "2026-01-01.jsonl.gz"), []byte("x"), 0o644))
	assert.Equal(t, 1, w.Cleanup(day1))
	_, err := os.Stat(old)
	assert.True(t, os.IsNotExist(err), "empty user dir removed")
}

func TestWriterCloseFlushesQueue(t *testing.T) {
	w, dir := newTestWriter(t, 1000)
	for i := range 50 {
		require.True(t, w.Enqueue(mkRecord(3, time.Duration(i).String())))
	}
	require.NoError(t, w.Close())
	b, err := os.ReadFile(filepath.Join(dir, "3", "2026-09-09.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, 50, countLines(b))
	assert.False(t, w.Enqueue(mkRecord(3, "late")), "enqueue after close is rejected")
}

func TestNewFailsOnUnwritableDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write anywhere")
	}
	base := t.TempDir()
	require.NoError(t, os.Chmod(base, 0o500))
	t.Cleanup(func() { _ = os.Chmod(base, 0o700) })
	_, err := New(Config{Dir: filepath.Join(base, "sub"), MaxSizeBytes: 1, RetentionDays: 1, QueueSize: 1},
		func() time.Time { return day1 }, t.Logf)
	require.Error(t, err)
}

func countLines(b []byte) int {
	n := 0
	for _, c := range b {
		if c == '\n' {
			n++
		}
	}
	return n
}
```

- [ ] **Step 2: 运行确认失败**

Run: `GOWORK=off go test ./pkg/bodylog/ -run TestWriter -v`
Expected: FAIL，`undefined: New`。

- [ ] **Step 3: 实现**

```go
// pkg/bodylog/writer.go
package bodylog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	idleClose    = 5 * time.Minute
	idleScan     = time.Minute
	cleanupEvery = 24 * time.Hour
	maxOpen      = 1024
)

// Writer receives records from any goroutine and writes them from one
// consumer goroutine, so every file has exactly one writer.
type Writer struct {
	cfg  Config
	now  func() time.Time
	logf func(string, ...any)

	queue     chan Record
	flushReq  chan chan struct{}
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	dropped   atomic.Uint64
	lastWarn  atomic.Int64 // unix seconds

	// consumer-goroutine-only state
	appenders map[int]*appender

	gate chan struct{} // nil in production; tests close it to release a paused consumer
}

// New validates and creates cfg.Dir, then starts the consumer and maintenance tickers.
func New(cfg Config, now func() time.Time, logf func(string, ...any)) (*Writer, error) {
	w, err := newPaused(cfg, now, logf)
	if err != nil {
		return nil, err
	}
	w.resume()
	return w, nil
}

// newPaused is New without releasing the consumer; tests use it to fill the queue deterministically.
func newPaused(cfg Config, now func() time.Time, logf func(string, ...any)) (*Writer, error) {
	if cfg.QueueSize <= 0 {
		return nil, errors.New("bodylog: QueueSize must be > 0")
	}
	if cfg.MaxSizeBytes <= 0 {
		return nil, errors.New("bodylog: MaxSizeBytes must be > 0")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("bodylog: create dir %s: %w", cfg.Dir, err)
	}
	probe, err := os.CreateTemp(cfg.Dir, ".probe-*")
	if err != nil {
		return nil, fmt.Errorf("bodylog: dir %s not writable: %w", cfg.Dir, err)
	}
	probe.Close()
	os.Remove(probe.Name())

	w := &Writer{
		cfg:       cfg,
		now:       now,
		logf:      logf,
		queue:     make(chan Record, cfg.QueueSize),
		flushReq:  make(chan chan struct{}),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		appenders: make(map[int]*appender),
		gate:      make(chan struct{}),
	}
	go w.run()
	return w, nil
}

func (w *Writer) resume() { close(w.gate) }

// Enqueue hands a record to the consumer without blocking.
// It returns false when the writer is closed or the queue is full.
func (w *Writer) Enqueue(rec Record) bool {
	if w.closed.Load() {
		return false
	}
	select {
	case w.queue <- rec:
		return true
	default:
		w.dropped.Add(1)
		nowSec := w.now().Unix()
		if last := w.lastWarn.Load(); nowSec-last >= 60 && w.lastWarn.CompareAndSwap(last, nowSec) {
			w.logf("bodylog: queue full, dropped %d records so far", w.dropped.Load())
		}
		return false
	}
}

func (w *Writer) Dropped() uint64 { return w.dropped.Load() }

// Flush blocks until every record enqueued before the call has been written.
func (w *Writer) Flush() {
	if w.closed.Load() {
		return
	}
	ack := make(chan struct{})
	select {
	case w.flushReq <- ack:
		<-ack
	case <-w.done:
	}
}

// Close stops intake, drains the queue, closes all files and stops tickers.
func (w *Writer) Close() error {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		close(w.stop)
		<-w.done
	})
	return nil
}

func (w *Writer) run() {
	defer close(w.done)
	select {
	case <-w.gate:
	case <-w.stop:
		return
	}
	idleTicker := time.NewTicker(idleScan)
	cleanTicker := time.NewTicker(cleanupEvery)
	defer idleTicker.Stop()
	defer cleanTicker.Stop()
	for {
		select {
		case rec := <-w.queue:
			w.handle(rec)
		case ack := <-w.flushReq:
			w.drain()
			close(ack)
		case <-idleTicker.C:
			w.closeIdle(w.now())
		case <-cleanTicker.C:
			w.Cleanup(w.now())
		case <-w.stop:
			w.drain()
			for id, a := range w.appenders {
				_ = a.close()
				delete(w.appenders, id)
			}
			return
		}
	}
}

func (w *Writer) drain() {
	for {
		select {
		case rec := <-w.queue:
			w.handle(rec)
		default:
			return
		}
	}
}

func (w *Writer) handle(rec Record) {
	line, err := rec.Line()
	if err != nil {
		w.logf("bodylog: serialize request %s: %v", rec.RequestID, err)
		return
	}
	a, ok := w.appenders[rec.UserID]
	if !ok {
		if len(w.appenders) >= maxOpen {
			w.closeOldest()
		}
		a, err = openAppender(w.cfg.Dir, rec.UserID, rec.Time, w.cfg.MaxSizeBytes, w.compressAsync)
		if err != nil {
			w.logf("bodylog: %v", err)
			return
		}
		w.appenders[rec.UserID] = a
	}
	if err := a.write(line, rec.Time); err != nil {
		w.logf("bodylog: %v", err)
		_ = a.close()
		delete(w.appenders, rec.UserID)
	}
}

func (w *Writer) compressAsync(path string) {
	go func() {
		if err := gzipFile(path); err != nil {
			w.logf("bodylog: %v", err)
		}
	}()
}

// closeIdle closes appenders not written for idleClose. Consumer goroutine only.
func (w *Writer) closeIdle(now time.Time) {
	for id, a := range w.appenders {
		if now.Sub(a.lastWrite()) >= idleClose {
			_ = a.close()
			delete(w.appenders, id)
		}
	}
}

func (w *Writer) closeOldest() {
	var oldestID int
	var oldest time.Time
	first := true
	for id, a := range w.appenders {
		if first || a.lastWrite().Before(oldest) {
			oldestID, oldest, first = id, a.lastWrite(), false
		}
	}
	if !first {
		_ = w.appenders[oldestID].close()
		delete(w.appenders, oldestID)
	}
}

func (w *Writer) openAppenders() int { return len(w.appenders) }

// Cleanup deletes files whose date part is older than RetentionDays and
// removes user directories left empty. It only touches names matching
// "<YYYY-MM-DD>[.<n>].jsonl[.gz]".
func (w *Writer) Cleanup(now time.Time) int {
	cutoff := now.AddDate(0, 0, -w.cfg.RetentionDays).Format(dateLayout)
	users, err := os.ReadDir(w.cfg.Dir)
	if err != nil {
		w.logf("bodylog: cleanup read dir: %v", err)
		return 0
	}
	deleted := 0
	for _, u := range users {
		if !u.IsDir() {
			continue
		}
		userDir := filepath.Join(w.cfg.Dir, u.Name())
		files, err := os.ReadDir(userDir)
		if err != nil {
			continue
		}
		names := make([]string, 0, len(files))
		for _, f := range files {
			names = append(names, f.Name())
		}
		sort.Strings(names)
		remaining := len(names)
		for _, name := range names {
			date, ok := fileDate(name)
			if !ok || date >= cutoff {
				continue
			}
			if err := os.Remove(filepath.Join(userDir, name)); err != nil {
				w.logf("bodylog: cleanup remove %s: %v", name, err)
				continue
			}
			deleted++
			remaining--
		}
		if remaining == 0 {
			_ = os.Remove(userDir)
		}
	}
	return deleted
}

// fileDate extracts the leading YYYY-MM-DD from a log file name.
func fileDate(name string) (string, bool) {
	if !strings.HasSuffix(name, ".jsonl") && !strings.HasSuffix(name, ".jsonl.gz") {
		return "", false
	}
	if len(name) < len(dateLayout) {
		return "", false
	}
	date := name[:len(dateLayout)]
	if _, err := time.Parse(dateLayout, date); err != nil {
		return "", false
	}
	return date, true
}
```

- [ ] **Step 4: 运行确认通过，并跑竞态检测**

Run: `GOWORK=off go test ./pkg/bodylog/ -race -v`
Expected: PASS，无 data race。

- [ ] **Step 5: 提交**

```bash
gofmt -l pkg/bodylog && git add pkg/bodylog/writer.go pkg/bodylog/writer_test.go && git commit -m "feat(bodylog): 异步单消费者 Writer，空闲回收、过期清理与优雅关闭

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018eDAyuMBUXRW4w3Eqg394r"
```

---

### Task 4: RelayInfo 响应字段 + OpenAI Chat handler 赋值

**Files:**
- Modify: `relay/common/relay_info.go`（在 `RequestId string` 字段附近，约第 155 行）
- Modify: `relay/channel/openai/relay-openai.go:182`（流式）与 `:354`（非流式）

**Interfaces:**
- Produces: `RelayInfo.ResponseBody []byte`（非流式，最终回给客户端的字节）、`RelayInfo.ResponseText string`（流式拼接文本）

- [ ] **Step 1: 加字段**

在 `relay/common/relay_info.go` 的 `RequestId string` 字段之后加入：

```go
	// ResponseBody holds the final non-stream response bytes sent to the client;
	// ResponseText holds the concatenated text of a streamed response.
	// Only populated by handlers that support request body logging.
	ResponseBody []byte
	ResponseText string
```

- [ ] **Step 2: 流式赋值**

在 `relay/channel/openai/relay-openai.go` 的 `OaiStreamHandler` 中，`if !containStreamUsage {` 这一段之前（约第 180 行）加入：

```go
	info.ResponseText = responseTextBuilder.String()
```

- [ ] **Step 3: 非流式赋值**

在 `OpenaiHandler` 中 `service.IOCopyBytesGracefully(c, resp, responseBody)` 这一行之前加入：

```go
	info.ResponseBody = responseBody
```

- [ ] **Step 4: 编译与既有测试**

Run: `GOWORK=off go build ./... && GOWORK=off go test ./relay/channel/openai/ ./relay/common/`
Expected: 通过。

- [ ] **Step 5: 提交**

```bash
gofmt -l relay/common relay/channel/openai && git add relay/common/relay_info.go relay/channel/openai/relay-openai.go && git commit -m "feat(relay): RelayInfo 记录 OpenAI Chat 的响应正文与流式文本

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018eDAyuMBUXRW4w3Eqg394r"
```

---

### Task 5: service 层：初始化与记录组装

**Files:**
- Create: `service/body_log.go`
- Test: `service/body_log_test.go`

**Interfaces:**
- Consumes: `bodylog.New/Enqueue/Close`、`bodylog.Record`、`bodylog.RawOrString`、`bodylog.RelativeFilePath`（Task 1、3）；`common.GetRequestBody`、`common.GetEnvOrDefault*`、`common.SysLog/SysError`；`relaycommon.RelayInfo` 的 `RequestId/UserId/TokenId/ChannelId/OriginModelName/UpstreamModelName/IsStream/StartTime/ResponseBody/ResponseText`
- Produces:
  ```go
  func InitRequestBodyLog()                 // 读环境变量，创建全局 writer；REQUEST_LOG_DIR=off 时保持 nil
  func CloseRequestBodyLog()                // main 关停时调用
  func RequestBodyLogEnabled() bool
  func RequestBodyLogFile(userID int, t time.Time) string   // 写入 logs.other 的相对路径
  type RequestBodyLogParams struct {
      Status           string   // bodylog.StatusOK / StatusError
      Error            string
      PromptTokens     int
      CompletionTokens int
  }
  func RecordRequestBodyLog(c *gin.Context, relayInfo *relaycommon.RelayInfo, params RequestBodyLogParams)
  func buildRequestBodyRecord(c *gin.Context, relayInfo *relaycommon.RelayInfo, params RequestBodyLogParams, now time.Time) bodylog.Record  // 纯组装，供测试
  ```

- [ ] **Step 1: 写失败的测试**

```go
// service/body_log_test.go
package service

import (
	"bytes"
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
		RequestId: "req-1", UserId: 42, TokenId: 9, ChannelId: 3,
		OriginModelName: "m-out", UpstreamModelName: "m-up", IsStream: false, StartTime: start,
		ResponseBody: []byte(`{"choices":[{"message":{"content":"hello"}}]}`),
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

func TestRequestBodyLogFile(t *testing.T) {
	ts := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	assert.Equal(t, "42/2026-09-09.jsonl", RequestBodyLogFile(42, ts))
}

func TestRecordRequestBodyLogNoopWhenDisabled(t *testing.T) {
	require.False(t, RequestBodyLogEnabled())
	c := newBodyLogContext(t, `{}`)
	RecordRequestBodyLog(c, &relaycommon.RelayInfo{UserId: 1, StartTime: time.Now()}, RequestBodyLogParams{Status: bodylog.StatusOK}) // must not panic
}
```

- [ ] **Step 2: 运行确认失败**

Run: `GOWORK=off go test ./service/ -run 'TestBuildRequestBodyRecord|TestRequestBodyLog|TestRecordRequestBodyLog' -v`
Expected: FAIL，`undefined: buildRequestBodyRecord`。

- [ ] **Step 3: 实现**

```go
// service/body_log.go
package service

import (
	"fmt"
	"io"
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
	cfg := bodylog.Config{
		Dir:           dir,
		MaxSizeBytes:  int64(common.GetEnvOrDefault("REQUEST_LOG_MAX_SIZE_MB", 256)) << 20,
		RetentionDays: common.GetEnvOrDefault("REQUEST_LOG_RETENTION_DAYS", 30),
		QueueSize:     common.GetEnvOrDefault("REQUEST_LOG_QUEUE_SIZE", 10000),
	}
	w, err := bodylog.New(cfg, time.Now, func(format string, args ...any) {
		common.SysError(fmt.Sprintf(format, args...))
	})
	if err != nil {
		common.SysError("request body log disabled: " + err.Error())
		return
	}
	requestBodyLogWriter = w
	common.SysLog(fmt.Sprintf("request body log enabled: dir=%s max_size=%dMB retention=%dd queue=%d",
		dir, cfg.MaxSizeBytes>>20, cfg.RetentionDays, cfg.QueueSize))
}

// CloseRequestBodyLog drains the queue; call it after the HTTP server has stopped.
func CloseRequestBodyLog() {
	if requestBodyLogWriter != nil {
		_ = requestBodyLogWriter.Close()
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
}

// RecordRequestBodyLog enqueues the request/response pair for the current relay.
// It never blocks and never affects the response.
func RecordRequestBodyLog(c *gin.Context, relayInfo *relaycommon.RelayInfo, params RequestBodyLogParams) {
	if requestBodyLogWriter == nil || relayInfo == nil || relayInfo.UserId == 0 {
		return
	}
	requestBodyLogWriter.Enqueue(buildRequestBodyRecord(c, relayInfo, params, time.Now()))
}

func buildRequestBodyRecord(c *gin.Context, relayInfo *relaycommon.RelayInfo, params RequestBodyLogParams, now time.Time) bodylog.Record {
	var input []byte
	if body, err := common.GetRequestBody(c); err == nil {
		if r, ok := body.(io.Reader); ok {
			input, _ = io.ReadAll(r)
		}
	}

	output := bodylog.RawOrString(nil)
	outputMissing := true
	if params.Status == bodylog.StatusOK {
		switch {
		case relayInfo.IsStream && relayInfo.ResponseText != "":
			output = bodylog.RawOrString(fmt.Appendf(nil, `{"text":%s}`, mustJSONString(relayInfo.ResponseText)))
			outputMissing = false
		case !relayInfo.IsStream && len(relayInfo.ResponseBody) > 0:
			output = bodylog.RawOrString(relayInfo.ResponseBody)
			outputMissing = false
		}
	} else {
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
		UpstreamModel:    relayInfo.UpstreamModelName,
		ChannelID:        relayInfo.ChannelId,
		Stream:           relayInfo.IsStream,
		Status:           params.Status,
		Error:            params.Error,
		PromptTokens:     params.PromptTokens,
		CompletionTokens: params.CompletionTokens,
		UseTimeMs:        useTimeMs,
		Input:            bodylog.RawOrString(input),
		Output:           output,
		OutputMissing:    outputMissing,
	}
}

func mustJSONString(s string) []byte {
	b, err := common.Marshal(s)
	if err != nil {
		return []byte(`""`)
	}
	return b
}
```

- [ ] **Step 4: 运行确认通过**

Run: `GOWORK=off go test ./service/ -run 'TestBuildRequestBodyRecord|TestRequestBodyLog|TestRecordRequestBodyLog' -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
gofmt -l service && git add service/body_log.go service/body_log_test.go && git commit -m "feat(service): 请求正文日志初始化与记录组装

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018eDAyuMBUXRW4w3Eqg394r"
```

---

### Task 6: 挂钩：结算成功、上游错误、生命周期

**Files:**
- Modify: `service/text_quota.go:523`（`model.RecordConsumeLog(` 之前与之后）
- Modify: `controller/relay.go:428`（`model.RecordErrorLog(` 之后）
- Modify: `main.go`（初始化在 `common.InitRedisClient()` 之后；关闭在 `srv.Shutdown` 之后、`SaveQuotaDataCache` 之前）

**Interfaces:**
- Consumes: `service.InitRequestBodyLog/CloseRequestBodyLog/RequestBodyLogEnabled/RequestBodyLogFile/RecordRequestBodyLog`、`bodylog.StatusOK/StatusError`

- [ ] **Step 1: 成功路径**

在 `service/text_quota.go` 中，`attachQuotaSaturation(ctx, relayInfo, other)` 之后、`model.RecordConsumeLog(` 之前加入：

```go
	if RequestBodyLogEnabled() {
		other.SetAdmin("body_file", RequestBodyLogFile(relayInfo.UserId, time.Now()))
	}
```

在 `model.RecordConsumeLog(...)` 调用结束的 `})` 之后加入：

```go
	RecordRequestBodyLog(ctx, relayInfo, RequestBodyLogParams{
		Status:           bodylog.StatusOK,
		PromptTokens:     summary.PromptTokens,
		CompletionTokens: summary.CompletionTokens,
	})
```

并在文件 import 中加入 `"github.com/QuantumNous/new-api/pkg/bodylog"`（`time` 已导入则无需重复）。

- [ ] **Step 2: 失败路径**

在 `controller/relay.go` 中 `model.RecordErrorLog(...)` 这一行之后（仍在 `if constant.ErrorLogEnabled && ...` 块内）加入：

```go
		service.RecordRequestBodyLog(c, relayInfo, service.RequestBodyLogParams{
			Status: bodylog.StatusError,
			Error:  err.MaskSensitiveErrorWithStatusCode(),
		})
```

在 `other.SetPublic("status_code", err.StatusCode)` 之后加入：

```go
		if service.RequestBodyLogEnabled() {
			other.SetAdmin("body_file", service.RequestBodyLogFile(userId, time.Now()))
		}
```

import 加入 `"github.com/QuantumNous/new-api/pkg/bodylog"`。

- [ ] **Step 3: 生命周期**

`main.go` 中 `err = common.InitRedisClient()` 及其错误处理之后加入：

```go
	service.InitRequestBodyLog()
```

`srv.Shutdown(ctx)` 的错误处理之后、`if common.DataExportEnabled {` 之前加入：

```go
	service.CloseRequestBodyLog()
```

若 `main.go` 尚未 import `service` 包，加入 `"github.com/QuantumNous/new-api/service"`。

- [ ] **Step 4: 编译、vet、相关测试**

Run: `GOWORK=off go build ./... && GOWORK=off go vet ./service/ ./controller/ . && GOWORK=off go test ./service/ ./controller/ ./pkg/bodylog/`
Expected: 全部通过。

- [ ] **Step 5: 提交**

```bash
gofmt -l service controller main.go && git add service/text_quota.go controller/relay.go main.go && git commit -m "feat: 在计费结算与错误日志处记录请求正文，随服务启停

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018eDAyuMBUXRW4w3Eqg394r"
```

---

### Task 7: 配置文件与 compose 挂卷

**Files:**
- Modify: `.env.example`（在 `# 日志数据库连接字符串` 段落之前）
- Modify: `docker-compose.local.yml`（server 服务）
- Modify: `deploy/aliyun/docker-compose.yml`（server 服务）
- Modify: `deploy/aliyun/.env.example`（`# ---------- 运行 ----------` 段）

- [ ] **Step 1: `.env.example`**

加入：

```
# 请求正文日志：每次中转请求的完整输入输出，按 <目录>/<user_id>/<日期>.jsonl 写入
# 默认启用，目录相对工作目录；设为 off 关闭
# REQUEST_LOG_DIR=data/requests
# 单文件超过此大小（MB）后轮转并 gzip
# REQUEST_LOG_MAX_SIZE_MB=256
# 保留天数，超期文件每天清理一次
# REQUEST_LOG_RETENTION_DAYS=30
# 内存队列长度，满则丢弃并计数
# REQUEST_LOG_QUEUE_SIZE=10000
```

- [ ] **Step 2: `docker-compose.local.yml`**

server 服务 `environment` 加 `- REQUEST_LOG_DIR=/data/requests`，并加：

```yaml
    volumes:
      - ./data/requests:/data/requests
```

- [ ] **Step 3: `deploy/aliyun/docker-compose.yml`**

server 服务加：

```yaml
    volumes:
      - ./requests:/data/requests
```

`deploy/aliyun/.env.example` 的运行段加入：

```
# 请求正文日志目录（容器内路径，compose 已挂卷到宿主机 ./requests）；设为 off 关闭
REQUEST_LOG_DIR=/data/requests
REQUEST_LOG_MAX_SIZE_MB=256
REQUEST_LOG_RETENTION_DAYS=30
```

- [ ] **Step 4: 校验 compose**

Run: `docker compose -f docker-compose.local.yml config --quiet && (cd deploy/aliyun && cp .env.example .env && sed -i 's/^SESSION_SECRET=$/SESSION_SECRET=x/' .env && docker compose config --quiet; rm -f .env)`
Expected: 无输出（语法正确）。

- [ ] **Step 5: 提交**

```bash
git add .env.example docker-compose.local.yml deploy/aliyun/docker-compose.yml deploy/aliyun/.env.example && git commit -m "chore: 请求正文日志的环境变量示例与 compose 挂卷

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018eDAyuMBUXRW4w3Eqg394r"
```

---

### Task 8: 端到端验收

**Files:** 无代码改动。

- [ ] **Step 1: 本地启动后端并让 web 容器指向宿主机**

```bash
docker compose -f docker-compose.local.yml stop server
IMAGE_TAG=test1 SERVER_UPSTREAM=host.docker.internal:3000 docker compose -f docker-compose.local.yml up -d --no-deps web
```

在 VS Code 按 F5（langfuse 配置）或终端：

```bash
SQL_DSN="postgresql://postgres:postgres@localhost:5433/new_api" REDIS_CONN_STRING="redis://:myredissecret@localhost:6380/5" \
SESSION_COOKIE_SECURE=false TRUSTED_PROXIES=172.16.0.0/12 NO_PROXY='*' go run main.go
```

启动日志应出现 `request body log enabled: dir=data/requests ...`。

- [ ] **Step 2: 非流式与流式各调一次**（令牌由用户提供，勿写入仓库）

```bash
TOKEN=sk-...
curl -s http://localhost:8080/v1/chat/completions -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"model":"deepseek-v4-flash-xiexin","messages":[{"role":"user","content":"用一句话介绍你自己"}]}' -D - | grep -i "x-oneapi-request-id"
curl -s http://localhost:8080/v1/chat/completions -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"model":"deepseek-v4-flash-xiexin","stream":true,"messages":[{"role":"user","content":"数到五"}]}' -D - | grep -i "x-oneapi-request-id"
```

- [ ] **Step 3: 核对文件**

```bash
ls data/requests/1/
tail -n 2 data/requests/1/$(date +%F).jsonl | python3 -c 'import sys,json
for l in sys.stdin:
    d=json.loads(l); print(d["request_id"], d["stream"], d["status"], d["prompt_tokens"], d["completion_tokens"], type(d["input"]).__name__, str(d["output"])[:60])'
```

Expected：两行，request_id 与响应头一致，非流式 `output` 为完整响应 JSON，流式 `output` 为 `{"text": "..."}`，`input` 为原始请求对象。

- [ ] **Step 4: 核对 logs.other**

```bash
docker exec langfuse-postgres-1 psql -U postgres -d new_api -tAc "select request_id, other::json->'admin_info'->>'body_file' from logs where type=2 order by id desc limit 2"
```

Expected：`1/<今天日期>.jsonl`。

- [ ] **Step 5: 更新 steps.md 并提交**

在 `steps.md` 末尾加一节：

```markdown
## 11. 请求正文日志

- 每次中转请求的完整输入输出写到 `REQUEST_LOG_DIR`（默认 `data/requests`，`off` 关闭）下 `<user_id>/<日期>.jsonl`，超 256MB 轮转为 `.n.jsonl.gz`，跨天压缩，保留 30 天。
- 查找：`logs.other.admin_info.body_file` 给出文件，`zgrep '"request_id":"…"' data/requests/<user_id>/<日期>*.jsonl*`。
- 本期只有 OpenAI Chat Completions 记录输出，其它格式 `output_missing=true`。
- 设计：`specs/2026-09-09-request-body-log-design.md`。
```

```bash
git add steps.md && git commit -m "docs: steps.md 增加请求正文日志说明

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018eDAyuMBUXRW4w3Eqg394r" && git push origin custom
```
