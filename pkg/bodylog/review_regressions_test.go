package bodylog

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests for the findings of the external review. Each protects one
// invariant the review found unprotected.

// Finding 1: records are not guaranteed to arrive in time order. A day-1 record
// arriving after day-2 records must not overwrite the day-1 segment that was
// already compressed.
func TestAppenderOutOfOrderDaysLoseNothing(t *testing.T) {
	dir := t.TempDir()
	a, err := openAppender(dir, 1, day1, 1<<20, syncGzip(t))
	require.NoError(t, err)
	require.NoError(t, a.write([]byte("A\n"), day1))
	require.NoError(t, a.write([]byte("B\n"), day2))
	require.NoError(t, a.write([]byte("C\n"), day1))
	require.NoError(t, a.write([]byte("D\n"), day2))
	require.NoError(t, a.close())

	assert.ElementsMatch(t, []string{"A", "C"}, segmentLines(t, filepath.Join(dir, "1"), "2026-09-09"))
	assert.ElementsMatch(t, []string{"B", "D"}, segmentLines(t, filepath.Join(dir, "1"), "2026-09-10"))
}

// Finding 1 (second half): the file handed to the compressor must never be the
// live "<date>.jsonl" path, otherwise a late record for that date reopens a
// file the compressor is about to delete and its bytes vanish with it.
func TestAppenderNeverCompressesLivePath(t *testing.T) {
	dir := t.TempDir()
	var handed []string
	a, err := openAppender(dir, 1, day1, 1<<20, func(p string) { handed = append(handed, p) })
	require.NoError(t, err)
	require.NoError(t, a.write([]byte("d1\n"), day1))
	require.NoError(t, a.write([]byte("d2\n"), day2)) // day rotation
	require.NoError(t, a.close())

	require.Len(t, handed, 1)
	assert.NotEqual(t, filepath.Join(dir, "1", "2026-09-09.jsonl"), handed[0], "compressor must get a rotated segment, not the live path")
	_, err = os.Stat(filepath.Join(dir, "1", "2026-09-09.jsonl"))
	assert.True(t, os.IsNotExist(err), "live day-1 path must be gone once rotated")
}

// Finding 2: a previous day's file left behind by an idle close (or a restart)
// must still get compressed by the maintenance pass, not sit uncompressed until
// retention deletes it.
func TestCleanupCompressesStaleUncompressedDays(t *testing.T) {
	w, dir := newTestWriter(t, 100)
	var handed []string
	var mu sync.Mutex
	w.gzip = func(p string) error { mu.Lock(); handed = append(handed, p); mu.Unlock(); return nil }
	require.True(t, w.Enqueue(mkRecord(1, "old")))
	w.Flush()
	w.Flush() // Flush twice so the consumer is idle when we poke its state.
	w.closeIdle(day1.Add(10 * time.Minute))
	require.Equal(t, 0, w.openAppenders())

	w.Cleanup(day2)
	require.NoError(t, w.Close()) // drains the compressor, so handed is final

	_, err := os.Stat(filepath.Join(dir, "1", "2026-09-09.jsonl"))
	assert.True(t, os.IsNotExist(err), "stale day-1 file must be rotated away")
	assert.Equal(t, []string{filepath.Join(dir, "1", "2026-09-09.1.jsonl")}, handed, "the rotated segment is handed to the compressor")
	assert.ElementsMatch(t, []string{"old"}, segmentIDs(t, filepath.Join(dir, "1"), "2026-09-09"))
}

// Finding 2 (guard): the maintenance pass must leave today's live file alone.
func TestCleanupLeavesTodayAlone(t *testing.T) {
	w, dir := newTestWriter(t, 100)
	require.True(t, w.Enqueue(mkRecord(1, "live")))
	w.Flush()
	w.Cleanup(day1)
	w.Flush()
	_, err := os.Stat(filepath.Join(dir, "1", "2026-09-09.jsonl"))
	require.NoError(t, err, "today's file must remain uncompressed and open")
	require.True(t, w.Enqueue(mkRecord(1, "live2")))
	w.Flush()
	assert.Equal(t, 2, countLines(readFile(t, filepath.Join(dir, "1", "2026-09-09.jsonl"))))
}

// Finding 3: compression runs on one worker; rotations faster than the
// compressor queue up instead of spawning unbounded concurrent gzips.
func TestCompressionIsSerialized(t *testing.T) {
	dir := t.TempDir()
	w, err := newPaused(Config{Dir: dir, MaxSizeBytes: 4, RetentionDays: 30, QueueSize: 100},
		func() time.Time { return day1 }, t.Logf)
	require.NoError(t, err)
	release := make(chan struct{})
	var inflight, maxInflight atomic.Int32
	w.gzip = func(path string) error {
		cur := inflight.Add(1)
		for {
			prev := maxInflight.Load()
			if cur <= prev || maxInflight.CompareAndSwap(prev, cur) {
				break
			}
		}
		<-release
		inflight.Add(-1)
		return gzipFile(path)
	}
	w.resume()
	for i := range 5 {
		require.True(t, w.Enqueue(mkRecord(1, "r"+string(rune('a'+i)))), "each record exceeds the 4-byte cap and rotates")
	}
	w.Flush()
	assert.Equal(t, 5, w.pendingCompressions()+int(inflight.Load()), "all five rotations are pending or in flight")
	close(release)
	require.NoError(t, w.Close())
	assert.Equal(t, int32(1), maxInflight.Load(), "never more than one gzip at a time")
	assert.Equal(t, 5, len(segmentIDs(t, filepath.Join(dir, "1"), "2026-09-09")))
}

// Finding 4: upstream error text and the skipped-input marker are unbounded
// strings too; the byte budget must weigh them.
func TestEnqueueBudgetCountsErrorText(t *testing.T) {
	dir := t.TempDir()
	w, err := newPaused(Config{Dir: dir, MaxSizeBytes: 1 << 20, RetentionDays: 30, QueueSize: 100, MaxQueueBytes: 100},
		func() time.Time { return day1 }, t.Logf)
	require.NoError(t, err)
	defer w.Close()
	big := Record{Time: day1, RequestID: "err", UserID: 1, Status: StatusError, Error: strings.Repeat("x", 1<<20)}
	assert.False(t, w.Enqueue(big), "a 1MB error message must not fit a 100-byte budget")
	assert.Equal(t, uint64(1), w.Dropped())
	assert.Equal(t, int64(0), w.queuedBytes.Load(), "rejected record reserves nothing")
	w.resume()
}

// Finding 5: an Enqueue that passed the closed check before Close started must
// either be written or rejected — never accepted and then silently discarded.
func TestEnqueueRacingCloseIsNotSilentlyLost(t *testing.T) {
	w, dir := newTestWriter(t, 100)
	var closeDone sync.WaitGroup
	w.beforeSend = func() {
		w.beforeSend = nil
		closeDone.Add(1)
		go func() {
			defer closeDone.Done()
			_ = w.Close()
		}()
		for !w.closed.Load() {
			runtime.Gosched()
		}
	}
	accepted := w.Enqueue(mkRecord(1, "late"))
	closeDone.Wait()
	if accepted {
		assert.ElementsMatch(t, []string{"late"}, segmentIDs(t, filepath.Join(dir, "1"), "2026-09-09"),
			"an accepted record must reach disk even when Close raced it")
	}
	assert.Equal(t, 0, len(w.queue), "nothing may be left stranded in the queue after Close")
}

// Finding 6: shutdown must honor a deadline instead of waiting forever on a
// stuck compressor.
func TestShutdownHonorsDeadline(t *testing.T) {
	dir := t.TempDir()
	w, err := newPaused(Config{Dir: dir, MaxSizeBytes: 4, RetentionDays: 30, QueueSize: 100},
		func() time.Time { return day1 }, t.Logf)
	require.NoError(t, err)
	block := make(chan struct{})
	w.gzip = func(path string) error { <-block; return nil }
	w.resume()
	require.True(t, w.Enqueue(mkRecord(1, "rotates")))
	w.Flush()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = w.Shutdown(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	close(block)
}

func TestShutdownDeadlineWhileEnqueueIsBlocked(t *testing.T) {
	for _, blockedAt := range []string{"before send", "drop warning"} {
		t.Run(blockedAt, func(t *testing.T) {
			dir := t.TempDir()
			cfg := Config{Dir: dir, MaxSizeBytes: 1 << 20, RetentionDays: 30, QueueSize: 10, MaxQueueBytes: 1 << 20}
			if blockedAt == "drop warning" {
				cfg.MaxQueueBytes = 1
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			w, err := newPaused(cfg, func() time.Time { return day1 }, func(string, ...any) {
				close(entered)
				<-release
			})
			require.NoError(t, err)
			if blockedAt == "before send" {
				w.beforeSend = func() {
					close(entered)
					<-release
				}
			}
			accepted := make(chan bool, 1)
			t.Cleanup(func() {
				close(release)
				assert.Equal(t, blockedAt == "before send", <-accepted)
				require.NoError(t, w.Close())
				if blockedAt == "before send" {
					assert.Equal(t, []string{"pending"}, segmentIDs(t, filepath.Join(dir, "1"), "2026-09-09"))
				}
			})
			w.resume()
			go func() { accepted <- w.Enqueue(mkRecord(1, "pending")) }()
			<-entered

			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer cancel()
			results := make(chan error, 2)
			for range 2 {
				go func() { results <- w.Shutdown(ctx) }()
			}
			for range 2 {
				select {
				case err := <-results:
					assert.ErrorIs(t, err, context.DeadlineExceeded)
				case <-time.After(5 * time.Second):
					// Deadlock guard only; entered/release control the interleaving.
					t.Fatal("Shutdown ignored its deadline while waiting for Enqueue")
				}
			}
		})
	}
}

// Finding 7: a partial line left on disk (a crash or a failed write) must be
// terminated before the next record is appended, so at most that one line is
// unparseable and every later record stays valid JSONL.
func TestAppenderTerminatesPartialLineOnOpen(t *testing.T) {
	dir := t.TempDir()
	userDir := filepath.Join(dir, "1")
	require.NoError(t, os.MkdirAll(userDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(userDir, "2026-09-09.jsonl"), []byte(`{"partial":`), 0o600))

	a, err := openAppender(dir, 1, day1, 1<<20, syncGzip(t))
	require.NoError(t, err)
	line, err := mkRecord(1, "after").Line()
	require.NoError(t, err)
	require.NoError(t, a.write(line, day1))
	require.NoError(t, a.close())

	lines := strings.Split(strings.TrimSuffix(string(readFile(t, filepath.Join(userDir, "2026-09-09.jsonl"))), "\n"), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, `{"partial":`, lines[0])
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &m), "record after a partial line must be intact")
	assert.Equal(t, "after", m["request_id"])
}

// segmentLines returns the trimmed lines of every segment file for date under
// userDir, compressed or not.
func segmentLines(t *testing.T, userDir, date string) []string {
	t.Helper()
	entries, err := os.ReadDir(userDir)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), date) || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		p := filepath.Join(userDir, e.Name())
		var content string
		if strings.HasSuffix(e.Name(), ".gz") {
			content = readGz(t, p)
		} else {
			content = string(readFile(t, p))
		}
		for _, l := range strings.Split(strings.TrimSuffix(content, "\n"), "\n") {
			if l != "" {
				out = append(out, l)
			}
		}
	}
	return out
}

// segmentIDs is segmentLines for JSON records, returning their request_ids.
func segmentIDs(t *testing.T, userDir, date string) []string {
	t.Helper()
	var ids []string
	for _, l := range segmentLines(t, userDir, date) {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &m))
		ids = append(ids, m["request_id"].(string))
	}
	return ids
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}
