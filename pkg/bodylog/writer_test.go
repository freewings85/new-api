package bodylog

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	var failed atomic.Bool
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				if !w.Enqueue(mkRecord(7, "g"+string(rune('a'+g))+"-"+time.Duration(i).String())) {
					failed.Store(true)
				}
			}
		})
	}
	wg.Wait()
	require.False(t, failed.Load())
	w.Flush()

	f, err := os.Open(filepath.Join(dir, "7", "2026-09-09.jsonl"))
	require.NoError(t, err)
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	count := 0
	ids := make(map[string]struct{})
	for sc.Scan() {
		var m map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &m), "line %d must be valid JSON", count)
		assert.Equal(t, float64(7), m["user_id"])
		id, _ := m["request_id"].(string)
		ids[id] = struct{}{}
		count++
	}
	require.NoError(t, sc.Err())
	assert.Equal(t, 1600, count)
	assert.Len(t, ids, 1600, "every request_id must be intact and unique, not corrupted or duplicated")
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

func TestWriterDropsWhenQueueByteBudgetExceeded(t *testing.T) {
	dir := t.TempDir()
	w, err := newPaused(Config{Dir: dir, MaxSizeBytes: 1 << 20, RetentionDays: 30, QueueSize: 10, MaxQueueBytes: 100},
		func() time.Time { return day1 }, t.Logf)
	require.NoError(t, err)
	defer w.Close()

	big := func(id string) Record {
		rec := mkRecord(1, id)
		rec.Input = RawOrString([]byte(`"` + strings.Repeat("x", 58) + `"`)) // 60 bytes
		rec.Output = nil
		return rec
	}
	require.Equal(t, int64(60), recordBytes(big("a")))
	assert.True(t, w.Enqueue(big("a")), "60 of a 100 byte budget fits")
	assert.False(t, w.Enqueue(big("b")), "120 bytes exceeds the budget, record is dropped whole")
	assert.Equal(t, uint64(1), w.Dropped())

	w.resume()
	w.Flush()
	b, err := os.ReadFile(filepath.Join(dir, "1", "2026-09-09.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, 1, countLines(b))
	assert.Equal(t, int64(0), w.queuedBytes.Load(), "the consumer releases the bytes it wrote")
	assert.True(t, w.Enqueue(big("c")), "budget is free again once the queue drained")
	w.Flush()
}

func TestWriterEvictsLeastRecentlyWrittenAtCap(t *testing.T) {
	dir := t.TempDir()
	w, err := newPaused(Config{Dir: dir, MaxSizeBytes: 1 << 20, RetentionDays: 30, QueueSize: 10},
		func() time.Time { return day1 }, t.Logf)
	require.NoError(t, err)
	defer w.Close()
	w.maxOpen = 2

	for i, userID := range []int{1, 2, 3} {
		rec := mkRecord(userID, "first")
		rec.Time = day1.Add(time.Duration(i) * time.Minute)
		require.True(t, w.Enqueue(rec))
	}
	w.resume()
	w.Flush()

	assert.Equal(t, 2, w.openAppenders(), "the cap is enforced, not exceeded")
	_, open1 := w.appenders[1]
	_, open2 := w.appenders[2]
	_, open3 := w.appenders[3]
	assert.False(t, open1, "user 1 was written least recently and must be the evicted one")
	assert.True(t, open2)
	assert.True(t, open3)

	// Eviction only closes the handle; the next record for that user reopens
	// the same file and appends to it.
	second := mkRecord(1, "second")
	second.Time = day1.Add(3 * time.Minute)
	require.True(t, w.Enqueue(second))
	w.Flush()
	b, err := os.ReadFile(filepath.Join(dir, "1", "2026-09-09.jsonl"))
	require.NoError(t, err)
	assert.Equal(t, 2, countLines(b))
	assert.Contains(t, string(b), `"request_id":"second"`)
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

func TestWriterCleanupSkipsNonUserDirsAndPreexistingEmptyDirs(t *testing.T) {
	w, dir := newTestWriter(t, 100)
	emptyUserDir := filepath.Join(dir, "7")
	require.NoError(t, os.MkdirAll(emptyUserDir, 0o755))

	notesDir := filepath.Join(dir, "notes")
	expiredLooking := filepath.Join(notesDir, "2026-01-01.jsonl.gz")
	require.NoError(t, os.MkdirAll(notesDir, 0o755))
	require.NoError(t, os.WriteFile(expiredLooking, []byte("x"), 0o644))

	assert.Equal(t, 0, w.Cleanup(day1))
	_, err := os.Stat(emptyUserDir)
	assert.NoError(t, err, "pre-existing empty user dir must survive when this call deleted nothing from it")
	_, err = os.Stat(expiredLooking)
	assert.NoError(t, err, "non-numeric directory name must not be descended into")
}

func TestWriterCleanupRemovesProbeLeftovers(t *testing.T) {
	w, dir := newTestWriter(t, 100)
	leftover := filepath.Join(dir, ".probe-123456")
	require.NoError(t, os.WriteFile(leftover, nil, 0o600))
	keep := filepath.Join(dir, "notes.txt")
	require.NoError(t, os.WriteFile(keep, []byte("x"), 0o600))

	assert.Equal(t, 1, w.Cleanup(day1))
	_, err := os.Stat(leftover)
	assert.True(t, os.IsNotExist(err), "a probe left by a process that died at startup is removed")
	_, err = os.Stat(keep)
	assert.NoError(t, err, "unrelated root files are never touched")
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

// TestWriterCloseRightAfterNewDrainsQueue guards against a race where the
// consumer goroutine hasn't been scheduled yet when Close is called right
// after New: both the resume gate and the stop signal are ready by the time
// the goroutine's initial select runs, and picking stop must not skip the
// drain. Repeated because the race is scheduling-dependent, not guaranteed
// to reproduce on a single iteration.
func TestWriterCloseRightAfterNewDrainsQueue(t *testing.T) {
	for i := range 50 {
		dir := filepath.Join(t.TempDir(), "iter")
		w, err := New(Config{Dir: dir, MaxSizeBytes: 1 << 20, RetentionDays: 30, QueueSize: 10},
			func() time.Time { return day1 }, t.Logf)
		require.NoError(t, err)
		require.True(t, w.Enqueue(mkRecord(1, "r")))
		require.NoError(t, w.Close())

		b, err := os.ReadFile(filepath.Join(dir, "1", "2026-09-09.jsonl"))
		require.NoError(t, err, "iteration %d: file must exist", i)
		assert.Equal(t, 1, countLines(b), "iteration %d: queued record must be drained before Close returns", i)
	}
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

func TestFileDate(t *testing.T) {
	cases := []struct {
		name  string
		match bool
		date  string
	}{
		{"2026-09-09.jsonl", true, "2026-09-09"},
		{"2026-09-09.12.jsonl.gz", true, "2026-09-09"},
		{"2026-09-09.jsonl.gz.tmp", true, "2026-09-09"},
		{"2026-01-01-backup.jsonl", false, ""},
		{"2026-01-01.notes.jsonl", false, ""},
		{"notes.jsonl", false, ""},
		{"2026-09-09.txt", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			date, ok := fileDate(tc.name)
			assert.Equal(t, tc.match, ok)
			assert.Equal(t, tc.date, date)
		})
	}
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
