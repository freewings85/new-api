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

	assert.Equal(t, "d1\n", readGz(t, filepath.Join(dir, "1", "2026-09-09.1.jsonl.gz")), "a closed day is a rotated segment like any other")
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

func TestAppenderDiscardedAfterRotationFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("test requires non-root to enforce permission restrictions")
	}
	dir := t.TempDir()
	a, err := openAppender(dir, 1, day1, 4, syncGzip(t)) // tiny cap to trigger rotation
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Chmod(filepath.Join(dir, "1"), 0o755)
	})

	// Write within cap
	require.NoError(t, a.write([]byte("a\n"), day1))
	// Write beyond cap to trigger rotateBySize, which will fail at os.Rename
	os.Chmod(filepath.Join(dir, "1"), 0o500)
	err = a.write([]byte("bb\n"), day1)
	require.Error(t, err)
	// After rotation failure, appender must be in predictable error state
	require.Nil(t, a.file, "appender.file should be nil after rotation failure")
	// Second write on failed appender returns consistent error
	err2 := a.write([]byte("c\n"), day1)
	require.Error(t, err2)
	assert.Equal(t, "bodylog: appender for "+filepath.Join(dir, "1")+" is closed", err2.Error())
	// close() does not panic on a failed appender
	require.NoError(t, a.close())
}
