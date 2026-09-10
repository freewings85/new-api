package bodylog

import (
	"compress/gzip"
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
// resuming the size counter from the existing file. A file that does not end
// in '\n' (a crash or a failed write left a partial line) gets one appended
// first, so the partial line stays isolated and every later record is intact.
func (a *appender) open(now time.Time) error {
	a.file = nil
	if err := os.MkdirAll(a.userDir, 0o700); err != nil {
		return fmt.Errorf("bodylog: mkdir %s: %w", a.userDir, err)
	}
	date := now.Format(dateLayout)
	path := filepath.Join(a.userDir, date+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("bodylog: open %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("bodylog: stat %s: %w", path, err)
	}
	size := st.Size()
	if size > 0 {
		last := make([]byte, 1)
		if _, err := f.ReadAt(last, size-1); err != nil {
			f.Close()
			return fmt.Errorf("bodylog: read tail %s: %w", path, err)
		}
		if last[0] != '\n' {
			n, err := f.Write([]byte{'\n'})
			size += int64(n)
			if err != nil {
				f.Close()
				return fmt.Errorf("bodylog: terminate partial line %s: %w", path, err)
			}
		}
	}
	a.file = f
	a.date = date
	a.size = size
	a.last = now
	return nil
}

func (a *appender) currentPath() string {
	return filepath.Join(a.userDir, a.date+".jsonl")
}

// write appends one line. A day change rotates before writing; exceeding the
// size cap rotates after writing, so a line is never split across files.
// After any returned error, the appender must be discarded by the caller.
func (a *appender) write(line []byte, now time.Time) error {
	if now.Format(dateLayout) != a.date {
		if err := a.rotate(now); err != nil {
			return err
		}
	}
	if a.file == nil {
		return fmt.Errorf("bodylog: appender for %s is closed", a.userDir)
	}
	before := a.size
	n, err := a.file.Write(line)
	a.size += int64(n)
	if err != nil {
		// A short write left part of this line on disk. Cut it back so the
		// next record appended to this file (by a fresh appender) does not
		// share a line with the fragment. Best effort: if the truncate fails
		// too, open() terminates the fragment with a newline instead.
		if n > 0 {
			if terr := a.file.Truncate(before); terr == nil {
				a.size = before
			}
		}
		return fmt.Errorf("bodylog: write %s: %w", a.currentPath(), err)
	}
	a.last = now
	if a.size >= a.maxSize {
		return a.rotate(now)
	}
	return nil
}

// rotate closes the current file, renames it to the next "<date>.<n>.jsonl"
// segment, hands that segment to the compressor and opens the file for now's
// date. The live "<date>.jsonl" path is never given to the compressor: a
// record for that date arriving later (records are not guaranteed to arrive in
// time order) simply opens a fresh live file, and every closed segment keeps
// its own index, so no segment can overwrite another.
func (a *appender) rotate(now time.Time) error {
	cur := a.currentPath()
	if err := a.file.Close(); err != nil {
		a.file = nil
		return fmt.Errorf("bodylog: close %s: %w", cur, err)
	}
	a.file = nil
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
	dst, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
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
		return fmt.Errorf("bodylog: gzip close %s: %w", path, err)
	}
	if err := dst.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("bodylog: gzip close %s: %w", path, err)
	}
	if err := os.Rename(tmp, path+".gz"); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("bodylog: gzip rename %s: %w", path, err)
	}
	return os.Remove(path)
}
