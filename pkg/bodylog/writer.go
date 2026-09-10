package bodylog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	idleClose      = 5 * time.Minute
	idleScan       = time.Minute
	cleanupEvery   = 24 * time.Hour
	defaultMaxOpen = 1024
	// defaultMaxQueueBytes caps the body bytes waiting in the queue, so a burst
	// of very large bodies cannot grow the queue's memory without bound even
	// though the record count stays under QueueSize.
	defaultMaxQueueBytes = 256 << 20
	// probePrefix is the name prefix of the writability probe newPaused creates
	// in cfg.Dir; a crash between creating and removing it leaves one behind.
	probePrefix = ".probe-"
	// compressBacklog bounds segments waiting for the single compressor
	// goroutine. A rotation that finds it full leaves its segment uncompressed;
	// the next maintenance pass picks it up.
	compressBacklog = 1024
)

// maintRequest asks the consumer goroutine to run one maintenance pass.
type maintRequest struct {
	now    time.Time
	result chan int
}

// Writer receives records from any goroutine and writes them from one
// consumer goroutine, so every file has exactly one writer.
type Writer struct {
	cfg  Config
	now  func() time.Time
	logf func(string, ...any)

	queue          chan Record
	flushReq       chan chan struct{}
	maintReq       chan maintRequest
	stop           chan struct{}
	done           chan struct{}
	compressCh     chan string
	compressorDone chan struct{}
	closeOnce      sync.Once
	closed         atomic.Bool
	// mu lets Close wait for Enqueues that already passed the closed check, so
	// a record is never accepted after the queue stopped being drained.
	mu            sync.RWMutex
	dropped       atomic.Uint64
	queuedBytes   atomic.Int64 // body bytes currently sitting in the queue
	lastWarn      atomic.Int64 // unix seconds; throttles the queue-full log line
	lastBytesWarn atomic.Int64 // unix seconds; throttles the byte-budget log line
	lastOpenWarn  atomic.Int64 // unix seconds; throttles the appender-open-failure log line
	lastWriteWarn atomic.Int64 // unix seconds; throttles the write-failure log line
	lastCompWarn  atomic.Int64 // unix seconds; throttles the compression-backlog log line

	// consumer-goroutine-only state
	appenders map[int]*appender
	maxOpen   int

	// gzip compresses one rotated segment; tests replace it.
	gzip func(path string) error
	// beforeSend is a test hook run between Enqueue's closed check and its send.
	beforeSend func()

	// gate is always allocated. New closes it immediately via resume() so the
	// consumer starts right away; newPaused leaves it open so tests can fill
	// the queue deterministically before releasing the consumer with resume().
	gate chan struct{}
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
	if cfg.RetentionDays < 0 {
		return nil, errors.New("bodylog: RetentionDays must be >= 0")
	}
	if cfg.MaxQueueBytes <= 0 {
		cfg.MaxQueueBytes = defaultMaxQueueBytes
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("bodylog: create dir %s: %w", cfg.Dir, err)
	}
	probe, err := os.CreateTemp(cfg.Dir, probePrefix+"*")
	if err != nil {
		return nil, fmt.Errorf("bodylog: dir %s not writable: %w", cfg.Dir, err)
	}
	probe.Close()
	os.Remove(probe.Name())

	w := &Writer{
		cfg:            cfg,
		now:            now,
		logf:           logf,
		queue:          make(chan Record, cfg.QueueSize),
		flushReq:       make(chan chan struct{}),
		maintReq:       make(chan maintRequest),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
		compressCh:     make(chan string, compressBacklog),
		compressorDone: make(chan struct{}),
		appenders:      make(map[int]*appender),
		maxOpen:        defaultMaxOpen,
		gate:           make(chan struct{}),
		gzip:           gzipFile,
	}
	go w.run()
	go w.compressor()
	return w, nil
}

func (w *Writer) resume() { close(w.gate) }

// Enqueue hands a record to the consumer without blocking. It returns false
// when the writer is closed, the queue is full, or the record's bodies would
// push the queue over MaxQueueBytes. Records are dropped whole; bodies are
// never truncated.
func (w *Writer) Enqueue(rec Record) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed.Load() {
		return false
	}
	size := recordBytes(rec)
	// Reserve the bytes before sending so a concurrent Enqueue sees them, and
	// give them back on every path that does not hand the record to the
	// consumer, which is the only place that releases a queued record's bytes.
	if w.queuedBytes.Add(size) > w.cfg.MaxQueueBytes {
		w.queuedBytes.Add(-size)
		w.dropped.Add(1)
		w.throttledLogf(&w.lastBytesWarn, "bodylog: queue byte budget exceeded, dropped %d records so far", w.dropped.Load())
		return false
	}
	if w.beforeSend != nil {
		w.beforeSend()
	}
	select {
	case w.queue <- rec:
		return true
	default:
		w.queuedBytes.Add(-size)
		w.dropped.Add(1)
		w.throttledLogf(&w.lastWarn, "bodylog: queue full, dropped %d records so far", w.dropped.Load())
		return false
	}
}

// recordBytes is the queue-budget weight of a record: every field whose length
// the client or the upstream controls.
func recordBytes(rec Record) int64 {
	return int64(len(rec.Input) + len(rec.Output) + len(rec.Error) + len(rec.InputSkipped))
}

func (w *Writer) Dropped() uint64 { return w.dropped.Load() }

// throttledLogf logs at most once per minute per throttle counter, so a
// sustained failure (a full queue, a directory that stays unwritable) does
// not spam logf once per record.
func (w *Writer) throttledLogf(last *atomic.Int64, format string, args ...any) {
	nowSec := w.now().Unix()
	if prev := last.Load(); nowSec-prev >= 60 && last.CompareAndSwap(prev, nowSec) {
		w.logf(format, args...)
	}
}

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

// Shutdown stops intake, waits for the consumer to drain the queue and close
// every file, then waits for pending compressions — each wait bounded by ctx.
// On a deadline it returns ctx.Err() and leaves the remaining work running;
// the process is about to exit anyway.
func (w *Writer) Shutdown(ctx context.Context) error {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		// Wait for Enqueues that passed the closed check before it flipped;
		// after this no producer can add to the queue. Keep the barrier out
		// of closeOnce so every Shutdown caller can honor its own deadline,
		// even if an Enqueue is stuck writing a queue-full warning.
		go func() {
			w.mu.Lock()
			w.mu.Unlock()
			close(w.stop)
		}()
	})
	select {
	case <-w.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-w.compressorDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// Close is Shutdown without a deadline.
func (w *Writer) Close() error {
	return w.Shutdown(context.Background())
}

// pendingCompressions is the number of segments waiting for the compressor.
func (w *Writer) pendingCompressions() int { return len(w.compressCh) }

func (w *Writer) run() {
	defer close(w.done)
	select {
	case <-w.gate:
	case <-w.stop:
		// gate and stop can both be ready here: New closes gate immediately,
		// and a caller may Close before this goroutine is ever scheduled. A
		// bare "case <-w.stop: return" would then pick stop at random and
		// exit without draining the queue Close is about to await. Check
		// gate directly: if it was released, fall through into the main
		// loop so its stop arm drains the queue and closes files as normal.
		// Only a writer closed while still paused (gate never released,
		// e.g. newPaused without resume) returns immediately here.
		select {
		case <-w.gate:
		default:
			close(w.compressCh)
			return
		}
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
		case req := <-w.maintReq:
			req.result <- w.maintain(req.now)
		case <-idleTicker.C:
			w.closeIdle(w.now())
		case <-cleanTicker.C:
			w.maintain(w.now())
		case <-w.stop:
			w.drain()
			for id, a := range w.appenders {
				if err := a.close(); err != nil {
					w.logf("bodylog: %v", err)
				}
				delete(w.appenders, id)
			}
			// The consumer is the only sender on compressCh, so closing it
			// here lets the compressor finish its backlog and exit.
			close(w.compressCh)
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
	// The record leaves the queue here, whatever happens to it below.
	defer w.queuedBytes.Add(-recordBytes(rec))
	line, err := rec.Line()
	if err != nil {
		w.logf("bodylog: serialize request %s: %v", rec.RequestID, err)
		return
	}
	a, ok := w.appenders[rec.UserID]
	if !ok {
		if len(w.appenders) >= w.maxOpen {
			w.closeOldest()
		}
		a, err = openAppender(w.cfg.Dir, rec.UserID, rec.Time, w.cfg.MaxSizeBytes, w.compressAsync)
		if err != nil {
			w.throttledLogf(&w.lastOpenWarn, "bodylog: %v", err)
			return
		}
		w.appenders[rec.UserID] = a
	}
	if err := a.write(line, rec.Time); err != nil {
		w.throttledLogf(&w.lastWriteWarn, "bodylog: %v", err)
		if cerr := a.close(); cerr != nil {
			w.logf("bodylog: %v", cerr)
		}
		delete(w.appenders, rec.UserID)
	}
}

// compressAsync hands a rotated segment to the compressor goroutine without
// blocking the consumer. When the backlog is full the segment stays
// uncompressed on disk; maintain() will hand it over again later.
func (w *Writer) compressAsync(path string) {
	select {
	case w.compressCh <- path:
	default:
		w.throttledLogf(&w.lastCompWarn, "bodylog: compression backlog full, %s left uncompressed for now", path)
	}
}

// compressor is the single goroutine that gzips rotated segments, so rotations
// never spawn unbounded concurrent compressions or file handles.
func (w *Writer) compressor() {
	defer close(w.compressorDone)
	for path := range w.compressCh {
		if err := w.gzip(path); err != nil {
			w.logf("bodylog: %v", err)
		}
	}
}

// closeIdle closes appenders not written for idleClose. Consumer goroutine only.
func (w *Writer) closeIdle(now time.Time) {
	for id, a := range w.appenders {
		if now.Sub(a.lastWrite()) >= idleClose {
			if err := a.close(); err != nil {
				w.logf("bodylog: %v", err)
			}
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
		if err := w.appenders[oldestID].close(); err != nil {
			w.logf("bodylog: %v", err)
		}
		delete(w.appenders, oldestID)
	}
}

func (w *Writer) openAppenders() int { return len(w.appenders) }

// Cleanup runs one maintenance pass on the consumer goroutine and returns the
// number of files it deleted. Routing through the consumer means the pass
// knows which files are open and never compresses a live one. It returns 0
// once the writer is closed.
func (w *Writer) Cleanup(now time.Time) int {
	if w.closed.Load() {
		return 0
	}
	req := maintRequest{now: now, result: make(chan int, 1)}
	select {
	case w.maintReq <- req:
	case <-w.done:
		return 0
	}
	select {
	case n := <-req.result:
		return n
	case <-w.done:
		return 0
	}
}

// maintain deletes log files whose date part is older than RetentionDays,
// removes a user directory only when this call deleted its last file, and
// hands every closed-but-uncompressed segment to the compressor: rotated
// "<date>.<n>.jsonl" segments the backlog rejected earlier, and live-named
// "<date>.jsonl" files of past days that an idle close or a restart left
// behind (renamed to the next segment index first, so the name stays unique).
// Today's live file, and any file an open appender still writes, is left
// alone. It only descends into subdirectories of cfg.Dir whose name is a user
// id (all digits) and only touches file names matching fileDate's pattern.
// Writability probes left in the root directory by a process that died
// mid-startup are removed too. Consumer goroutine only.
func (w *Writer) maintain(now time.Time) int {
	cutoff := now.AddDate(0, 0, -w.cfg.RetentionDays).Format(dateLayout)
	today := now.Format(dateLayout)
	users, err := os.ReadDir(w.cfg.Dir)
	if err != nil {
		w.logf("bodylog: cleanup read dir: %v", err)
		return 0
	}
	deleted := 0
	for _, u := range users {
		if !u.IsDir() {
			if strings.HasPrefix(u.Name(), probePrefix) {
				if err := os.Remove(filepath.Join(w.cfg.Dir, u.Name())); err != nil {
					w.logf("bodylog: cleanup remove %s: %v", u.Name(), err)
					continue
				}
				deleted++
			}
			continue
		}
		if !isUserDirName(u.Name()) {
			continue
		}
		userID, err := strconv.Atoi(u.Name())
		if err != nil {
			continue
		}
		userDir := filepath.Join(w.cfg.Dir, u.Name())
		files, err := os.ReadDir(userDir)
		if err != nil {
			continue
		}
		deletedHere := 0
		for _, f := range files {
			date, ok := fileDate(f.Name())
			if !ok {
				continue
			}
			path := filepath.Join(userDir, f.Name())
			if date < cutoff {
				if err := os.Remove(path); err != nil {
					w.logf("bodylog: cleanup remove %s: %v", f.Name(), err)
					continue
				}
				deleted++
				deletedHere++
				continue
			}
			if !strings.HasSuffix(f.Name(), ".jsonl") {
				continue // already compressed, or a .gz.tmp the compressor owns
			}
			if f.Name() == date+".jsonl" {
				// Live-named file. Skip today's, and any date an open appender holds.
				if date >= today {
					continue
				}
				if a, open := w.appenders[userID]; open && a.date == date {
					continue
				}
				rotated := filepath.Join(userDir, nextRotatedName(userDir, date))
				if err := os.Rename(path, rotated); err != nil {
					w.logf("bodylog: cleanup rename %s: %v", f.Name(), err)
					continue
				}
				path = rotated
			}
			w.compressAsync(path)
		}
		// os.Remove fails harmlessly if files this call did not touch remain.
		if deletedHere > 0 {
			_ = os.Remove(userDir)
		}
	}
	return deleted
}

// isUserDirName reports whether name is a bare non-negative integer, the
// form Writer uses for per-user directories under cfg.Dir.
func isUserDirName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// logFileNamePattern matches "<YYYY-MM-DD>[.<n>].jsonl", its gzipped form,
// and the ".gz.tmp" file gzipFile leaves behind if killed mid-compression.
var logFileNamePattern = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})(?:\.\d+)?\.jsonl(?:\.gz(?:\.tmp)?)?$`)

// fileDate extracts the YYYY-MM-DD date from a log file name, requiring an
// exact match of logFileNamePattern so unrelated files are never touched.
func fileDate(name string) (string, bool) {
	m := logFileNamePattern.FindStringSubmatch(name)
	if m == nil {
		return "", false
	}
	if _, err := time.Parse(dateLayout, m[1]); err != nil {
		return "", false
	}
	return m[1], true
}
