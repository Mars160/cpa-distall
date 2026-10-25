package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSQLiteLayoutManifestIsWrittenAndFixed pins the on-disk layout contract: a
// data directory always carries manifest.json and one shard-XX.db per writer,
// writer_count=1 included, and the manifest is never rewritten to another value.
func TestSQLiteLayoutManifestIsWrittenAndFixed(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")

	for _, writerCount := range []int{1, 2, 4} {
		dir := filepath.Join(dir, fmt.Sprintf("w%d", writerCount))
		store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: writerCount})
		if err != nil {
			t.Fatalf("Open(writer_count %d): %v", writerCount, err)
		}
		if got := store.ShardCount(); got != writerCount {
			t.Fatalf("ShardCount() = %d, want %d", got, writerCount)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		raw, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		var got manifest
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("parse manifest: %v", err)
		}
		if want := newManifest(writerCount); got != want {
			t.Fatalf("manifest = %+v, want %+v", got, want)
		}
		for i := 0; i < writerCount; i++ {
			if _, err := os.Stat(filepath.Join(dir, shardFileName(i))); err != nil {
				t.Fatalf("writer_count %d: shard %d file: %v", writerCount, i, err)
			}
		}
		if _, err := os.Stat(filepath.Join(dir, shardFileName(writerCount))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("writer_count %d: an extra shard file exists: %v", writerCount, err)
		}
	}
}

// TestSQLiteLayoutRestartKeepsDataAndRejectsMismatch proves that a data directory
// is bound to the writer_count and the algorithm that created it: the same
// configuration reopens it with its data, any other configuration is refused and
// nothing is deleted, moved or migrated.
func TestSQLiteLayoutRestartKeepsDataAndRejectsMismatch(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")

	store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: 3})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	at := time.Date(2027, 2, 3, 4, 5, 6, 0, time.UTC)
	captureID := testCaptureID("layout-restart")
	if err := store.AppendCapture(ctx, Capture{
		CaptureID: captureID, StartedAt: at, CompletedAt: at.Add(time.Second),
	}, CaptureBodies{RequestBody: []byte("keep")}); err != nil {
		t.Fatalf("AppendCapture: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	before := directoryListing(t, dir)

	// A matching configuration reopens the directory and finds the record.
	again, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: 3})
	if err != nil {
		t.Fatalf("reopen(matching): %v", err)
	}
	detail, err := again.RecordDetail(ctx, captureID)
	if err != nil {
		t.Fatalf("RecordDetail after reopen: %v", err)
	}
	if string(detail.Bodies.RequestBody) != "keep" {
		t.Fatalf("request body after reopen = %q", detail.Bodies.RequestBody)
	}
	if err := again.Close(); err != nil {
		t.Fatalf("Close after reopen: %v", err)
	}

	for _, writerCount := range []int{1, 2, 4, MaxWriterCount} {
		store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: writerCount})
		if err == nil {
			store.Close()
			t.Fatalf("Open(writer_count %d) on a directory written with 3 succeeded, want a layout mismatch", writerCount)
		}
		if !strings.Contains(err.Error(), "different layout") {
			t.Fatalf("Open(writer_count %d) error = %v, want a layout mismatch report", writerCount, err)
		}
	}
	// The refused opens must not have created, removed or rewritten anything.
	if after := directoryListing(t, dir); after != before {
		t.Fatalf("a refused open changed the data directory:\n before %s\n after  %s", before, after)
	}

	// A tampered manifest is refused too, even when the writer_count matches.
	path := filepath.Join(dir, ManifestFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	tampered := strings.Replace(string(raw), shardAlgorithm, "identity-mod-n", 1)
	if tampered == string(raw) {
		t.Fatal("the manifest does not record the shard algorithm")
	}
	if err := os.WriteFile(path, []byte(tampered), 0o600); err != nil {
		t.Fatalf("tamper manifest: %v", err)
	}
	if store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: 3}); err == nil {
		store.Close()
		t.Fatal("Open with a tampered algorithm succeeded, want a layout mismatch")
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("restore manifest: %v", err)
	}

	// An unreadable manifest is an error, not a silent rewrite.
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("corrupt manifest: %v", err)
	}
	if store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: 3}); err == nil {
		store.Close()
		t.Fatal("Open with a corrupt manifest succeeded, want an error")
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("restore manifest: %v", err)
	}

	// The directory still holds its record.
	store, err = Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: 3})
	if err != nil {
		t.Fatalf("reopen after tampering: %v", err)
	}
	defer store.Close()
	if got := countCaptures(t, store); got != 1 {
		t.Fatalf("stored %d captures, want 1", got)
	}
}

// directoryListing renders the names and sizes of a directory, so a test can prove
// that a refused open did not touch it.
func directoryListing(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	var out strings.Builder
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("stat %s: %v", entry.Name(), err)
		}
		fmt.Fprintf(&out, "%s=%d;", entry.Name(), info.Size())
	}
	return out.String()
}

// TestSQLiteRejectsUnknownLayouts keeps the loud-failure behaviour for anything the
// plugin did not write itself.
func TestSQLiteRejectsUnknownLayouts(t *testing.T) {
	ctx := context.Background()

	t.Run("existing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "captures.db")
		if err := os.WriteFile(path, []byte("not a data directory"), 0o600); err != nil {
			t.Fatalf("create file: %v", err)
		}
		store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: path})
		if err == nil {
			store.Close()
			t.Fatal("Open(existing file) succeeded, want an error")
		}
		if !strings.Contains(err.Error(), "existing file") {
			t.Fatalf("Open(existing file) error = %v, want it to explain the directory layout", err)
		}
		if data, err := os.ReadFile(path); err != nil || string(data) != "not a data directory" {
			t.Fatalf("the refused open touched the file: %q %v", data, err)
		}
	})

	t.Run("directory without manifest", func(t *testing.T) {
		dir := t.TempDir()
		stray := filepath.Join(dir, "records.db")
		if err := os.WriteFile(stray, []byte("stray"), 0o600); err != nil {
			t.Fatalf("create stray file: %v", err)
		}
		store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir})
		if err == nil {
			store.Close()
			t.Fatal("Open(directory without manifest) succeeded, want an error")
		}
		if !strings.Contains(err.Error(), ManifestFileName) {
			t.Fatalf("Open(directory without manifest) error = %v, want it to mention %s", err, ManifestFileName)
		}
		if data, err := os.ReadFile(stray); err != nil || string(data) != "stray" {
			t.Fatalf("the refused open touched the stray file: %q %v", data, err)
		}
		if _, err := os.Stat(filepath.Join(dir, ManifestFileName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the refused open wrote a manifest: %v", err)
		}
	})

	t.Run("sqlite uri dsn", func(t *testing.T) {
		for _, dsn := range []string{
			"file:/tmp/cpa-distill.db",
			"file:records.db?_pragma=busy_timeout(5000)",
			"file::memory:?cache=shared",
		} {
			store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dsn})
			if err == nil {
				store.Close()
				t.Fatalf("Open(%q) succeeded, want a rejection", dsn)
			}
			if !strings.Contains(err.Error(), "URI") {
				t.Fatalf("Open(%q) error = %v, want it to explain that SQLite URIs are not supported", dsn, err)
			}
		}
	})

	t.Run("relative directory is created", func(t *testing.T) {
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatalf("getwd: %v", err)
		}
		if err := os.Chdir(t.TempDir()); err != nil {
			t.Fatalf("chdir: %v", err)
		}
		defer func() { _ = os.Chdir(cwd) }()

		store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: "cpa-distill-data", WriterCount: 2})
		if err != nil {
			t.Fatalf("Open(relative directory): %v", err)
		}
		defer store.Close()
		if got := store.ShardCount(); got != 2 {
			t.Fatalf("ShardCount() = %d, want 2", got)
		}
		if _, err := os.Stat(filepath.Join("cpa-distill-data", ManifestFileName)); err != nil {
			t.Fatalf("relative data directory has no manifest: %v", err)
		}
	})
}

// TestSQLiteMemoryLayoutIsThrowawayAndIsolated covers the in-memory DSN: it needs
// no manifest, survives several shards, and two stores never share a database.
func TestSQLiteMemoryLayoutIsThrowawayAndIsolated(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2027, 3, 4, 5, 6, 7, 0, time.UTC)

	first, err := Open(ctx, Config{Driver: DriverSQLite, DSN: memoryDSN, WriterCount: 3})
	if err != nil {
		t.Fatalf("Open(:memory:, writer_count 3): %v", err)
	}
	second, err := Open(ctx, Config{Driver: DriverSQLite, DSN: memoryDSN, WriterCount: 3})
	if err != nil {
		first.Close()
		t.Fatalf("Open(second :memory:): %v", err)
	}
	defer first.Close()
	defer second.Close()

	if got := first.ShardCount(); got != 3 {
		t.Fatalf("ShardCount() = %d, want 3", got)
	}
	captureID := testCaptureID("memory")
	if err := first.AppendCapture(ctx, Capture{CaptureID: captureID, StartedAt: at, CompletedAt: at.Add(time.Second)}, CaptureBodies{RequestBody: []byte("memory-only")}); err != nil {
		t.Fatalf("AppendCapture: %v", err)
	}
	detail, err := first.RecordDetail(ctx, captureID)
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	if string(detail.Bodies.RequestBody) != "memory-only" {
		t.Fatalf("request body = %q", detail.Bodies.RequestBody)
	}
	if _, err := second.RecordDetail(ctx, captureID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the second in-memory store sees the first one's record: %v", err)
	}
	// A memory store carries no manifest and no files at all.
	if _, err := os.Stat(ManifestFileName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an in-memory store wrote %s", ManifestFileName)
	}
}

// TestSQLiteEveryShardCarriesTheSchema pins that a shard is never opened half
// initialised: each one has the three tables of this version.
func TestSQLiteEveryShardCarriesTheSchema(t *testing.T) {
	store, _ := openTempSQLiteShards(t, 4)
	forEachShard(t, store, func(index int, db *sql.DB) {
		for _, table := range []string{"captures", "capture_bodies", "client_keys"} {
			var name string
			err := db.QueryRowContext(context.Background(),
				"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&name)
			if err != nil {
				t.Fatalf("shard %d is missing table %s: %v", index, table, err)
			}
		}
	})
}

// TestSQLiteDataDirectoryWithSpecialCharacters covers the DSN escaping: a data
// directory whose name carries a space, a percent sign, a hash or an ampersand must
// still resolve to the same files instead of a mangled path.
func TestSQLiteDataDirectoryWithSpecialCharacters(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"data dir", "data%20dir", "data#1", "data&2", "data?3"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), name)
			store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: 2})
			if err != nil {
				t.Fatalf("Open(%q): %v", dir, err)
			}
			defer store.Close()

			at := time.Date(2027, 4, 5, 6, 7, 8, 0, time.UTC)
			captureID := testCaptureID("special-" + name)
			if err := store.AppendCapture(ctx, Capture{
				CaptureID: captureID, StartedAt: at, CompletedAt: at.Add(time.Second),
			}, CaptureBodies{RequestBody: []byte("special")}); err != nil {
				t.Fatalf("AppendCapture: %v", err)
			}
			detail, err := store.RecordDetail(ctx, captureID)
			if err != nil {
				t.Fatalf("RecordDetail: %v", err)
			}
			if string(detail.Bodies.RequestBody) != "special" {
				t.Fatalf("request body = %q", detail.Bodies.RequestBody)
			}
			// The shard files really live inside that directory, not next to it.
			for _, want := range []string{ManifestFileName, shardFileName(0), shardFileName(1)} {
				if _, err := os.Stat(filepath.Join(dir, want)); err != nil {
					t.Fatalf("expected %s inside %q: %v", want, dir, err)
				}
			}
		})
	}
}

// TestSQLiteMissingShardFileIsRefusedNotRecreated is the data-loss regression: a
// published manifest implies a complete set of shard databases, so a directory that
// lost one of them must be refused. Recreating the file would open an empty
// database and hide the loss of everything it used to hold.
func TestSQLiteMissingShardFileIsRefusedNotRecreated(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	at := time.Date(2027, 5, 6, 7, 8, 9, 0, time.UTC)
	captureID := testCaptureID("missing-shard")

	store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.AppendCapture(ctx, Capture{
		CaptureID: captureID, StartedAt: at, CompletedAt: at.Add(time.Second),
	}, CaptureBodies{RequestBody: []byte("precious")}); err != nil {
		t.Fatalf("AppendCapture: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Move one shard aside: it stands in for a file lost by a failed disk, a bad
	// backup restore or a manual cleanup. Moving keeps the file for inspection.
	lost := shardFileName(1)
	aside := filepath.Join(dir, lost+".moved-aside")
	if err := os.Rename(filepath.Join(dir, lost), aside); err != nil {
		t.Fatalf("move %s aside: %v", lost, err)
	}
	before := directoryListing(t, dir)

	store, err = Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: 2})
	if err == nil {
		store.Close()
		t.Fatal("Open with a missing shard database succeeded, want a refusal")
	}
	if !strings.Contains(err.Error(), lost) {
		t.Fatalf("error = %v, want it to name the missing %s", err, lost)
	}
	if _, statErr := os.Stat(filepath.Join(dir, lost)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("the refused open recreated %s: %v", lost, statErr)
	}
	if after := directoryListing(t, dir); after != before {
		t.Fatalf("the refused open changed the data directory:\n before %s\n after  %s", before, after)
	}

	// Restoring the file is what makes the directory usable again: the record is
	// still there, so the refusal did not cost anything.
	if err := os.Rename(aside, filepath.Join(dir, lost)); err != nil {
		t.Fatalf("restore %s: %v", lost, err)
	}
	store, err = Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: 2})
	if err != nil {
		t.Fatalf("Open after restoring the shard: %v", err)
	}
	defer store.Close()
	detail, err := store.RecordDetail(ctx, captureID)
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	if string(detail.Bodies.RequestBody) != "precious" {
		t.Fatalf("request body = %q", detail.Bodies.RequestBody)
	}
	if got := countCaptures(t, store); got != 1 {
		t.Fatalf("stored %d captures, want 1", got)
	}
}

// TestSQLiteManifestPublishIsExclusive pins that publishing a layout never replaces
// one that is already on disk: a second initialisation of the same directory has to
// accept the stored layout or fail, and it never leaves a partial manifest behind.
func TestSQLiteManifestPublishIsExclusive(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: 2})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	path := filepath.Join(dir, ManifestFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	// A different layout must not be able to claim the directory.
	if err := publishManifest(dir, newManifest(4)); !errors.Is(err, errManifestExists) {
		t.Fatalf("publishManifest(writer_count 4) error = %v, want %v", err, errManifestExists)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != string(raw) {
		t.Fatalf("the published manifest was replaced: %q %v", got, err)
	}

	// Publishing the very same layout again is a no-op, not a conflict.
	if err := publishManifest(dir, newManifest(2)); err != nil {
		t.Fatalf("publishManifest(writer_count 2) error = %v, want nil", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != string(raw) {
		t.Fatalf("a repeated publish changed the manifest: %q %v", got, err)
	}
	for _, entry := range mustReadDir(t, dir) {
		if strings.HasPrefix(entry.Name(), ManifestFileName+".tmp-") {
			t.Fatalf("a temporary manifest was left behind: %s", entry.Name())
		}
	}
}

// TestSQLiteConcurrentInitialisationCannotClobberTheLayout runs two Opens against
// the same fresh directory with different writer_counts: exactly one layout may win,
// the stored manifest must be that winner's, and the loser must be refused instead
// of rewriting it.
func TestSQLiteConcurrentInitialisationCannotClobberTheLayout(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")

	type result struct {
		writerCount int
		err         error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, writerCount := range []int{1, 4} {
		go func(writerCount int) {
			<-start
			store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: writerCount})
			if store != nil {
				store.Close()
			}
			results <- result{writerCount: writerCount, err: err}
		}(writerCount)
	}
	close(start)

	var winner int
	failures := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		got := <-results
		if got.err == nil {
			winner = got.writerCount
			continue
		}
		failures = append(failures, fmt.Sprintf("writer_count %d: %v", got.writerCount, got.err))
	}
	if len(failures) != 1 {
		t.Fatalf("concurrent initialisation with writer_count 1 and 4 produced %d failures (%v), want exactly one winner", len(failures), failures)
	}

	raw, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	got, err := parseManifest(raw)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if want := newManifest(winner); got != want {
		t.Fatalf("published manifest = %+v, want the winner's %+v", got, want)
	}

	// The winning layout reopens; the losing one stays refused.
	store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: winner})
	if err != nil {
		t.Fatalf("Open(writer_count %d, the winner): %v", winner, err)
	}
	defer store.Close()
	if got := store.ShardCount(); got != winner {
		t.Fatalf("ShardCount() = %d, want %d", got, winner)
	}
	loser := 4
	if winner == 4 {
		loser = 1
	}
	if store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir, WriterCount: loser}); err == nil {
		store.Close()
		t.Fatalf("Open(writer_count %d, the loser) succeeded, want a layout mismatch", loser)
	} else if !strings.Contains(err.Error(), "different layout") {
		t.Fatalf("Open(writer_count %d, the loser) error = %v, want a layout mismatch", loser, err)
	}
}

func mustReadDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	return entries
}
