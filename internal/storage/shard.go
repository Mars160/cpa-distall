package storage

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	// DefaultWriterCount is the writer_count used when the configuration does not
	// name one. One writer is always correct, sharding only adds write parallelism.
	DefaultWriterCount = 1

	// MaxWriterCount bounds writer_count. Each SQLite shard is one database file
	// with its own writer connection, so the limit is a safety valve against a
	// configuration that would open hundreds of files and goroutines per plugin
	// instance, not a technical maximum.
	MaxWriterCount = 64

	// ManifestFileName is the layout descriptor inside a SQLite data directory.
	ManifestFileName = "manifest.json"

	// manifestFormat tags the descriptor so an unrelated JSON file is never
	// mistaken for a layout.
	manifestFormat = "cpa-distill/capture-shards"

	// manifestVersion is the layout version. It is bumped whenever the meaning of
	// the files in a data directory changes; a directory written by another
	// version is refused instead of being reinterpreted.
	manifestVersion = 1

	// shardAlgorithm names the fixed capture_id -> shard mapping. The name is
	// stored in the manifest so a directory can never be reopened with a
	// different mapping than the one that wrote it.
	shardAlgorithm = "sha256-be64-mod-n"

	// shardFileTemplate is the printf template of the shard database names.
	shardFileTemplate = "shard-%02d.db"

	// memoryDSNPrefix marks the in-memory (test/development) layout.
	memoryDSN = ":memory:"
)

// ErrKeyIncomplete is wrapped by the error AppendCapture returns when the capture
// row itself was stored but its client key could not be replicated to every shard.
// The capture is not lost, so the caller must not count it as a failed capture
// write; it must however keep the key eligible for a retry.
var ErrKeyIncomplete = errors.New("storage: client key replication incomplete")

// manifest is the on-disk descriptor of one SQLite data directory. All fields are
// fixed for the lifetime of the directory: a mismatch is an error, never a
// migration.
type manifest struct {
	Format         string `json:"format"`
	Version        int    `json:"version"`
	WriterCount    int    `json:"writer_count"`
	ShardAlgorithm string `json:"shard_algorithm"`
	ShardFile      string `json:"shard_file"`
}

func newManifest(writerCount int) manifest {
	return manifest{
		Format:         manifestFormat,
		Version:        manifestVersion,
		WriterCount:    writerCount,
		ShardAlgorithm: shardAlgorithm,
		ShardFile:      shardFileTemplate,
	}
}

// normalizeWriterCount applies the default and rejects out-of-range values. A
// zero value means "not configured"; a negative or too large value is a
// configuration error, not something to clamp silently.
func normalizeWriterCount(n int) (int, error) {
	switch {
	case n == 0:
		return DefaultWriterCount, nil
	case n < 0:
		return 0, fmt.Errorf("storage: writer_count must be >= 1 (got %d)", n)
	case n > MaxWriterCount:
		return 0, fmt.Errorf("storage: writer_count must be at most %d (got %d)", MaxWriterCount, n)
	}
	return n, nil
}

// shardIndex maps a capture ID onto one of n shards. The mapping is fixed and
// recorded in the manifest: sha256 of the 16 raw capture ID bytes, first 8 bytes
// read as a big-endian uint64, modulo n. It is stable across restarts and across
// processes, and uniform for the random IDs the plugin generates.
func shardIndex(id []byte, n int) int {
	if n <= 1 {
		return 0
	}
	sum := sha256.Sum256(id)
	return int(binary.BigEndian.Uint64(sum[:8]) % uint64(n))
}

// shardFileName is the database file name of shard i.
func shardFileName(i int) string {
	return fmt.Sprintf(shardFileTemplate, i)
}

// layout is the validated (or to-be-created) data directory of one sharded store.
type layout struct {
	dir string
	// publish is true when the directory has no manifest yet. The descriptor is
	// published only after every shard database exists, so a manifest on disk always
	// implies a complete set of shards: a missing shard file is a lost database, not
	// an initialisation step.
	publish bool
}

// errManifestExists reports that another process published a manifest while this
// one was initialising the same directory.
var errManifestExists = errors.New("manifest already published")

// prepareLayout validates (or reserves) the data directory of a sharded SQLite
// store. An existing directory is only reused when its manifest matches the
// requested writer_count and algorithm and every shard database it declares still
// exists; anything else is reported as an error. Nothing is ever deleted, moved or
// rewritten, and a manifest is never published here: the caller publishes it once
// the shards are on disk.
func prepareLayout(dir string, writerCount int) (layout, error) {
	dir = filepath.Clean(strings.TrimSpace(dir))
	info, err := os.Stat(dir)
	switch {
	case err == nil && !info.IsDir():
		return layout{}, fmt.Errorf("storage: sqlite DSN %q is an existing file; the sharded layout needs a data directory holding %s and %s files (migrate manually, nothing is deleted or rewritten automatically)", dir, ManifestFileName, shardFileName(0))
	case err != nil && !errors.Is(err, fs.ErrNotExist):
		return layout{}, fmt.Errorf("storage: inspect sqlite data directory %q: %w", dir, err)
	case errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return layout{}, fmt.Errorf("storage: create sqlite data directory %q: %w", dir, err)
		}
		return layout{dir: dir, publish: true}, nil
	}

	raw, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	if errors.Is(err, fs.ErrNotExist) {
		entries, readErr := os.ReadDir(dir)
		if readErr != nil {
			return layout{}, fmt.Errorf("storage: read sqlite data directory %q: %w", dir, readErr)
		}
		if len(entries) > 0 {
			return layout{}, fmt.Errorf("storage: sqlite data directory %q already holds %d entries but no %s; refusing to reuse an unknown layout (an interrupted initialisation or a foreign directory needs to be moved aside by hand, nothing is deleted or migrated automatically)", dir, len(entries), ManifestFileName)
		}
		return layout{dir: dir, publish: true}, nil
	}
	if err != nil {
		return layout{}, fmt.Errorf("storage: read %s in %q: %w", ManifestFileName, dir, err)
	}

	got, err := parseManifest(raw)
	if err != nil {
		return layout{}, fmt.Errorf("storage: %s in %q is not a valid layout descriptor: %w", ManifestFileName, dir, err)
	}
	if err := checkManifest(got, writerCount, dir); err != nil {
		return layout{}, err
	}
	if missing := missingShardFiles(dir, got); len(missing) > 0 {
		return layout{}, fmt.Errorf("storage: sqlite data directory %q is missing %s (or it is not a regular file); refusing to recreate an empty database, because that would silently hide the records the file used to hold — restore it from backup or point dsn at a new directory (nothing is deleted automatically)", dir, strings.Join(missing, ", "))
	}
	return layout{dir: dir}, nil
}

func parseManifest(raw []byte) (manifest, error) {
	var got manifest
	if err := json.Unmarshal(raw, &got); err != nil {
		return manifest{}, err
	}
	return got, nil
}

// checkManifest compares a stored descriptor with the requested configuration. The
// layout of a data directory is fixed for its lifetime: a mismatch is an error,
// never a migration.
func checkManifest(got manifest, writerCount int, dir string) error {
	want := newManifest(writerCount)
	if got != want {
		return fmt.Errorf("storage: sqlite data directory %q was written with a different layout (found %+v, want %+v); writer_count and the shard algorithm are fixed for the lifetime of a data directory, so use the original configuration or a new directory (another process may also have initialised this directory concurrently)", dir, got, want)
	}
	return nil
}

// missingShardFiles lists the shard databases a manifest declares that do not exist
// as regular files.
func missingShardFiles(dir string, m manifest) []string {
	missing := make([]string, 0, m.WriterCount)
	for i := 0; i < m.WriterCount; i++ {
		name := shardFileName(i)
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil || !info.Mode().IsRegular() {
			missing = append(missing, name)
		}
	}
	return missing
}

// writeManifest publishes the layout descriptor of a data directory whose shards
// already exist. The body is written next to its final name and linked into place,
// so a reader sees either no manifest or a complete one, and an already published
// manifest is never overwritten.
func writeManifest(dir string, writerCount int) error {
	return publishManifest(dir, newManifest(writerCount))
}

// publishManifest atomically publishes m as the layout of dir. Publishing is
// exclusive: when another process already published a manifest, the stored one wins
// and is validated instead of being replaced, so two processes initialising the same
// directory can never clobber a layout that is already in use. The directory is
// expected to be used by a single plugin instance at a time; concurrent
// initialisation with a different writer_count is refused, not merged.
//
// Exclusivity relies on os.Link failing with EEXIST. On a filesystem where hard
// links are unsupported the initialisation fails with a clear error, and there is
// deliberately no fallback to rename or to a non-exclusive create: either would
// silently allow an already published layout to be replaced. Use a data directory on
// a filesystem that supports hard links (local filesystems do; some network or
// object-store mounts do not).
func publishManifest(dir string, m manifest) error {
	target := filepath.Join(dir, ManifestFileName)
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("storage: encode %s: %w", ManifestFileName, err)
	}
	body = append(body, '\n')
	tmp, err := os.CreateTemp(dir, ManifestFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("storage: create %s in %q: %w", ManifestFileName, dir, err)
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: write %s in %q: %w", ManifestFileName, dir, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("storage: write %s in %q: %w", ManifestFileName, dir, err)
	}
	// link fails with EEXIST instead of replacing the target, which rename would do.
	if err := os.Link(name, target); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("storage: publish %s in %q: %w", ManifestFileName, dir, err)
		}
		raw, readErr := os.ReadFile(target)
		if readErr != nil {
			return fmt.Errorf("storage: read %s in %q: %w", ManifestFileName, dir, readErr)
		}
		got, parseErr := parseManifest(raw)
		if parseErr != nil {
			return fmt.Errorf("storage: %s in %q is not a valid layout descriptor: %w", ManifestFileName, dir, parseErr)
		}
		if got != m {
			return fmt.Errorf("storage: sqlite data directory %q was initialised concurrently with a different layout (found %+v, want %+v): %w", dir, got, m, errManifestExists)
		}
	}
	return nil
}

// memoryShardDSN builds the DSN of one in-memory shard. Every Open gets its own
// unique names, so two stores in one process never share a database; the shared
// cache keeps a shard coherent even if its single pooled connection is recycled.
func memoryShardDSN(token string, index int) string {
	return fmt.Sprintf("file:cpa-distill-%s-%02d?mode=memory&cache=shared&_pragma=busy_timeout(5000)", token, index)
}

// newMemoryToken is the per-Open namespace of the in-memory shards.
func newMemoryToken() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("storage: generate in-memory shard namespace: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
