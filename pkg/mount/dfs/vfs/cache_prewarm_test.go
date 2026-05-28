package vfs

import (
	"os"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "vfs-test-cfg")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

const mb = 1024 * 1024

func TestTailPrewarmSize(t *testing.T) {
	cases := []struct {
		name       string
		file       string
		size       int64
		headWarmed int64
		want       int64
	}{
		// Large non-faststart MP4: warm the 4MB tail for the moov atom.
		{"large_mp4", "movie.mp4", 2000 * mb, 16 * mb, 4 * mb},
		{"large_mov", "clip.mov", 800 * mb, 16 * mb, 4 * mb},
		{"large_m4v", "ep.m4v", 500 * mb, 16 * mb, 4 * mb},
		// MKV front-loads its header — no tail warm (would waste bandwidth).
		{"mkv", "movie.mkv", 2000 * mb, 16 * mb, 0},
		{"webm", "clip.webm", 500 * mb, 16 * mb, 0},
		// Non-video: no tail warm.
		{"flac", "song.flac", 50 * mb, 8 * mb, 0},
		{"srt", "subs.srt", 1 * mb, 1 * mb, 0},
		// Small mp4 already fully covered by head pre-warm: no separate tail.
		{"small_mp4", "tiny.mp4", 10 * mb, 16 * mb, 0},
		{"mp4_head_reaches_eof", "x.mp4", 18 * mb, 16 * mb, 0}, // 18 <= 16+4
		{"mp4_just_over", "x.mp4", 21 * mb, 16 * mb, 4 * mb},   // 21 > 16+4
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := tailPrewarmSize(c.file, c.size, c.headWarmed); got != c.want {
				t.Fatalf("tailPrewarmSize(%q, %d, %d) = %d, want %d",
					c.file, c.size, c.headWarmed, got, c.want)
			}
		})
	}
}

func TestPrewarmSizeForCapsAtFileSize(t *testing.T) {
	// A tiny video file should pre-warm only its actual size, never more.
	if got := prewarmSizeFor("x.mkv", 3*mb); got != 3*mb {
		t.Fatalf("prewarmSizeFor tiny video = %d, want %d", got, 3*mb)
	}
	// A large video file pre-warms the 16MB video header.
	if got := prewarmSizeFor("x.mkv", 2000*mb); got != 16*mb {
		t.Fatalf("prewarmSizeFor large video = %d, want %d", got, 16*mb)
	}
}
