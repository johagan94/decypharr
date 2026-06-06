//go:build linux || (darwin && amd64)

package hanwen

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

const (
	// ReadTimeout is the maximum time a single read operation can take
	// Increased to 120s to handle slow debrid/CDN connections
	ReadTimeout  = 120 * time.Second
	AttrTimeout  = 30 * time.Second
	EntryTimeout = 1 * time.Second
)

func init() {
	backend.Register(backend.Hanwen, NewBackend)
}

// Backend implements the hanwen/go-fuse backend
type Backend struct {
	config      *config.FuseConfig
	logger      zerolog.Logger
	server      *fuse.Server
	ready       atomic.Bool
	unmountFunc func(ctx context.Context)
	root        *Dir
	vfs         *vfs.Manager
}

// NewBackend creates a new hanwen backend
func NewBackend(vfs *vfs.Manager, config *config.FuseConfig) (backend.Backend, error) {
	now := utils.Now()
	log := logger.New("hanwen-backend")
	// One shared rate-limited logger for the whole mount. Files/Dirs reference
	// it instead of allocating their own xsync map per inode — dedup keys are
	// already unique per inode so a shared map gives identical behaviour.
	rl := logger.NewRateLimitedLogger(logger.WithLogger(log))
	root := NewDir(vfs, "", LevelRoot, uint64(now.Unix()), config, log, rl)
	return &Backend{
		config: config,
		logger: log,
		root:   root,
		vfs:    vfs,
	}, nil
}

// Mount mounts the filesystem using hanwen/go-fuse
func (b *Backend) Mount(ctx context.Context) error {
	// Create mount point if it doesn't exist(skip if on Windows)

	if b.root == nil {
		return fmt.Errorf("root node is not initialized")
	}
	if b.vfs == nil {
		return fmt.Errorf("VFS manager is not initialized")
	}

	_ = os.MkdirAll(b.config.MountPath, 0755)

	// Clear any stale mount left behind by a previous or still-exiting process
	// before mounting. A force-unmount can lose a race with the old process (the
	// mount isn't freed until it fully exits), so verify with isMounted and retry
	// a few times. fs.Mount below is still bounded by DaemonTimeout, so a mount
	// point that stays busy fails fast with a clear error instead of hanging.
	for attempt := 0; attempt < 6; attempt++ {
		if !isMounted(b.config.MountPath) {
			break
		}
		if attempt > 0 {
			b.logger.Warn().
				Int("attempt", attempt).
				Str("mount_path", b.config.MountPath).
				Msg("Mount point still busy from a previous run; clearing before remount")
			time.Sleep(time.Second)
		}
		b.forceUnmount(ctx)
	}

	mountOpt := fuse.MountOptions{
		FsName:               "decypharr",
		Debug:                false,
		Name:                 "decypharr",
		DisableXAttrs:        true,
		IgnoreSecurityLabels: true,
		MaxWrite:             1024 * 1024,
		AllowOther:           true,
	}

	var opt []string

	opt = append(opt, "default_permissions")

	if runtime.GOOS == "darwin" {
		opt = append(opt, "volname=decypharr")
		opt = append(opt, "noapplexattr")
		opt = append(opt, "noappledouble")
	}

	mountOpt.Options = opt

	// Configure FUSE options
	// Use short entry timeout (1s) to ensure new files appear quickly
	entryTimeout := EntryTimeout
	attrTimeout := AttrTimeout
	opts := &fs.Options{
		AttrTimeout:  &attrTimeout,
		EntryTimeout: &entryTimeout,
		MountOptions: mountOpt,
		UID:          b.config.UID,
		GID:          b.config.GID,
	}

	// Start timer before creating NodeFS - adjust timeout duration as needed
	mountCtx, cancel := context.WithTimeout(ctx, b.config.DaemonTimeout)
	defer cancel()

	// Channel to receive the result of fs.Mount
	type fsResult struct {
		server *fuse.Server
		err    error
	}
	fsResultChan := make(chan fsResult, 1)

	// Run fs.Mount in a goroutine
	go func() {
		server, err := fs.Mount(b.config.MountPath, b.root, opts)
		fsResultChan <- fsResult{server: server, err: err}
	}()

	var server *fuse.Server
	select {
	case result := <-fsResultChan:
		server = result.server
		if result.err != nil {
			return fmt.Errorf("failed to create mount: %w", result.err)
		}
	case <-mountCtx.Done():
		b.ready.Store(false)
		return fmt.Errorf("timeout creating mount: %w", mountCtx.Err())
	}

	b.server = server

	// Now wait for the mount to be ready with the same timeout context
	b.logger.Info().
		Str("mount_path", b.config.MountPath).
		Msg("Waiting for mount to be ready")

	waitChan := make(chan error, 1)
	go func() {
		waitChan <- server.WaitMount()
	}()

	select {
	case err := <-waitChan:
		if err != nil {
			_ = server.Unmount() // cleanup on error
			return fmt.Errorf("failed to wait for mount: %w", err)
		}
	case <-mountCtx.Done():
		_ = server.Unmount() // cleanup on timeout
		return fmt.Errorf("timeout waiting for mount to be ready: %w", mountCtx.Err())
	}

	umount := func(ctx context.Context) {
		b.logger.Info().Msg("Unmounting filesystem")

		// Create a channel to track completion
		done := make(chan struct{})

		go func() {
			// Close VFS manager
			if b.vfs != nil {
				if err := b.vfs.Close(); err != nil {
					b.logger.Warn().Err(err).Msg("Failed to close VFS")
				}
			}

			_ = server.Unmount()
			time.Sleep(1 * time.Second)

			// Check if still mounted. os.Stat alone is wrong here — the mount-point
			// directory exists whether or not it's mounted — so use isMounted,
			// which also catches a stale/half-detached FUSE mount (ENOTCONN).
			if isMounted(b.config.MountPath) {
				b.logger.Warn().Msg("FUSE filesystem still mounted, attempting force unmount")
				b.forceUnmount(ctx)
			}

			close(done)
		}()

		// Wait for unmount to complete or context timeout
		select {
		case <-done:
			b.logger.Info().Msg("Filesystem unmounted successfully")
		case <-ctx.Done():
			b.logger.Warn().Err(ctx.Err()).Msg("Unmount timed out, forcing unmount")
			b.forceUnmount(ctx)
		}
	}

	b.unmountFunc = umount
	b.ready.Store(true)
	return nil
}

// Unmount unmounts the filesystem
func (b *Backend) Unmount(ctx context.Context) error {
	b.logger.Info().Msg("Unmounting hanwen backend")
	if b.unmountFunc != nil {
		b.unmountFunc(ctx)
	} else {
		// Use force unmount
		b.forceUnmount(ctx)
	}

	// Close VFS manager
	if b.vfs != nil {
		if err := b.vfs.Close(); err != nil {
			b.logger.Warn().Err(err).Msg("Failed to close VFS")
		}
	}
	return nil
}

// WaitReady waits for the mount to be ready
func (b *Backend) WaitReady(ctx context.Context) error {
	if b.server == nil {
		return fmt.Errorf("server not initialized")
	}
	return b.server.WaitMount()
}

// IsReady returns true if the mount is ready
func (b *Backend) IsReady() bool {
	return b.ready.Load()
}

// Type returns the backend type
func (b *Backend) Type() backend.Type {
	return backend.Hanwen
}

func (b *Backend) Refresh(dir string) {
	// Refresh the root dir first
	if b.root != nil {
		b.root.Refresh()
		if dir != "" {
			b.root.RefreshChild(dir)
		}
	}
}

// forceUnmount attempts to force unmount a path using system commands
func (b *Backend) forceUnmount(ctx context.Context) {
	if !isMounted(b.config.MountPath) {
		return
	}
	methods := [][]string{
		{"umount", b.config.MountPath},
		{"umount", "-l", b.config.MountPath}, // lazy unmount
		{"fusermount", "-uz", b.config.MountPath},
		{"fusermount3", "-uz", b.config.MountPath},
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for _, method := range methods {
		// Ignore the command's exit code and verify the actual mount state
		// instead: `umount -l` returns 0 but detaches lazily, so trusting the
		// exit code would wrongly declare success while the mount lingers.
		_ = b.tryUnmountCommand(ctx, method...)
		if !isMounted(b.config.MountPath) {
			return
		}
		if ctx.Err() != nil {
			b.logger.Warn().Err(ctx.Err()).Msg("Unmount command timed out")
			return
		}
	}
}

// isMounted reports whether path is currently a mount point. It compares the
// device of path against its parent (a differing device means path is a mount
// point) and treats ENOTCONN — a stale/half-detached FUSE mount left by a
// crashed or still-exiting previous process — as still-mounted so it is cleared.
func isMounted(path string) bool {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return errors.Is(err, syscall.ENOTCONN)
	}
	var parent syscall.Stat_t
	if err := syscall.Stat(filepath.Dir(path), &parent); err != nil {
		return false
	}
	return st.Dev != parent.Dev
}

// tryUnmountCommand tries to run an unmount command
func (b *Backend) tryUnmountCommand(ctx context.Context, args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	return cmd.Run()
}
