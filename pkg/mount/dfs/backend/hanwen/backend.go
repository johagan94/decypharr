//go:build linux || (darwin && amd64)

package hanwen

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync/atomic"
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

	entryTimeout := EntryTimeout
	attrTimeout := AttrTimeout
	opts := &fs.Options{
		AttrTimeout:  &attrTimeout,
		EntryTimeout: &entryTimeout,
		MountOptions: mountOpt,
		UID:          b.config.UID,
		GID:          b.config.GID,
	}

	// Retry loop: zombie FUSE mounts from OOM crashes can block a single
	// forceUnmount attempt. Retry up to 3 times with exponential backoff
	// (2 s, 4 s) so the process never starts silently without a DFS mount.
	type fsResult struct {
		server *fuse.Server
		err    error
	}
	const maxMountAttempts = 3
	var (
		server  *fuse.Server
		lastErr error
	)
	for attempt := 1; attempt <= maxMountAttempts; attempt++ {
		if attempt > 1 {
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			b.logger.Warn().
				Int("attempt", attempt).
				Dur("backoff", backoff).
				Err(lastErr).
				Msg("Mount failed, retrying after backoff")
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return fmt.Errorf("context canceled before mount retry: %w", ctx.Err())
			}
		}
		b.forceUnmount(ctx)

		mountCtx, cancel := context.WithTimeout(ctx, b.config.DaemonTimeout)
		fsResultChan := make(chan fsResult, 1)
		go func() {
			s, err := fs.Mount(b.config.MountPath, b.root, opts)
			fsResultChan <- fsResult{server: s, err: err}
		}()

		var mountResult fsResult
		select {
		case mountResult = <-fsResultChan:
		case <-mountCtx.Done():
			cancel()
			lastErr = fmt.Errorf("attempt %d: timeout creating mount: %w", attempt, mountCtx.Err())
			continue
		}
		if mountResult.err != nil {
			cancel()
			lastErr = fmt.Errorf("attempt %d: failed to create mount: %w", attempt, mountResult.err)
			continue
		}

		b.logger.Info().
			Str("mount_path", b.config.MountPath).
			Int("attempt", attempt).
			Msg("Waiting for mount to be ready")

		waitChan := make(chan error, 1)
		go func() { waitChan <- mountResult.server.WaitMount() }()
		select {
		case err := <-waitChan:
			if err != nil {
				_ = mountResult.server.Unmount()
				cancel()
				lastErr = fmt.Errorf("attempt %d: failed to wait for mount: %w", attempt, err)
				continue
			}
		case <-mountCtx.Done():
			_ = mountResult.server.Unmount()
			cancel()
			lastErr = fmt.Errorf("attempt %d: timeout waiting for mount: %w", attempt, mountCtx.Err())
			continue
		}
		cancel()
		server = mountResult.server
		lastErr = nil
		break
	}
	if lastErr != nil {
		b.ready.Store(false)
		b.logger.Error().Err(lastErr).
			Int("attempts", maxMountAttempts).
			Str("mount_path", b.config.MountPath).
			Msg("DFS failed to mount after all retries — streaming will not work")
		return lastErr
	}
	b.server = server

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

			// Check if still mounted
			if _, err := os.Stat(b.config.MountPath); err == nil {
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
	methods := [][]string{
		{"umount", b.config.MountPath},
		{"umount", "-l", b.config.MountPath}, // lazy unmount
		{"fusermount", "-uz", b.config.MountPath},
		{"fusermount3", "-uz", b.config.MountPath},
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for _, method := range methods {
		if err := b.tryUnmountCommand(ctx, method...); err == nil {
			return
		}
		if ctx.Err() != nil {
			b.logger.Warn().Err(ctx.Err()).Msg("Unmount command timed out")
			return
		}
	}
}

// tryUnmountCommand tries to run an unmount command
func (b *Backend) tryUnmountCommand(ctx context.Context, args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	return cmd.Run()
}
