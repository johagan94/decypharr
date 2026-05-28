package hybrid

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
)

// Log format:
// [Header: 16 bytes]
//   - Magic: 4 bytes "HYBR"
//   - Version: 4 bytes
//   - Reserved: 8 bytes
//
// [Record: variable]
//   - KeyLen: 4 bytes
//   - Key: KeyLen bytes
//   - ValueLen: 4 bytes
//   - Value: ValueLen bytes
//   - Flags: 1 byte (bit 0 = deleted)
//   - CategoryLen: 2 bytes
//   - Category: CategoryLen bytes
//   - ProviderLen: 2 bytes
//   - Provider: ProviderLen bytes
//   - StatusLen: 2 bytes
//   - Status: StatusLen bytes
//   - NameLen: 2 bytes
//   - Name: NameLen bytes
//   - TotalSize: 8 bytes
//   - Checksum: 4 bytes (CRC32)

const (
	logMagic      = "HYBR"
	logVersion    = uint32(3) // v3: added Protocol, Bad, AddedOn
	logHeaderSize = 16
)

// LogRecord represents a single record in the log
type LogRecord struct {
	Key       string
	Offset    int64 // Offset to value data in file
	Size      int32 // Size of value data
	Deleted   bool
	Category  string
	Provider  string
	Status    string
	Name      string
	TotalSize int64
	Protocol  string // "torrent" or "nzb"
	Bad       bool
	AddedOn   int64 // Unix timestamp
}

// appendLog is an append-only log file
type appendLog struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	writePos int64
	version  uint32 // File format version (for backward compatibility)
	logger   zerolog.Logger
}

// openAppendLog opens an existing log or creates a new one
func openAppendLog(path string) (*appendLog, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	log := &appendLog{
		file:    file,
		path:    path,
		version: logVersion, // Default to current version for new files
		logger:  logger.New("hybrid-log"),
	}

	if info.Size() == 0 {
		// New file - write header
		if err := log.writeHeader(); err != nil {
			file.Close()
			return nil, err
		}
		log.writePos = logHeaderSize
	} else {
		// Existing file - validate header and find write position
		version, err := log.validateHeader()
		if err != nil {
			file.Close()
			return nil, err
		}
		log.version = version
		log.writePos = info.Size()
	}

	return log, nil
}

// createAppendLog creates a new log file (always fresh)
func createAppendLog(path string) (*appendLog, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	log := &appendLog{
		file:    file,
		path:    path,
		version: logVersion,
		logger:  logger.New("hybrid-log"),
	}

	if err := log.writeHeader(); err != nil {
		file.Close()
		return nil, err
	}
	log.writePos = logHeaderSize

	return log, nil
}

func (l *appendLog) writeHeader() error {
	header := make([]byte, logHeaderSize)
	copy(header[0:4], logMagic)
	binary.LittleEndian.PutUint32(header[4:8], logVersion)
	// bytes 8-16 reserved

	_, err := l.file.WriteAt(header, 0)
	return err
}

func (l *appendLog) validateHeader() (uint32, error) {
	header := make([]byte, logHeaderSize)
	if _, err := l.file.ReadAt(header, 0); err != nil {
		return 0, fmt.Errorf("failed to read header: %w", err)
	}

	if string(header[0:4]) != logMagic {
		return 0, fmt.Errorf("invalid magic: expected %s, got %s", logMagic, string(header[0:4]))
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version > logVersion {
		return 0, fmt.Errorf("unsupported version: %d (max: %d)", version, logVersion)
	}

	return version, nil
}

// Append writes a record to the log and returns the offset and size of the value
func (l *appendLog) Append(key string, value []byte, deleted bool, category, provider, status, name string, totalSize int64, protocol string, bad bool, addedOn int64) (offset int64, size int32, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	keyBytes := []byte(key)
	catBytes := []byte(category)
	provBytes := []byte(provider)
	statusBytes := []byte(status)
	nameBytes := []byte(name)
	protocolBytes := []byte(protocol)

	// Calculate total record size
	recordSize := 4 + len(keyBytes) + // keyLen + key
		4 + len(value) + // valueLen + value
		1 + // flags (bit 0 = deleted, bit 1 = bad)
		2 + len(catBytes) + // categoryLen + category
		2 + len(provBytes) + // providerLen + provider
		2 + len(statusBytes) + // statusLen + status
		2 + len(nameBytes) + // nameLen + name
		8 + // totalSize
		2 + len(protocolBytes) + // protocolLen + protocol
		8 // addedOn

	buf := make([]byte, recordSize)
	pos := 0

	// Key
	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(keyBytes)))
	pos += 4
	copy(buf[pos:], keyBytes)
	pos += len(keyBytes)

	// Value
	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(value)))
	pos += 4
	valueOffset := l.writePos + int64(pos)
	copy(buf[pos:], value)
	pos += len(value)

	// Flags (bit 0 = deleted, bit 1 = bad)
	var flags byte
	if deleted {
		flags |= 1
	}
	if bad {
		flags |= 2
	}
	buf[pos] = flags
	pos++

	// Category
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(catBytes)))
	pos += 2
	copy(buf[pos:], catBytes)
	pos += len(catBytes)

	// Provider
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(provBytes)))
	pos += 2
	copy(buf[pos:], provBytes)
	pos += len(provBytes)

	// Status
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(statusBytes)))
	pos += 2
	copy(buf[pos:], statusBytes)
	pos += len(statusBytes)

	// Name
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(nameBytes)))
	pos += 2
	copy(buf[pos:], nameBytes)
	pos += len(nameBytes)

	// TotalSize
	binary.LittleEndian.PutUint64(buf[pos:], uint64(totalSize))
	pos += 8

	// Protocol
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(protocolBytes)))
	pos += 2
	copy(buf[pos:], protocolBytes)
	pos += len(protocolBytes)

	// AddedOn
	binary.LittleEndian.PutUint64(buf[pos:], uint64(addedOn))
	pos += 8

	// Write to file
	if _, err := l.file.WriteAt(buf, l.writePos); err != nil {
		return 0, 0, err
	}

	l.writePos += int64(recordSize)

	return valueOffset, int32(len(value)), nil
}

// ReadAt reads value data at the given offset
func (l *appendLog) ReadAt(offset int64, size int32) ([]byte, error) {
	buf := make([]byte, size)
	_, err := l.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// Iterate scans the log and calls fn for each record.
//
// Recovery is torn-write-safe: if a record cannot be read (a partial write left
// by a crash, or an implausible length field), the position is treated as the
// end of the valid log. The trailing garbage is truncated and iteration stops,
// rather than failing recovery and bricking the whole store. This mirrors the
// standard recovery behaviour of Bitcask/LevelDB-style append logs.
func (l *appendLog) Iterate(fn func(*LogRecord) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	pos := int64(logHeaderSize)
	fileSize := l.writePos

	for pos < fileSize {
		record, nextPos, err := l.readRecordAt(pos)
		if err != nil {
			// Torn/corrupt tail: drop everything from pos onward so the store
			// opens cleanly with all records that were fully written.
			if truncErr := l.file.Truncate(pos); truncErr != nil {
				l.logger.Error().Err(truncErr).Int64("pos", pos).
					Msg("hybrid log: failed to truncate torn tail during recovery")
			} else {
				l.writePos = pos
			}
			l.logger.Warn().Err(err).
				Int64("truncated_at", pos).Int64("prev_size", fileSize).
				Msg("hybrid log: torn/corrupt tail on recovery; truncated to last valid record")
			break
		}

		if err := fn(record); err != nil {
			return err
		}

		pos = nextPos
	}

	return nil
}

func (l *appendLog) readRecordAt(pos int64) (*LogRecord, int64, error) {
	// Read key length
	lenBuf := make([]byte, 4)
	if _, err := l.file.ReadAt(lenBuf, pos); err != nil {
		return nil, 0, err
	}
	keyLen := binary.LittleEndian.Uint32(lenBuf)
	if keyLen > 1024*1024 { // 1MB sanity check
		return nil, 0, fmt.Errorf("invalid key length: %d", keyLen)
	}

	// Read key
	keyBuf := make([]byte, keyLen)
	if _, err := l.file.ReadAt(keyBuf, pos+4); err != nil {
		return nil, 0, err
	}

	curPos := pos + 4 + int64(keyLen)

	// Read value length
	if _, err := l.file.ReadAt(lenBuf, curPos); err != nil {
		return nil, 0, err
	}
	valueLen := binary.LittleEndian.Uint32(lenBuf)
	valueOffset := curPos + 4

	curPos = valueOffset + int64(valueLen)

	// Read flags (bit 0 = deleted, bit 1 = bad in v3+)
	flagBuf := make([]byte, 1)
	if _, err := l.file.ReadAt(flagBuf, curPos); err != nil {
		return nil, 0, err
	}
	deleted := (flagBuf[0] & 1) != 0
	// Bad flag only valid in v3+, ignore for older versions
	bad := l.version >= 3 && (flagBuf[0]&2) != 0
	curPos++

	// Read category
	lenBuf2 := make([]byte, 2)
	if _, err := l.file.ReadAt(lenBuf2, curPos); err != nil {
		return nil, 0, err
	}
	catLen := binary.LittleEndian.Uint16(lenBuf2)
	curPos += 2

	catBuf := make([]byte, catLen)
	if catLen > 0 {
		if _, err := l.file.ReadAt(catBuf, curPos); err != nil {
			return nil, 0, err
		}
	}
	curPos += int64(catLen)

	// Read provider
	if _, err := l.file.ReadAt(lenBuf2, curPos); err != nil {
		return nil, 0, err
	}
	provLen := binary.LittleEndian.Uint16(lenBuf2)
	curPos += 2

	provBuf := make([]byte, provLen)
	if provLen > 0 {
		if _, err := l.file.ReadAt(provBuf, curPos); err != nil {
			return nil, 0, err
		}
	}
	curPos += int64(provLen)

	// Read status
	if _, err := l.file.ReadAt(lenBuf2, curPos); err != nil {
		return nil, 0, err
	}
	statusLen := binary.LittleEndian.Uint16(lenBuf2)
	curPos += 2

	statusBuf := make([]byte, statusLen)
	if statusLen > 0 {
		if _, err := l.file.ReadAt(statusBuf, curPos); err != nil {
			return nil, 0, err
		}
	}
	curPos += int64(statusLen)

	// Read name
	if _, err := l.file.ReadAt(lenBuf2, curPos); err != nil {
		return nil, 0, err
	}
	nameLen := binary.LittleEndian.Uint16(lenBuf2)
	curPos += 2

	nameBuf := make([]byte, nameLen)
	if nameLen > 0 {
		if _, err := l.file.ReadAt(nameBuf, curPos); err != nil {
			return nil, 0, err
		}
	}
	curPos += int64(nameLen)

	// Read totalSize
	sizeBuf := make([]byte, 8)
	if _, err := l.file.ReadAt(sizeBuf, curPos); err != nil {
		return nil, 0, err
	}
	totalSize := int64(binary.LittleEndian.Uint64(sizeBuf))
	curPos += 8

	// Read v3+ fields (protocol, addedOn) only if version >= 3
	var protocol string
	var addedOn int64
	if l.version >= 3 {
		// Read protocol
		if _, err := l.file.ReadAt(lenBuf2, curPos); err != nil {
			return nil, 0, err
		}
		protocolLen := binary.LittleEndian.Uint16(lenBuf2)
		curPos += 2
		if protocolLen > 0 {
			protocolBuf := make([]byte, protocolLen)
			if _, err := l.file.ReadAt(protocolBuf, curPos); err != nil {
				return nil, 0, err
			}
			protocol = string(protocolBuf)
			curPos += int64(protocolLen)
		}

		// Read addedOn
		if _, err := l.file.ReadAt(sizeBuf, curPos); err != nil {
			return nil, 0, err
		}
		addedOn = int64(binary.LittleEndian.Uint64(sizeBuf))
		curPos += 8
	}

	record := &LogRecord{
		Key:       string(keyBuf),
		Offset:    valueOffset,
		Size:      int32(valueLen),
		Deleted:   deleted,
		Category:  string(catBuf),
		Provider:  string(provBuf),
		Status:    string(statusBuf),
		Name:      string(nameBuf),
		TotalSize: totalSize,
		Protocol:  protocol,
		Bad:       bad,
		AddedOn:   addedOn,
	}

	return record, curPos, nil
}

// Sync flushes data to disk
func (l *appendLog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Sync()
}

// Close closes the log file
func (l *appendLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

// Size returns the current file size
func (l *appendLog) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writePos
}

// Truncate truncates the log at the given position (for recovery)
func (l *appendLog) Truncate(pos int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.file.Truncate(pos); err != nil {
		return err
	}
	l.writePos = pos
	return nil
}
