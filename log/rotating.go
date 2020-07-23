package log

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"time"
)

const MaxBytesChan = 1024

type WriteFlushCloser interface {
	io.WriteCloser
	Flush() error
}

type RotatingOuter struct {
	nBytes int // The number of bytes written to this file
	writer WriteFlushCloser

	bytesChan   chan []byte
	fileName    string
	maxBytes    int
	backupCount int
	bufferSize  int
}

type FileFlush struct {
	fd *os.File
}

func (f *FileFlush) Write(p []byte) (int, error) {
	return f.fd.Write(p)
}

func (f *FileFlush) Flush() error {
	return nil
	//TODO: sync?
	//return f.fd.Sync()
}

func (f *FileFlush) Close() error {
	return f.fd.Close()
}

type BufferWriter struct {
	fd     *os.File
	writer *bufio.Writer
}

func (w *BufferWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *BufferWriter) Flush() error {
	return w.writer.Flush()
}

func (w *BufferWriter) Close() error {
	w.writer.Flush()
	return w.fd.Close()
}

func NewBufferWriter(fd *os.File, size int) WriteFlushCloser {
	if size <= 0 {
		return &FileFlush{fd: fd}
	}
	writer := bufio.NewWriterSize(fd, size)
	return &BufferWriter{
		fd:     fd,
		writer: writer,
	}
}

func NewRotatingOuter(fileName string, bufferSize int, maxBytes int, backupCount int) (*RotatingOuter, error) {
	dir := path.Dir(fileName)
	os.MkdirAll(dir, 0777)

	h := new(RotatingOuter)

	if maxBytes <= 0 {
		return nil, fmt.Errorf("invalid maxBytes")
	}

	if bufferSize > 10*1024*1024 || bufferSize < 0 {
		return nil, fmt.Errorf("invalid bufferSize")
	}

	if backupCount > 20 || backupCount < 0 {
		return nil, fmt.Errorf("invalid backupCount")
	}

	h.fileName = fileName
	h.maxBytes = maxBytes
	h.backupCount = backupCount
	h.bufferSize = bufferSize

	if err := h.openFile(); err != nil {
		return nil, err
	}

	h.bytesChan = make(chan []byte, MaxBytesChan)

	go h.writeLoop()
	return h, nil
}

func (h *RotatingOuter) writeLoop() {
	var err error
	flushTicker := time.NewTicker(30 * time.Second)

	for {
		select {
		case bytes, ok := <-h.bytesChan:
			if !ok {
				fmt.Println("write done...")
				return
			}

			if bytes == nil {
				fmt.Println("write done...")
				return
			}

			_, err = h.syncWrite(bytes)
			if err != nil {
				fmt.Printf("write error, %s\n", err)
			}
		case <-flushTicker.C:
			h.writer.Flush()
		}
	}
}

func (h *RotatingOuter) asyncPut(p []byte) {
	select {
	case h.bytesChan <- p:
	default:
	}
}

func (h *RotatingOuter) Write(p []byte) (int, error) {
	h.asyncPut(p)
	return len(p), nil
}

func (h *RotatingOuter) Close() error {
	fmt.Printf("log file %s close\n", h.fileName)
	h.bytesChan <- nil // send nil to channel
	err := h.writer.Close()
	if err != nil {
		fmt.Printf("log file %s close, %s\n", h.fileName, err)
	}
	return err
}

func (h *RotatingOuter) openFile() error {
	fd, err := os.OpenFile(h.fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	h.nBytes = 0
	if err != nil {
		return err
	}
	f, err := fd.Stat()
	if err != nil {
		return err
	}
	h.nBytes = int(f.Size())
	h.writer = NewBufferWriter(fd, h.bufferSize)
	return nil
}

func (h *RotatingOuter) syncWrite(p []byte) (n int, err error) {
	if h.nBytes+len(p) >= h.maxBytes {
		h.rotate()
	}
	n, err = h.writer.Write(p)
	h.nBytes += n
	return n, err
}

func (h *RotatingOuter) rotate() {
	if h.backupCount > 1 {
		h.writer.Close()

		for i := h.backupCount - 1; i > 0; i-- {
			sfn := fmt.Sprintf("%s.%d", h.fileName, i)
			dfn := fmt.Sprintf("%s.%d", h.fileName, i+1)

			os.Rename(sfn, dfn)
		}

		dfn := fmt.Sprintf("%s.1", h.fileName)
		os.Rename(h.fileName, dfn)

		err := h.openFile()
		if err != nil {
			fmt.Printf("open file err: %s\n", err)
		}
	}
}
