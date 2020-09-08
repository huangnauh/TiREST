package log

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/huangnauh/tirest/utils"
)

const MaxBytesChan = 1024

type WriteFlushCloser interface {
	io.WriteCloser
	Flush() error
}

type RotatingOuter struct {
	nBytes int // The number of bytes written to this file
	writer WriteFlushCloser

	bufChan     chan *bytes.Buffer
	fileName    string
	maxBytes    int
	backupCount int
	bufferSize  int
	format      logrus.Formatter
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

func NewRotatingOuter(fileName string, bufferSize int, maxBytes int, backupCount int,
	format logrus.Formatter) (*RotatingOuter, error) {
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
	h.format = format

	if err := h.openFile(); err != nil {
		return nil, err
	}

	h.bufChan = make(chan *bytes.Buffer, MaxBytesChan)

	go h.writeLoop()
	return h, nil
}

func (h *RotatingOuter) writeLoop() {
	var err error
	flushTicker := time.NewTicker(30 * time.Second)

	for {
		select {
		case buf, ok := <-h.bufChan:
			if !ok {
				fmt.Println("write done...")
				return
			}

			if buf == nil {
				fmt.Println("write done...")
				return
			}

			data := buf.Bytes()
			_, err = h.syncWrite(data)
			if err != nil {
				fmt.Printf("write error, %s\n", err)
			}
			utils.PutBuf(buf)
		case <-flushTicker.C:
			h.writer.Flush()
		}
	}
}

func (h *RotatingOuter) asyncPut(buf *bytes.Buffer) {
	select {
	case h.bufChan <- buf:
	default:
	}
}

func (h *RotatingOuter) WriteBuffer(buf *bytes.Buffer) (int, error) {
	h.asyncPut(buf)
	return buf.Len(), nil
}

func (h *RotatingOuter) Close() error {
	fmt.Printf("log file %s close\n", h.fileName)
	h.bufChan <- nil // send nil to channel
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

// logrus hook
func (h *RotatingOuter) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *RotatingOuter) Fire(entry *logrus.Entry) error {
	//Notice: PutBuf in writeLoop
	originBuffer := entry.Buffer
	entry.Buffer = utils.GetBuf()
	_, err := h.format.Format(entry)
	if err != nil {
		return err
	}
	_, err = h.WriteBuffer(entry.Buffer)
	entry.Buffer = originBuffer
	return err
}
