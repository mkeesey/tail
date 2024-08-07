// Copyright (c) Microsoft Corporation.

package tail

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

const MaxFiles = 11
const NumLines = 10001
const MaxSleepNs = 500000 // 0.5 ms
const MinSleepNS = 1000   // 0.001 ms

// Disable Library e2e tests unless we're digging into things.
const EnableE2ETests = true

func TestTail_Offsets(t *testing.T) {
	t.Run("Offsets without FileIdentifier", func(t *testing.T) {
		testFile, f := testFile(t)
		defer f.Close()
		f.WriteString("hello\n") //6

		tailer, err := TailFile(testFile, Config{Follow: true, ReOpen: true})
		noError(t, err)
		line := <-tailer.Lines
		cleanTailer(tailer)

		eq(t, line.Text, "hello")
		eq(t, line.Offset, int64(6))

		f.WriteString("world\n") //6 + 6 = 12
		tailer, err = TailFile(testFile, Config{Follow: true, ReOpen: true, Location: &SeekInfo{Offset: line.Offset, Whence: 0}})
		noError(t, err)
		line = <-tailer.Lines
		cleanTailer(tailer)

		eq(t, line.Text, "world")
		eq(t, line.Offset, int64(12))
	})

	t.Run("Offsets with FileIdentifier", func(t *testing.T) {
		testFile, f := testFile(t)
		f.WriteString("hello\n") //6

		tailer, err := TailFile(testFile, Config{Follow: true, ReOpen: true})
		noError(t, err)
		line := <-tailer.Lines
		cleanTailer(tailer)

		eq(t, line.Text, "hello")
		eq(t, line.Offset, int64(6))

		// Old file removed, new file created
		f.Close()
		err = os.Rename(testFile, testFile+".1")
		noError(t, err)
		f, err = os.Create(testFile)
		noError(t, err)
		f.WriteString("world\n") //6

		tailer, err = TailFile(testFile, Config{Follow: true, ReOpen: true, Location: &SeekInfo{Offset: line.Offset, Whence: 0, FileIdentifier: line.FileIdentifier}})
		noError(t, err)
		line = <-tailer.Lines
		cleanTailer(tailer)

		// New file, so we did not seek. Offset is 6 again.
		eq(t, line.Text, "world")
		eq(t, line.Offset, int64(6))

		f.WriteString("again\n") //6 + 6 = 12
		tailer, err = TailFile(testFile, Config{Follow: true, ReOpen: true, Location: &SeekInfo{Offset: line.Offset, Whence: 0, FileIdentifier: line.FileIdentifier}})
		noError(t, err)
		line = <-tailer.Lines
		cleanTailer(tailer)

		eq(t, line.Text, "again")
		eq(t, line.Offset, int64(12))
	})
}

// Exercise the library against how files are rotated with kubernetes log drivers.
func TestTail_KubernetesLogDriver(t *testing.T) {
	if !EnableE2ETests || testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	testDir := t.TempDir()
	// testDir := "/tmp/tail_e2e_test"
	// os.MkdirAll(testDir, 0755)
	testFile := filepath.Join(testDir, "test.log")

	tailer, err := TailFile(testFile, Config{Follow: true, ReOpen: true, Poll: true})
	noError(t, err)
	defer tailer.Cleanup()
	defer tailer.Stop()

	group, ctx := errgroup.WithContext(context.Background())
	counterCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// Read lines from the tailer
	group.Go(func() error {
		count := 0
		for {
			select {
			case <-counterCtx.Done():
				return fmt.Errorf("tailer exited with count %d: %w", count, counterCtx.Err())
			case line, ok := <-tailer.Lines:
				if !ok {
					return fmt.Errorf("tailer closed the channel: exited with count %d", count)
				}
				if line.Err != nil {
					t.Logf("tailer error: %v", line.Err)
					//skip
					continue
				}
				number, err := strconv.Atoi(line.Text)
				noError(t, err)
				if number != count {
					t.Fatalf("expected %d, got %d", count, number)
				}
				count++
				if count == NumLines {
					return nil
				}
				// Add a bit of jitter to log reads
				pause(t)
			}
		}
	})

	// Write the log files
	group.Go(func() error {
		writeLogsToFiles(t, testFile)
		return nil
	})

	err = group.Wait()
	noError(t, err)
}

func TestNotifications(t *testing.T) {
	//t.Skip()
	testDir := "/tmp/tail_e2e_test"
	os.MkdirAll(testDir, 0755)
	//os.RemoveAll(testDir + "/")
	testFile := filepath.Join(testDir, "test.log")

	writeLogsToFiles(t, testFile)
}

func pause(t *testing.T) {
	t.Helper()
	sleepTime, err := rand.Int(rand.Reader, big.NewInt(MaxSleepNs-MinSleepNS))
	//t.Logf("Sleeping for %d", sleepTime.Int64()+MinSleepNS)
	noError(t, err)
	time.Sleep(time.Duration(sleepTime.Int64()+MinSleepNS) * time.Nanosecond)
}

// writeLogsToFiles simulates a log driver that does the following operations:
// 1. Opens/Truncates an initial file
// 2. Writes to this file until it reaches a max size (or max number of lines for this test)
// 3. Shifts all old files by one and renames the file to <filename>.1
// 4. Compresses the file that was just rotated
// 5. Opens the same file with truncation mode and writes to it
func writeLogsToFiles(t *testing.T, filename string) {
	t.Helper()

	writeDir := filepath.Join(filepath.Dir(filename), "write")
	err := os.MkdirAll(writeDir, 0755)
	noError(t, err)

	writeFilename := filepath.Join(writeDir, filepath.Base(filename))

	f, err := os.OpenFile(writeFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	noError(t, err)
	//noError(t, os.Symlink(writeFilename, filename)) TODO TODO TODO TODO
	os.Symlink(writeFilename, filename)
	for i := 0; i < NumLines; i++ {
		if i%1000 == 0 && i != 0 {
			fmt.Printf("Rotating file with log line num %d\n", i)
			f.Sync()
			f.Close()
			rotate(writeFilename, MaxFiles, true)

			compressFile(t, fmt.Sprintf("%s.1", writeFilename), time.Now())
			f, err = os.OpenFile(writeFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			noError(t, err)
		}
		_, err := f.WriteString(fmt.Sprintf("%d\n", i))
		noError(t, err)

		// Add a bit of jitter to log writes
		pause(t)
	}
	f.Close()
	t.Log(time.Now(), "Done writing logs")
}

func rotate(name string, maxFiles int, compress bool) error {
	if maxFiles < 2 {
		return nil
	}

	var extension string
	if compress {
		extension = ".gz"
	}

	lastFile := fmt.Sprintf("%s.%d%s", name, maxFiles-1, extension)
	err := os.Remove(lastFile)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error removing oldest log file: %w", err)
	}

	for i := maxFiles - 1; i > 1; i-- {
		toPath := name + "." + strconv.Itoa(i) + extension
		fromPath := name + "." + strconv.Itoa(i-1) + extension
		fmt.Printf("rename %s -> %s\n", fromPath, toPath)
		if err := os.Rename(fromPath, toPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	fmt.Printf("rename %s -> %s\n", name, name+".1")
	if err := os.Rename(name, name+".1"); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

type rotateFileMetadata struct {
	LastTime time.Time `json:"lastTime,omitempty"`
}

func compressFile(t *testing.T, fileName string, lastTimestamp time.Time) {
	t.Helper()
	file, err := os.Open(fileName)
	if err != nil {
		noError(t, err)
		return
	}
	defer func() {
		file.Close()
		err := os.Remove(fileName)
		if err != nil {
			noError(t, err)
		}
	}()

	outFile, err := os.OpenFile(fileName+".gz", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0640)
	if err != nil {
		noError(t, err)
		return
	}
	defer func() {
		outFile.Close()
		if err != nil {
			os.Remove(fileName + ".gz")
		}
	}()

	compressWriter := gzip.NewWriter(outFile)
	defer compressWriter.Close()

	// Add the last log entry timestamp to the gzip header
	extra := rotateFileMetadata{}
	extra.LastTime = lastTimestamp
	compressWriter.Header.Extra, err = json.Marshal(&extra)
	if err != nil {
		noError(t, err)
	}

	_, err = io.Copy(compressWriter, file)
	if err != nil {
		noError(t, err)
		return
	}
}

func testFile(t *testing.T) (string, *os.File) {
	t.Helper()
	testDir := t.TempDir()
	testFile := filepath.Join(testDir, "test.log")
	f, err := os.Create(testFile)
	noError(t, err)
	return testFile, f
}

func cleanTailer(tailer *Tail) {
	tailer.Cleanup()
	tailer.Stop()
}

func noError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func eq(t *testing.T, actual, expected any) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}
}
