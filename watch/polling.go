// Copyright (c) 2015 HPE Software Inc. All rights reserved.
// Copyright (c) 2013 ActiveState Software Inc. All rights reserved.

package watch

import (
	"io/fs"
	"os"
	"runtime"
	"time"

	"gopkg.in/tomb.v1"
)

// PollingFileWatcher polls the file for changes.
type PollingFileWatcher struct {
	Filename string
	Size     int64
}

func NewPollingFileWatcher(filename string) *PollingFileWatcher {
	fw := &PollingFileWatcher{filename, 0}
	return fw
}

var POLL_DURATION time.Duration

func (fw *PollingFileWatcher) BlockUntilExists(t *tomb.Tomb) error {
	for {
		if _, err := os.Stat(fw.Filename); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-time.After(POLL_DURATION):
			continue
		case <-t.Dying():
			return tomb.ErrDying
		}
	}
}

func (fw *PollingFileWatcher) BlockUntilEvent(t *tomb.Tomb, openedFileInfo fs.FileInfo, pos int64) (ChangeType, error) {
	for {
		changeType, err := StatChanges(openedFileInfo, pos)
		if err != nil {
			return None, err
		}
		if changeType != None {
			return changeType, nil
		}

		select {
		case <-time.After(POLL_DURATION):
			continue
		case <-t.Dying():
			return None, tomb.ErrDying
		}
	}
}

func StatChanges(openedFileInfo fs.FileInfo, pos int64) (ChangeType, error) {
	fi, err := os.Stat(openedFileInfo.Name())
	if err != nil {
		// Windows cannot delete a file if a handle is still open (tail keeps one open)
		// so it gives access denied to anything trying to read it until all handles are released.
		if os.IsNotExist(err) || (runtime.GOOS == "windows" && os.IsPermission(err)) {
			return Deleted, nil
		}
		return None, err
	}

	if !os.SameFile(openedFileInfo, fi) {
		return Deleted, nil
	}

	if fi.Size() > pos {
		return Modified, nil
	} else if fi.Size() < pos {
		return Truncated, nil
	}

	return None, nil
}

func init() {
	POLL_DURATION = 250 * time.Millisecond
}
