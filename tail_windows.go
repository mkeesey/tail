//go:build windows

package tail

import (
	"fmt"
	"io/fs"
	"os"

	"github.com/tenebris-tech/tail/winfile"
	"golang.org/x/sys/windows"
)

func OpenFile(name string) (file *os.File, fileIdentifier fs.FileInfo, err error) {
	file, err = winfile.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, nil, err
	}

	fileinfo, err := file.Stat()
	return file, fileinfo, err
}

func FileIdentifier(file *os.File) (string, error) {
	handle := windows.Handle(file.Fd())
	var data windows.ByHandleFileInformation
	err := windows.GetFileInformationByHandle(handle, &data)
	if err != nil {
		return "", err
	}
	// On Windows, files are identified by a combination of the volume serial number and the file index.
	// See os.SameFile
	return fmt.Sprintf("%d:%d:%d", data.VolumeSerialNumber, data.FileIndexHigh, data.FileIndexLow), nil
}
