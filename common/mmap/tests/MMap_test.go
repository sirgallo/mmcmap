package mmaptests

import "bytes"
import "io"
import "os"
import "path/filepath"
import "testing"

import "github.com/sirgallo/pcmap/common/mmap"


var TestData = []byte("0123456789ABCDEF")
var TestPath = filepath.Join(os.TempDir(), "testfile")

func init() {
	testFile := openFile(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	testFile.Write(TestData)
	testFile.Close()
}

func openFile(flags int) *os.File {
	file, openErr := os.OpenFile(TestPath, flags, 0644)
	if openErr != nil { panic(openErr.Error()) }

	return file
}

func TestUnmap(t *testing.T) {
	testFile := openFile(os.O_RDONLY)
	defer testFile.Close()

	mMap, mmapErr := mmap.Map(testFile, mmap.RDONLY, 0)
	if mmapErr != nil { t.Errorf("error mapping: %s", mmapErr) }

	unmapErr := mMap.Unmap() 
	if unmapErr != nil { t.Errorf("mmap != testData: %q, %q", mMap, TestData) }

}

func TestReadWrite(t *testing.T) {
	testFile := openFile(os.O_RDWR)
	defer testFile.Close()
	
	mMap, mmapErr := mmap.Map(testFile, mmap.RDWR, 0)
	if mmapErr != nil { t.Errorf("error mapping: %s", mmapErr) }

	defer mMap.Unmap()
	
	if ! bytes.Equal(TestData, mMap) {
		t.Errorf("mmap != testData: %q, %q", mMap, TestData)
	}

	mMap[9] = 'X'
	mMap.Flush()

	fileData, err := io.ReadAll(testFile)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	if !bytes.Equal(fileData, []byte("012345678XABCDEF")) {
		t.Errorf("file wasn't modified")
	}

	// leave things how we found them
	mMap[9] = '9'
	mMap.Flush()
}

