package mmcmap

import "math"
import "os"
import "sync/atomic"

import "github.com/sirgallo/utils"

import "github.com/sirgallo/mmcmap/common/mmap"


//============================================= MMCMap


// Open initializes a new mmcmap
//	This will create the memory mapped file or read it in if it already exists.
//	Then, the meta data is initialized and written to the first 0-23 bytes in the memory map.
//	An initial root MMCMapNode will also be written to the memory map as well.
func Open(opts MMCMapOpts) (*MMCMap, error) {
	bitChunkSize := 5
	hashChunks := int(math.Pow(float64(2), float64(bitChunkSize))) / bitChunkSize

	// np := NewMMCMapNodePool(10000)

	mmcMap := &MMCMap{
		BitChunkSize: bitChunkSize,
		HashChunks: hashChunks,
		Opened: true,
		SignalResize: make(chan bool),
		SignalFlush: make(chan bool),
		// nodePool: np,
	}

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	var openFileErr error

	mmcMap.File, openFileErr = os.OpenFile(opts.Filepath, flag, 0600)
	if openFileErr != nil { return nil, openFileErr	}

	mmcMap.Filepath = mmcMap.File.Name()
	atomic.StoreUint32(&mmcMap.IsResizing, 0)
	mmcMap.Data.Store(mmap.MMap{})

	initFileErr := mmcMap.initializeFile()
	if initFileErr != nil { return nil, initFileErr	}

	go mmcMap.handleFlush()
	go mmcMap.handleResize()

	return mmcMap, nil
}

// Close
//	Close the mmcmap, unmapping the file from memory and closing the file.
func (mmcMap *MMCMap) Close() error {
	if ! mmcMap.Opened { return nil }
	mmcMap.Opened = false

	flushErr := mmcMap.File.Sync()
	if flushErr != nil { return flushErr }

	unmapErr := mmcMap.munmap()
	if unmapErr != nil { return unmapErr }

	if mmcMap.File != nil {
		closeErr := mmcMap.File.Close()
		if closeErr != nil { return closeErr }
	}

	mmcMap.Filepath = utils.GetZero[string]()
	return nil
}

// FileSize
//	Determine the memory mapped file size.
func (mmcMap *MMCMap) FileSize() (int, error) {
	stat, statErr := mmcMap.File.Stat()
	if statErr != nil { return 0, statErr }

	size := int(stat.Size())
	return size, nil
}

// Remove
//	Close the MMCMap and remove the source file.
func (mmcMap *MMCMap) Remove() error {
	closeErr := mmcMap.Close()
	if closeErr != nil { return closeErr }

	removeErr := os.Remove(mmcMap.File.Name())
	if removeErr != nil { return removeErr }

	return nil
}

// InitializeFile
//	Initialize the memory mapped file to persist the hamt.
//	If file size is 0, initiliaze the file size to 64MB and set the initial metadata and root values into the map.
//	Otherwise, just map the already initialized file into the memory map.
func (mmcMap *MMCMap) initializeFile() error {
	fSize, fSizeErr := mmcMap.FileSize()
	if fSizeErr != nil { return fSizeErr }

	if fSize == 0 {
		_, resizeErr := mmcMap.resizeMmap()
		if resizeErr != nil { return resizeErr }

		endOffset, initRootErr := mmcMap.initRoot()
		if initRootErr != nil { return initRootErr }

		initMetaErr := mmcMap.initMeta(endOffset)
		if initMetaErr != nil { return initMetaErr }
	} else {
		mmapErr := mmcMap.mMap()
		if mmapErr != nil { return mmapErr }
	}

	return nil
}