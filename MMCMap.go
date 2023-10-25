package mmcmap

import "math"
import "os"
import "runtime"
import "sync"
import "sync/atomic"

import "github.com/sirgallo/logger"
import "github.com/sirgallo/utils"

import "github.com/sirgallo/mmcmap/common/mmap"


var cLog = logger.NewCustomLog("MMCMap")


//============================================= MMCMap


// Open initializes a new mmcmap
//	This will create the memory mapped file or read it in if it already exists.
//	Then, the meta data is initialized and written to the first 0-23 bytes in the memory map.
//	An initial root MMCMapNode will also be written to the memory map as well
//
// Parameters:
//	opts: a MMCMapOpts object for initializing the mem map
//
// Returns:
//	The newly initialized hash array mapped trie
func Open(opts MMCMapOpts) (*MMCMap, error) {
	bitChunkSize := 5
	hashChunks := int(math.Pow(float64(2), float64(bitChunkSize))) / bitChunkSize

	mmcMap := &MMCMap{
		BitChunkSize: bitChunkSize,
		HashChunks: hashChunks,
		Opened: true,
		SignalResize: make(chan uint64),
		FlushWG: sync.WaitGroup{},
	}

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	var openFileErr error

	mmcMap.File, openFileErr = os.OpenFile(opts.Filepath, flag, 0600)
	if openFileErr != nil { return nil, openFileErr	}

	mmcMap.Filepath = mmcMap.File.Name()
	atomic.StoreUint32(&mmcMap.IsResizing, 0)

	initFileErr := mmcMap.initializeFile()
	if initFileErr != nil { return nil, initFileErr	}

	go func() {
		for offset := range mmcMap.SignalResize {
			_, resizeErr := mmcMap.resizeMmap(offset)
			if resizeErr != nil { cLog.Error("error resizing:", resizeErr.Error()) }
		}
	}()

	go func() {
		for range mmcMap.SignalFlush {
			mmcMap.FlushWG.Add(1)
			
			func() {
				for atomic.LoadUint32(&mmcMap.IsResizing) == 1 { runtime.Gosched() }
				
				mmcMap.WriteResizeLock.RLock()
				defer mmcMap.WriteResizeLock.RUnlock()

				flushErr := mmcMap.File.Sync()
				if flushErr != nil { cLog.Error("error flushing to disk", flushErr.Error()) } 
			}()

			mmcMap.FlushWG.Done()
		}
	}()

	return mmcMap, nil
}

// Close
//	Close the mmcmap, unmapping the file from memory and closing the file.
//
// Returns:
//	Error if error unmapping and closing the file
func (mmcMap *MMCMap) Close() error {
	mmcMap.FlushWG.Wait()

	if ! mmcMap.Opened { return nil }
	mmcMap.Opened = false

	unmapErr := mmcMap.munmap()
	if unmapErr != nil {
		cLog.Error("error removing memory map:", unmapErr.Error())
		return unmapErr 
	}

	if mmcMap.File != nil {
		closeErr := mmcMap.File.Close()
		if closeErr != nil {
			cLog.Error("error closing file:", closeErr.Error())
			return closeErr 
		}
	}

	mmcMap.Filepath = utils.GetZero[string]()
	return nil
}

// FileSize
//	Determine the memory mapped file size.
//
// Returns:
//	The size in bytes, or an error.
func (mmcMap *MMCMap) FileSize() (int, error) {
	stat, statErr := mmcMap.File.Stat()
	if statErr != nil { return 0, statErr }

	size := int(stat.Size())
	return size, nil
}

// FlushRegionToDisk
//	Flushes a region of the memory map to disk instead of flushing the entire map. 
//	When a startoffset is provided, if it is not aligned with the start of the last page, the offset needs to be normalized.
//
// Parameters:
//	startOffset: the offset of the start of the region
//	endOffset: the end of the region
//
// Returns:
//	Error if flushing to disk fails
func (mmcMap *MMCMap) FlushRegionToDisk(startOffset, endOffset uint64) error {
	startOffsetOfPage := startOffset & ^(uint64(DefaultPageSize) - 1)

	mMap := mmcMap.Data.Load().(mmap.MMap)
	if len(mMap) == 0 { return nil }

	flushErr := mMap[startOffsetOfPage:endOffset].Flush()
	if flushErr != nil { return flushErr }

	return nil
}

// Remove
//	Close the MMCMap and remove the source file.
//
// Returns:
//	Error if operation fails
func (mmcMap *MMCMap) Remove() error {
	closeErr := mmcMap.Close()
	if closeErr != nil { return closeErr }

	removeErr := os.Remove(mmcMap.File.Name())
	if removeErr != nil {
		cLog.Error("error removing file:", removeErr.Error()) 
		return removeErr 
	}

	return nil
}

// InitializeFile
//	Initialize the memory mapped file to persist the hamt.
//	If file size is 0, initiliaze the file size to 64MB and set the initial metadata and root values into the map.
//	Otherwise, just map the already initialized file into the memory map
//
// Returns:
//	Error if the initialization fails
func (mmcMap *MMCMap) initializeFile() error {
	fSize, fSizeErr := mmcMap.FileSize()
	if fSizeErr != nil {
		cLog.Error("error getting file size:", fSizeErr.Error())
		return fSizeErr 
	}

	if fSize == 0 {
		cLog.Info("initializing memory map for the first time.")
		
		mmcMap.Data.Store(mmap.MMap{})
		_, resizeErr := mmcMap.resizeMmap(0)
		if resizeErr != nil {
			cLog.Error("error resizing memory map:", resizeErr.Error())
			return resizeErr 
		}

		endOffset, initRootErr := mmcMap.initRoot()
		if initRootErr != nil {
			cLog.Error("error initializing root version 0:", initRootErr.Error()) 
			return initRootErr 
		}

		initMetaErr := mmcMap.initMeta(endOffset)
		if initMetaErr != nil {
			cLog.Error("error initializing metadata:", initMetaErr.Error()) 
			return initMetaErr 
		}
	} else {
		cLog.Info("file already initialized, memory mapping.")
		
		mmapErr := mmcMap.mMap()
		if mmapErr != nil {
			cLog.Error("error initializing memory map:", mmapErr.Error()) 
			return mmapErr 
		}
	}

	return nil
}

// InitMeta
//	Initialize and serialize the metadata in a new MMCMap. Version starts at 0 and increments, and root offset starts at 16.
//
// Returns:
//	Error if initializing the meta data fails
func (mmcMap *MMCMap) initMeta(endRoot uint64) error {
	newMeta := &MMCMapMetaData{
		Version: 0,
		RootOffset: uint64(InitRootOffset),
		EndMmapOffset: endRoot,
	}

	serializedMeta := newMeta.SerializeMetaData()

	_, flushErr := mmcMap.WriteMetaToMemMap(serializedMeta)
	if flushErr != nil { return flushErr }
	
	return nil
}

// InitRoot
//	Initialize the Version 0 root where operations will begin traversing.
//
// Returns:
//	Error if initializing root and serializing the MMCMapNode fails.
func (mmcMap *MMCMap) initRoot() (uint64, error) {
	root := &MMCMapNode{
		Version: 0,
		StartOffset: uint64(InitRootOffset),
		Bitmap: 0,
		IsLeaf: false,
		KeyLength: uint16(0),
		Children: []*MMCMapNode{},
	}

	endOffset, writeNodeErr := mmcMap.WriteNodeToMemMap(root)
	if writeNodeErr != nil { return 0, writeNodeErr }

	return endOffset, nil
}

// ExclusiveWriteMmap
//	Takes a path copy and writes the nodes to the memory map, then updates the metadata.
//
// Returns:
//	True if success, error if failure.
func (mmcMap *MMCMap) exclusiveWriteMmap(path *MMCMapNode) (bool, error) {
	if atomic.LoadUint32(&mmcMap.IsResizing) == 1 { return false, nil }

	versionPtr, _ := mmcMap.LoadMetaVersionPointer()
	rootOffsetPtr, _ := mmcMap.LoadMetaRootOffsetPointer()
	endOffsetPtr, _ := mmcMap.LoadMetaEndMmapPointer()
	
	version := atomic.LoadUint64(versionPtr)
	endOffset := atomic.LoadUint64(endOffsetPtr)
	prevRootOffset := atomic.LoadUint64(rootOffsetPtr)

	newOffsetInMMap := endOffset + 1
	serializedPath, serializeErr := mmcMap.SerializePathToMemMap(path, newOffsetInMMap)
	if serializeErr != nil { return false, serializeErr }

	updatedMeta := &MMCMapMetaData{
		Version: path.Version,
		RootOffset: newOffsetInMMap,
		EndMmapOffset: newOffsetInMMap + uint64(len(serializedPath)),
	}

	isResize := mmcMap.determineIfResize(updatedMeta.EndMmapOffset)
	if isResize { return false, nil }

	if atomic.LoadUint32(&mmcMap.IsResizing) == 0 {
		if version == updatedMeta.Version - 1 && atomic.CompareAndSwapUint64(versionPtr, version, updatedMeta.Version) {
			atomic.StoreUint64(endOffsetPtr, updatedMeta.EndMmapOffset)

			_, writeNodesToMmapErr := mmcMap.writeNodesToMemMap(serializedPath, newOffsetInMMap)
			if writeNodesToMmapErr != nil {
				atomic.StoreUint64(rootOffsetPtr, prevRootOffset)
				atomic.StoreUint64(versionPtr, version)

				return false, writeNodesToMmapErr 
			}
			
			atomic.StoreUint64(rootOffsetPtr, updatedMeta.RootOffset)

			select {
				case mmcMap.SignalFlush <- true:
				default:
			}

			return true, nil
		}
	}

	return false, nil
}

// determineIfResize
//	Helper function that signals go routine for resizing if the condition to resize is met.
//
// Parameters:
//	offset: the offset to check against the memory map
//
// Returns:
//	True if already resizing, false if no resize is needed
func (mmcMap *MMCMap) determineIfResize(offset uint64) bool {
	mMap := mmcMap.Data.Load().(mmap.MMap)

	switch {
		case offset > 0 && int(offset) < len(mMap):
			return false
		case len(mMap) == 0 || ! atomic.CompareAndSwapUint32(&mmcMap.IsResizing, 0, 1):
			return true
		default:
			mmcMap.SignalResize <- offset
			return true
	}
}

// ResizeMmap
//	Dynamically resizes the underlying memory mapped file.
//	When a file is first created, default size is 64MB and doubles the mem map on each resize until 1GB.
//
// Returns:
//	Error if resize fails.
func (mmcMap *MMCMap) resizeMmap(offset uint64) (bool, error) {
	mmcMap.ReadResizeLock.Lock()
	mmcMap.WriteResizeLock.Lock()

	defer mmcMap.ReadResizeLock.Unlock()
	defer mmcMap.WriteResizeLock.Unlock()

	defer atomic.StoreUint32(&mmcMap.IsResizing, 0)

	cLog.Debug("resizing mmap...")

	mMap := mmcMap.Data.Load().(mmap.MMap)

	allocateSize := func() int64 {
		switch {
			case len(mMap) == 0:
				return int64(DefaultPageSize) * 16 * 1000 // 64MB
			case len(mMap) >= MaxResize:
				return int64(len(mMap) + MaxResize)
			default:
				return int64(len(mMap) * 2)
		}
	}()

	if len(mMap) > 0 {
		flushErr := mmcMap.File.Sync()
		if flushErr != nil { return false, flushErr }
		
		unmapErr := mmcMap.munmap()
		if unmapErr != nil { return false, unmapErr }
	}

	truncateErr := mmcMap.File.Truncate(allocateSize)
	if truncateErr != nil { return false, truncateErr }

	mmapErr := mmcMap.mMap()
	if mmapErr != nil { return false, mmapErr }

	cLog.Debug("mmap resized with size in bytes:", allocateSize)

	return true, nil
}

// mmap
//	Helper to memory map the mmcMap File in to buffer.
//
// Parameters:
//	minsize: the minimum allocation size for the mem map
//
// Returns:
//	Error if failure
func (mmcMap *MMCMap) mMap() error {
	mMap, mmapErr := mmap.Map(mmcMap.File, mmap.RDWR, 0)
	if mmapErr != nil { return mmapErr }

	mmcMap.Data.Store(mMap)
	return nil
}

// munmap
//	Unmaps the memory map from RAM.
//
// Returns:
//	Error if failure
func (mmcMap *MMCMap) munmap() error {
	mMap := mmcMap.Data.Load().(mmap.MMap)

	unmapErr := mMap.Unmap()
	if unmapErr != nil { return unmapErr }

	mmcMap.Data.Store(mmap.MMap{})
	return nil
}

func DetermineCoreRange(isRead bool) (int, int) {
	var startCpu, endCpu int
	
	numCpus := runtime.NumCPU()
	numAvailCpus := numCpus - 2
	numWritePinned := numAvailCpus / 2

	if isRead {
		startCpu = numCpus - numWritePinned - 1
		endCpu = numCpus -1

		return startCpu, endCpu
	} else {
		startCpu = numCpus - numAvailCpus - 1
		endCpu = numCpus - numWritePinned - 1

		return startCpu, endCpu
	}
}