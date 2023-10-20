package mmcmap

import "math"
import "os"
import "sync/atomic"
import "unsafe"

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
	}

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	var openFileErr error

	mmcMap.File, openFileErr = os.OpenFile(opts.Filepath, flag, 0600)
	if openFileErr != nil { return nil, openFileErr	}

	mmcMap.Filepath = mmcMap.File.Name()

	initFileErr := mmcMap.initializeFile()
	if initFileErr != nil { return nil, initFileErr	}

	meta, readMetaErr := mmcMap.ReadMetaFromMemMap()
	if readMetaErr != nil { return nil, readMetaErr	}

	mmcMap.Meta = unsafe.Pointer(meta)
	return mmcMap, nil
}

// Close
//	Close the mmcmap, unmapping the file from memory and closing the file.
//
// Returns:
//	Error if error unmapping and closing the file
func (mmcMap *MMCMap) Close() error {
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
//	The size in bytes, or an error
func (mmcMap *MMCMap) FileSize() (int, error) {
	stat, statErr := mmcMap.File.Stat()
	if statErr != nil { return 0, statErr }

	size := int(stat.Size())
	return size, nil
}

// FlushToDisk
//	Manually flush the memory map to disk.
//
// Returns:
//	Error if flushing fails
func (mmcMap *MMCMap) FlushToDisk() error {
	flushErr := mmcMap.Data.Flush()
	if flushErr != nil {
		cLog.Error("error flushing to disk:", flushErr.Error()) 
		return flushErr 
	}

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

// ReadMetaFromMemMap
//	Read and deserialize the current metadata object from the memory map.
//
// Returns:
//	Deserialized MMCMapMetaData object, or error if failure
func (mmcMap *MMCMap) ReadMetaFromMemMap() (*MMCMapMetaData, error) {
	currMeta := mmcMap.Data[MetaVersionIdx:MetaEndMmapOffset + OffsetSize]
	meta, readMetaErr := DeserializeMetaData(currMeta)
	if readMetaErr != nil { return nil, readMetaErr }

	return meta, nil
}

// WriteMetaToMemMap
//	Copy the serialized metadata into the memory map.
//
// Parameters:
//	sMeta: the serialized metadata object
//
// Returns:
//	True when copied
func (mmcMap *MMCMap) WriteMetaToMemMap(sMeta []byte) bool {
	copy(mmcMap.Data[MetaVersionIdx:MetaEndMmapOffset + OffsetSize], sMeta)
	return true
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
		
		resizeErr := mmcMap.resizeMmap()
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
	mmcMap.WriteMetaToMemMap(serializedMeta)
	return nil
}

// InitRoot
//	Initialize the Version 0 root where operations will begin traversing.
//
// Returns:
//	Error if initializing root and serializing the MMCMapNode fails
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
//	True if success, error if failure
func (mmcMap *MMCMap) exclusiveWriteMmap(path *MMCMapNode, currMeta *MMCMapMetaData, currMetaPtr *unsafe.Pointer) (bool, error) {
	newOffsetInMMap := currMeta.EndMmapOffset + 1
	serializedPath, serializeErr := mmcMap.SerializePathToMemMap(path, newOffsetInMMap)
	if serializeErr != nil { return false, serializeErr }

	if *currMetaPtr == atomic.LoadPointer(&mmcMap.Meta) {
		updatedMeta := &MMCMapMetaData{
			Version: path.Version,
			RootOffset: newOffsetInMMap,
			EndMmapOffset: newOffsetInMMap + uint64(len(serializedPath)),
		}

		if atomic.CompareAndSwapPointer(&mmcMap.Meta, unsafe.Pointer(currMeta), unsafe.Pointer(updatedMeta)) {
			_, writeNodesToMmapErr := mmcMap.writeNodesToMemMap(serializedPath, newOffsetInMMap)
			if writeNodesToMmapErr != nil { return false, writeNodesToMmapErr }

			mmcMap.WriteMetaToMemMap(updatedMeta.SerializeMetaData())
			return true, nil
		}
	}

	return false, nil
}

// ResizeMmap
//	Dynamically resizes the underlying memory mapped file.
//	When a file is first created, default size is 64MB and doubles the mem map on each resize until 1GB.
//
// Returns:
//	Error if resize fails.
func (mmcMap *MMCMap) resizeMmap() error {
	allocateSize := func() int64 {
		if mmcMap.Data == nil { return int64(DefaultPageSize) * 16 * 1000 } // 64MB
		if len(mmcMap.Data) >= MaxResize { return int64(len(mmcMap.Data) + MaxResize) }
		return int64(len(mmcMap.Data) * 2)
	}()

	if mmcMap.Data != nil {
		unmapErr := mmcMap.munmap()
		if unmapErr != nil { return unmapErr }
	}

	truncateErr := mmcMap.File.Truncate(allocateSize)
	if truncateErr != nil { return truncateErr }

	mmapErr := mmcMap.mMap()
	if mmapErr != nil { return mmapErr }

	return nil
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

	mmcMap.Data = mMap
	return nil
}

// munmap
//	Unmaps the memory map from RAM.
//
// Returns:
//	Error if failure
func (mmcMap *MMCMap) munmap() error {
	unmapErr := mmcMap.Data.Unmap()
	if unmapErr != nil { return unmapErr }

	return nil
}