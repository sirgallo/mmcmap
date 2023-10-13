package pcmap

import "math"
import "fmt"
import "os"
import "unsafe"
import "sync/atomic"

import "github.com/sirgallo/pcmap/common/mmap"
import "github.com/sirgallo/pcmap/common/utils"


//============================================= PCMap


// Open initializes a new pcmap
//	This will create the memory mapped file or read it in if it already exists.
//	Then, the meta data is initialized and written to the first 0-15 bytes in the memory map.
//	An initial root PCMapNode will also be written to the memory map as well
//
// Parameters:
//	opts: a PCMapOpts object for initializing the mem map
//
// Returns:
//	The newly initialized hash array mapped trie
func Open(opts PCMapOpts) (*PCMap, error) {
	bitChunkSize := 5
	hashChunks := int(math.Pow(float64(2), float64(bitChunkSize))) / bitChunkSize

	pcMap := &PCMap{ 
		BitChunkSize: bitChunkSize,
		HashChunks: hashChunks,
		Opened: true,
		AllocSize: DefaultPageSize,
	}

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	var openFileErr error

	pcMap.File, openFileErr = os.OpenFile(opts.Filepath, flag, 0600)
	if openFileErr != nil { return nil, openFileErr }

	pcMap.Filepath = pcMap.File.Name()

	initFileErr := pcMap.InitializeFile()
	if initFileErr != nil { return nil, initFileErr }

	meta, readMetaErr := pcMap.ReadMetaFromMemMap()
	if readMetaErr != nil { return nil, readMetaErr }

	pcMap.Meta = unsafe.Pointer(meta)
	return pcMap, nil
}

// InitializeFile
func (pcMap *PCMap) InitializeFile() error {
	fSize, fSizeErr := pcMap.FileSize()
	if fSizeErr != nil { return fSizeErr }

	if fSize == 0 {
		resizeErr := pcMap.ResizeMmap()
		if resizeErr != nil { return resizeErr }

		endOffset, initRootErr := pcMap.InitRoot()
		if initRootErr != nil { return initRootErr }
		
		initMetaErr := pcMap.InitMeta(endOffset)
		if initMetaErr != nil { return initMetaErr }
	}	else { 
		mmapErr := pcMap.mMap()
		if mmapErr != nil { return mmapErr }
	}

	return nil
}

// InitMeta
//	Initialize and serialize the metadata in a new PCMap. Version starts at 0 and increments, and root offset starts at 16
func (pcMap *PCMap) InitMeta(endRoot uint64) error {
	newMeta := &PCMapMetaData{
		Version: 0,
		RootOffset: uint64(InitRootOffset),
		EndMmapOffset: endRoot,
	}

	serializedMeta := newMeta.SerializeMetaData()
	pcMap.WriteMetaToMemMap(serializedMeta)
	return nil
}

// InitRoot
//	Initialize the Version 0 root where operations will begin traversing.
//
// Returns:
//	error if initializing root and serializing the PCMapNode fails
func (pcMap *PCMap) InitRoot() (uint64, error) {
	root := &PCMapNode{
		Version: 0,
		StartOffset: uint64(InitRootOffset),
		Bitmap: 0,
		IsLeaf: false,
		KeyLength: uint16(0),
		Children: []*PCMapNode{},
	}

	endOffset, writeNodeErr := pcMap.WriteNodeToMemMap(root)
	if writeNodeErr != nil { return 0, writeNodeErr }

	return endOffset, nil
}

// ReadMetaFromMemMap
//	Read and deserialize the current metadata object from the memory map.
//
// Returns:
//	Deserialized PCMapMetaData object, or error if failure
func (pcMap *PCMap) ReadMetaFromMemMap() (*PCMapMetaData, error) {
	currMeta := pcMap.Data[MetaVersionIdx:MetaEndMmapOffset + OffsetSize]
	meta, readMetaErr := DeserializeMetaData(currMeta)
	if readMetaErr != nil { return nil, readMetaErr }

	return meta, nil
}

// WriteMetaToMemMap
//	copy the serialized metadata into the memory map.
//
// Parameters:
//	sMeta: the serialized metadata object
//
// Returns:
//	true when copied
func (pcMap *PCMap) WriteMetaToMemMap(sMeta []byte) (bool) {
	copy(pcMap.Data[MetaVersionIdx:MetaEndMmapOffset + OffsetSize], sMeta)
	return true
}

// ExclusiveWriteMmap
//	Takes a path copy and writes the nodes to the memory map, then updates the metadata.
//
// Returns
//	true is success, error if failure
func (pcMap *PCMap) ExclusiveWriteMmap(path *PCMapNode, currMeta *PCMapMetaData, currMetaPtr *unsafe.Pointer) (bool, error) {
	newOffsetInMMap := currMeta.EndMmapOffset + 1
	serializedPath, serializeErr := pcMap.SerializePathToMemMap(path, newOffsetInMMap)
	if serializeErr != nil { return false, serializeErr }

	if *currMetaPtr == atomic.LoadPointer(&pcMap.Meta) {
		updatedMeta := &PCMapMetaData{
			Version: path.Version,
			RootOffset: newOffsetInMMap,
			EndMmapOffset: newOffsetInMMap + uint64(len(serializedPath)),
		}

		if atomic.CompareAndSwapPointer(&pcMap.Meta, unsafe.Pointer(currMeta), unsafe.Pointer(updatedMeta)) {
			_, writeNodesToMmapErr := pcMap.WriteNodesToMemMap(serializedPath, newOffsetInMMap)
			if writeNodesToMmapErr != nil { return false, writeNodesToMmapErr }

			pcMap.WriteMetaToMemMap(updatedMeta.SerializeMetaData())
			return true, nil
		}
	}

	return false, nil
}

// Close
//	Close the pcmap, unmapping the file from memory and closing the file.
//
// Returns:
//	error if error unmapping and closing the file
func (pcMap *PCMap) Close() error {
	if ! pcMap.Opened { return nil }
	pcMap.Opened = false

	unmapErr := pcMap.munmap()
	if unmapErr != nil { return unmapErr }

	if pcMap.File != nil { 
		closeErr := pcMap.File.Close()
		if closeErr != nil { return closeErr }
	}

	pcMap.Filepath = utils.GetZero[string]()
	return nil
}

// Remove
//	Close the PCMap and remove the source file.
//
// Returns:
//	error if operation fails
func (pcMap *PCMap) Remove() error {
	closeErr := pcMap.Close()
	if closeErr != nil { return closeErr }

	removeErr := os.Remove(pcMap.File.Name())
	if removeErr != nil { return removeErr }

	return nil
}

// FileSize
//	Determine the memory mapped file size.
//
// Returns:
//	the size in bytes, or an error
func (pcMap *PCMap) FileSize() (int, error) {
	stat, statErr := pcMap.File.Stat()
	if statErr != nil { return 0, statErr }

	size := int(stat.Size())
	return size, nil
}

// ResizeMmap
//	Dynamically resizes the underlying memory mapped file. 
//	When a file is first created, default size is 64MB and doubles the mem map on each resize until 1GB
//
// Returns:
//	Error if resize fails.
func (pcMap *PCMap) ResizeMmap() error {
	allocateSize := func() int64 { 
		if pcMap.Data == nil { return int64(DefaultPageSize) * 16 * 1000 }	// 64MB
		if len(pcMap.Data) >= MaxResize { return int64(len(pcMap.Data) + MaxResize) }
		return int64(len(pcMap.Data) * 2)
	}()

	fmt.Println("resizing memmap with size", allocateSize)

	if pcMap.Data != nil {
		unmapErr := pcMap.munmap()
		if unmapErr != nil { return unmapErr }
	}

	truncateErr := pcMap.File.Truncate(allocateSize)
	if truncateErr != nil { return truncateErr }
	
	mmapErr := pcMap.mMap()
	if mmapErr != nil { return mmapErr }

	return nil
}

// FlushToDisk
//	Manually flush the memory map to disk
//
// Returns:
//	Error if flushing fails
func (pcMap *PCMap) FlushToDisk() error {	
	flushErr := pcMap.Data.Flush()
	if flushErr != nil { return flushErr }

	return nil
}

// mmap
//	Helper to memory map the pcMap File in to buffer.
//
// Parameters:
//	minsize: the minimum allocation size for the mem map
//
// Returns:
//	Error if failure
func (pcMap *PCMap) mMap() error {
	mMap, mmapErr := mmap.Map(pcMap.File, mmap.RDWR, 0)
	if mmapErr != nil { return mmapErr }

	pcMap.Data = mMap
	return nil
}

// munmap
//	Unmaps the memory map from RAM.
//
// Returns:
//	Error if failure
func (pcMap *PCMap) munmap() error {
	unmapErr := pcMap.Data.Unmap()
	if unmapErr != nil { return unmapErr }

	return nil
}