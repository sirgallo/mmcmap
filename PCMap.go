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
	pcMap.mmap(DefaultPageSize)

	fSize, fSizeErr := pcMap.fileSize()
	if fSizeErr != nil { return nil, fSizeErr }
	
	if fSize == 0 {
		pcMap.InitMeta()
		initRootErr := pcMap.InitRoot()
		if initRootErr != nil { return nil, initRootErr }
	}

	meta, readMetaErr := pcMap.ReadMetaFromMemMap()
	if readMetaErr != nil { return nil, readMetaErr }

	pcMap.Meta = unsafe.Pointer(meta)
	return pcMap, nil
}

// InitMeta
//	Initialize and serialize the metadata in a new PCMap. Version starts at 0 and increments, and root offset starts at 16
func (pcMap *PCMap) InitMeta() {
	newMeta := &PCMapMetaData{
		Version: 0,
		RootOffset: uint64(InitRootOffset),
	}

	pcMap.Data = append(pcMap.Data, newMeta.SerializeMetaData()...)
}

// InitRoot
//	Initialize the Version 0 root where operations will begin traversing.
//
// Returns:
//	error if initializing root and serializing the PCMapNode fails
func (pcMap *PCMap) InitRoot() error {
	root := &PCMapNode{
		Version: 0,
		StartOffset: uint64(InitRootOffset),
		Bitmap: 0,
		IsLeaf: false,
		KeyLength: uint16(0),
		Children: []*PCMapNode{},
	}

	sNode, serializeErr := root.SerializeNode(root.StartOffset)
	if serializeErr != nil { return serializeErr }

	pcMap.Data = append(pcMap.Data, sNode...)
	return nil
}

// ReadMetaFromMemMap
//	Read and deserialize the current metadata object from the memory map.
//
// Returns:
//	Deserialized PCMapMetaData object, or error if failure
func (pcMap *PCMap) ReadMetaFromMemMap() (*PCMapMetaData, error) {
	currMeta := pcMap.Data[MetaVersionIdx:MetaRootOffsetIdx + OffsetSize]
	
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
func (pcMap *PCMap) WriteMetaToMemMap(sMeta []byte) bool {
	copy(pcMap.Data[MetaVersionIdx:MetaRootOffsetIdx + OffsetSize], sMeta)
	return true
}

// ExclusiveWriteMmap
//	Takes a path copy and writes the nodes to the memory map, then updates the metadata.
//
// Returns
//	true is success, error if failure
func (pcMap *PCMap) ExclusiveWriteMmap(path *PCMapNode) (bool, error) {
	newOffset, serializedPath, serializeErr := pcMap.SerializePathToMemMap(path)
	if serializeErr != nil { return false, serializeErr }
	
	currMetaPtr := atomic.LoadPointer(&pcMap.Meta)
	currMeta := (*PCMapMetaData)(currMetaPtr)

	updatedMeta := &PCMapMetaData{
		Version: path.Version,
		RootOffset: newOffset,
	} 

	if pcMap.ExistsInMemMap(newOffset) { 
		fmt.Println(newOffset, "exists in mem already")
		return false, nil 
	}

	if currMetaPtr == atomic.LoadPointer(&pcMap.Meta) {
		if atomic.CompareAndSwapPointer(&pcMap.Meta, unsafe.Pointer(currMeta), unsafe.Pointer(updatedMeta)) {
			pcMap.WriteNodesToMemMap(serializedPath)
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

// DetermineNextOffset
//	When appending a path to the mem map, determine the next available offset.
//
// Returns:
//	The offset
func (pcMap *PCMap) DetermineNextOffset() uint64 {
	return uint64(len(pcMap.Data))
}

// fileSize
//	Determine the memory mapped file size.
//
// Returns:
//	the size in bytes, or an error
func (pcMap *PCMap) fileSize() (int, error) {
	stat, statErr := pcMap.File.Stat()
	if statErr != nil { return 0, statErr }

	size := int(stat.Size())
	return size, nil
}

// mmap
//	Helper to memory map the pcMap File in to buffer.
//
// Parameters:
//	minsize: the minimum allocation size for the mem map
//
// Returns:
//	Error if failure
func (pcMap *PCMap) mmap(minsize int) error {
	unmapErr := pcMap.munmap()
	if unmapErr != nil { return unmapErr }

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