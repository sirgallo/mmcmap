package pcmap

import "math"
import "fmt"
import "os"
import "unsafe"
import "sync/atomic"

import "github.com/sirgallo/pcmap/common/mmap"
import "github.com/sirgallo/pcmap/common/utils"


// Open initializes a new hash array mapped trie
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
	pcMap.mmap(DefaultPageSize * 100)

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

func (pcMap *PCMap) Remove() error {
	closeErr := pcMap.Close()
	if closeErr != nil { return closeErr }

	removeErr := os.Remove(pcMap.File.Name())
	if removeErr != nil { return removeErr }

	return nil
}

func (pcMap *PCMap) InitMeta() {
	newMeta := &PCMapMetaData{
		Version: 0,
		RootOffset: uint64(InitRootOffset),
	}

	pcMap.Data = append(pcMap.Data, newMeta.SerializeMetaData()...)
	// fmt.Println("mmap length", len(pcMap.Data), "meta length:", len(newMeta.SerializeMetaData()))
}

func (pcMap *PCMap) InitRoot() error {
	root := &PCMapNode{
		Version: 0,
		StartOffset: uint64(InitRootOffset),
		Bitmap: 0,
		IsLeaf: false,
		Children: []*PCMapNode{},
	}

	sNode, serializeErr := root.SerializeNode(root.StartOffset)
	if serializeErr != nil { return serializeErr }

	pcMap.Data = append(pcMap.Data, sNode...)
	// fmt.Println("init root length:", len(sNode), "mmap length:", len(pcMap.Data))

	return nil
}

func (pcMap *PCMap) ReadMetaFromMemMap() (*PCMapMetaData, error) {
	currMeta := pcMap.Data[MetaVersionIdx:MetaRootOffsetIdx + OffsetSize]
	
	meta, readMetaErr := DeserializeMetaData(currMeta)
	if readMetaErr != nil { return nil, readMetaErr }

	return meta, nil
}

func (pcMap *PCMap) ExclusiveWriteMmap(path *PCMapNode) (bool, error) {
	newOffset, serializedPath, serializeErr := pcMap.SerializePathToMemMap(path)
	if serializeErr != nil { return false, serializeErr }
	
	// fmt.Println("new offset", newOffset)
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

	if atomic.CompareAndSwapPointer(&pcMap.Meta, unsafe.Pointer(currMeta), unsafe.Pointer(updatedMeta)) {
		pcMap.WriteNodesToMemMap(serializedPath)
		pcMap.WriteMetaToMemMap(updatedMeta.SerializeMetaData())
		
		return true, nil
	}

	return false, nil
}


func (pcMap *PCMap) WriteMetaToMemMap(sMeta []byte) bool {
	copy(pcMap.Data[MetaVersionIdx:MetaRootOffsetIdx + OffsetSize], sMeta)
	return true
}

func (pcMap *PCMap) DetermineNextOffset() uint64 {
	return uint64(len(pcMap.Data))
}


func (pcMap *PCMap) fileSize() (int, error) {
	stat, statErr := pcMap.File.Stat()
	if statErr != nil { return 0, statErr }

	size := int(stat.Size())

	return size, nil
}

func (pcMap *PCMap) mmap(minsize int) error {
	unmapErr := pcMap.munmap()
	if unmapErr != nil { return unmapErr }

	mMap, mmapErr := mmap.Map(pcMap.File, mmap.RDWR, 0)
	if mmapErr != nil { return mmapErr }

	pcMap.Data = mMap

	return nil
}

func (pcMap *PCMap) munmap() error {
	unmapErr := pcMap.Data.Unmap()
	if unmapErr != nil { return unmapErr }

	return nil
}