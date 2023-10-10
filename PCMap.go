package pcmap

import "math"
import "os"
import "unsafe"

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
}

func (pcMap *PCMap) InitRoot() error {
	root := &PCMapNode{
		Version: 0,
		StartOffset: uint64(InitRootOffset),
		Bitmap: 0,
		IsLeaf: false,
		Children: []*PCMapNode{},
	}

	sNode, serializeErr := root.SerializeNode()
	if serializeErr != nil { return serializeErr }

	pcMap.Data = append(pcMap.Data, sNode...)

	return nil
}

func (pcMap *PCMap) ReadMetaFromMemMap() (*PCMapMetaData, error) {
	pcMap.RWLock.RLock()
	defer pcMap.RWLock.RUnlock()

	currMeta := pcMap.Data[MetaVersionIdx:MetaRootOffsetIdx + OffsetSize]
	
	meta, readMetaErr := DeserializeMetaData(currMeta)
	if readMetaErr != nil { return nil, readMetaErr }

	return meta, nil
}

func (pcMap *PCMap) ExclusiveWriteMmap(meta *PCMapMetaData, sNodes []byte, offset uint64) (bool, error) {
	if pcMap.ExistsInMemMap(offset) { return false, nil }

	pcMap.RWLock.Lock()
	defer pcMap.RWLock.Unlock()

	pcMap.WriteNodesToMemMap(sNodes)
	pcMap.WriteMetaToMemMap(meta.SerializeMetaData()) 
	
	return true, nil
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