package mmcmap

import "unsafe"

import "github.com/sirgallo/mmcmap/common/mmap"


//============================================= Metadata


// ReadMetaFromMemMap
//	Read and deserialize the current metadata object from the memory map.
//
// Returns:
//	Deserialized MMCMapMetaData object, or error if failure
func (mmcMap *MMCMap) ReadMetaFromMemMap() (*MMCMapMetaData, error) {
	mMap := mmcMap.Data.Load().(mmap.MMap)
	currMeta := mMap[MetaVersionIdx:MetaEndMmapOffset + OffsetSize]
	
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
func (mmcMap *MMCMap) WriteMetaToMemMap(sMeta []byte) (bool, error) {
	mMap := mmcMap.Data.Load().(mmap.MMap)
	copy(mMap[MetaVersionIdx:MetaEndMmapOffset + OffsetSize], sMeta)

	flushErr := mmcMap.FlushRegionToDisk(MetaVersionIdx, MetaEndMmapOffset + OffsetSize)
	if flushErr != nil { return false, flushErr }

	return true, nil
}

// LoadMetaVersionPointer
//	Get the uint64 pointer from the memory map
//
// Returns:
//	The pointer to the metadata version
func (mmcMap *MMCMap) LoadMetaVersionPointer() (*uint64, error) {
	mMap := mmcMap.Data.Load().(mmap.MMap)
	return (*uint64)(unsafe.Pointer(&mMap[MetaVersionIdx])), nil
}

// LoadMetaRootOffsetPointer
//	Get the uint64 pointer from the memory map
//
// Returns:
//	The pointer to the metadata root offset
func (mmcMap *MMCMap) LoadMetaRootOffsetPointer() (*uint64, error) {
	mMap := mmcMap.Data.Load().(mmap.MMap)
	return (*uint64)(unsafe.Pointer(&mMap[MetaRootOffsetIdx])), nil
}

// LoadMetaEndMmapPointer
//	Get the uint64 pointer from the memory map
//
// Returns:
//	The pointer to the end of the serialized data
func (mmcMap *MMCMap) LoadMetaEndMmapPointer() (*uint64, error) {
	mMap := mmcMap.Data.Load().(mmap.MMap)
	return (*uint64)(unsafe.Pointer(&mMap[MetaEndMmapOffset])), nil
}