package mmcmap

import "errors"
import "sync/atomic"
import "unsafe"

import "github.com/sirgallo/mmcmap/common/mmap"


//============================================= MMCMap Metadata


// ReadMetaFromMemMap
//	Read and deserialize the current metadata object from the memory map.
func (mmcMap *MMCMap) ReadMetaFromMemMap() (meta *MMCMapMetaData, err error) {
	defer func() {
		r := recover()
		if r != nil {
			meta = nil
			err = errors.New("error reading metadata from mmap")
		}
	}()
	
	mMap := mmcMap.Data.Load().(mmap.MMap)
	currMeta := mMap[MetaVersionIdx:MetaEndSerializedOffset + OffsetSize]
	
	meta, readMetaErr := DeserializeMetaData(currMeta)
	if readMetaErr != nil { return nil, readMetaErr }

	return meta, nil
}

// WriteMetaToMemMap
//	Copy the serialized metadata into the memory map.
func (mmcMap *MMCMap) WriteMetaToMemMap(sMeta []byte) (ok bool, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ok = false
			err = errors.New("error writing metadata to mmap")
		}
	}()

	mMap := mmcMap.Data.Load().(mmap.MMap)
	copy(mMap[MetaVersionIdx:MetaEndSerializedOffset + OffsetSize], sMeta)

	flushErr := mmcMap.flushRegionToDisk(MetaVersionIdx, MetaEndSerializedOffset + OffsetSize)
	if flushErr != nil { return false, flushErr }

	return true, nil
}

// initMeta
//	Initialize and serialize the metadata in a new MMCMap.
//	Version starts at 0 and increments, and root offset starts at 24.
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

// loadMetaRootOffsetPointer
//	Get the uint64 pointer from the memory map.
func (mmcMap *MMCMap) loadMetaRootOffset() (ptr *uint64, rOff uint64, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ptr = nil
			rOff = 0
			err = errors.New("error getting root offset from mmap")
		}
	}()

	mMap := mmcMap.Data.Load().(mmap.MMap)
	rootOffsetPtr := (*uint64)(unsafe.Pointer(&mMap[MetaRootOffsetIdx]))
	rootOffset := atomic.LoadUint64(rootOffsetPtr)
	
	return rootOffsetPtr, rootOffset, nil
}

// loadMetaEndMmapPointer
//	Get the uint64 pointer from the memory map.
func (mmcMap *MMCMap) loadMetaEndSerialized() (ptr *uint64, sOff uint64, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ptr = nil
			sOff = 0
			err = errors.New("error getting end of serialized data from mmap")
		}
	}()

	mMap := mmcMap.Data.Load().(mmap.MMap)
	endSerializedPtr := (*uint64)(unsafe.Pointer(&mMap[MetaEndSerializedOffset]))
	endSerialized := atomic.LoadUint64(endSerializedPtr)
	
	return endSerializedPtr, endSerialized, nil
}

// loadMetaVersionPointer
//	Get the uint64 pointer from the memory map.
func (mmcMap *MMCMap) loadMetaVersion() (ptr *uint64, v uint64, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ptr = nil
			v = 0
			err = errors.New("error getting version from mmap")
		}
	}()

	mMap := mmcMap.Data.Load().(mmap.MMap)
	versionPtr := (*uint64)(unsafe.Pointer(&mMap[MetaVersionIdx]))
	version := atomic.LoadUint64(versionPtr)

	return versionPtr, version, nil
}

// storeMetaPointer
//	Store the pointer associated with the particular metadata (root offset, end serialized, version) back in the memory map.
func (mmcMap *MMCMap) storeMetaPointer(ptr *uint64, val uint64) (err error) {
	defer func() {
		r := recover()
		if r != nil { 
			err = errors.New("error storing meta value in mmap")
		}
	}()

	atomic.StoreUint64(ptr, val)
	return nil
}