package mmcmap

import "runtime"
import "sync/atomic"

import "github.com/sirgallo/mmcmap/common/mmap"


//============================================= MMCMap IO Utils


// determineIfResize
//	Helper function that signals go routine for resizing if the condition to resize is met.
func (mmcMap *MMCMap) determineIfResize(offset uint64) bool {
	mMap := mmcMap.Data.Load().(mmap.MMap)

	switch {
		case offset > 0 && int(offset) < len(mMap):
			return false
		case len(mMap) == 0 || ! atomic.CompareAndSwapUint32(&mmcMap.IsResizing, 0, 1):
			return true
		default:
			mmcMap.SignalResize <- true
			return true
	}
}

// flushRegionToDisk
//	Flushes a region of the memory map to disk instead of flushing the entire map. 
//	When a startoffset is provided, if it is not aligned with the start of the last page, the offset needs to be normalized.
func (mmcMap *MMCMap) flushRegionToDisk(startOffset, endOffset uint64) error {
	startOffsetOfPage := startOffset & ^(uint64(DefaultPageSize) - 1)

	mMap := mmcMap.Data.Load().(mmap.MMap)
	if len(mMap) == 0 { return nil }

	flushErr := mMap[startOffsetOfPage:endOffset].Flush()
	if flushErr != nil { return flushErr }

	return nil
}

// handleFlush
//	This is "optimistic" flushing. 
//	A separate go routine is spawned and signalled to flush changes to the mmap to disk.
func (mmcMap *MMCMap) handleFlush() {
	for range mmcMap.SignalFlush {
		func() {
			for atomic.LoadUint32(&mmcMap.IsResizing) == 1 { runtime.Gosched() }
			
			mmcMap.RWResizeLock.RLock()
			defer mmcMap.RWResizeLock.RUnlock()

			mmcMap.File.Sync()
		}()
	}
}

// handleResize
//	A separate go routine is spawned to handle resizing the memory map.
//	When the mmap reaches its size limit, the go routine is signalled.
func (mmcMap *MMCMap) handleResize() {
	for range mmcMap.SignalResize { mmcMap.resizeMmap() }
}

// mmap
//	Helper to memory map the mmcMap File in to buffer.
func (mmcMap *MMCMap) mMap() error {
	mMap, mmapErr := mmap.Map(mmcMap.File, mmap.RDWR, 0)
	if mmapErr != nil { return mmapErr }

	mmcMap.Data.Store(mMap)
	return nil
}

// munmap
//	Unmaps the memory map from RAM.
func (mmcMap *MMCMap) munmap() error {
	mMap := mmcMap.Data.Load().(mmap.MMap)
	unmapErr := mMap.Unmap()
	if unmapErr != nil { return unmapErr }

	mmcMap.Data.Store(mmap.MMap{})
	return nil
}

// resizeMmap
//	Dynamically resizes the underlying memory mapped file.
//	When a file is first created, default size is 64MB and doubles the mem map on each resize until 1GB.
func (mmcMap *MMCMap) resizeMmap() (bool, error) {
	mmcMap.RWResizeLock.Lock()
	
	defer mmcMap.RWResizeLock.Unlock()
	defer atomic.StoreUint32(&mmcMap.IsResizing, 0)

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

	return true, nil
}

// signalFlush
//	Called by all writes to "optimistically" handle flushing changes to the mmap to disk.
func (mmcMap *MMCMap) signalFlush() {
	select {
		case mmcMap.SignalFlush <- true:
		default:
	}
}

// exclusiveWriteMmap
//	Takes a path copy and writes the nodes to the memory map, then updates the metadata.
func (mmcMap *MMCMap) exclusiveWriteMmap(path *MMCMapINode) (bool, error) {
	if atomic.LoadUint32(&mmcMap.IsResizing) == 1 { return false, nil }

	versionPtr, version, loadVErr := mmcMap.loadMetaVersion()
	if loadVErr != nil { return false, nil }

	rootOffsetPtr, prevRootOffset, loadROffErr := mmcMap.loadMetaRootOffset()
	if loadROffErr != nil { return false, nil }

	endOffsetPtr, endOffset, loadSOffErr := mmcMap.loadMetaEndSerialized()
	if loadSOffErr != nil { return false, nil }

	newVersion := path.Version
	newOffsetInMMap := endOffset
	
	serializedPath, serializeErr := mmcMap.SerializePathToMemMap(path, newOffsetInMMap)
	if serializeErr != nil { return false, serializeErr }

	updatedMeta := &MMCMapMetaData{
		Version: newVersion,
		RootOffset: newOffsetInMMap,
		NextStartOffset: newOffsetInMMap + uint64(len(serializedPath)),
	}

	isResize := mmcMap.determineIfResize(updatedMeta.NextStartOffset)
	if isResize { return false, nil }

	if atomic.LoadUint32(&mmcMap.IsResizing) == 0 {
		if version == updatedMeta.Version - 1 && atomic.CompareAndSwapUint64(versionPtr, version, updatedMeta.Version) {
			mmcMap.storeMetaPointer(endOffsetPtr, updatedMeta.NextStartOffset)
			
			_, writeNodesToMmapErr := mmcMap.writeNodesToMemMap(serializedPath, newOffsetInMMap)
			if writeNodesToMmapErr != nil {
				mmcMap.storeMetaPointer(endOffsetPtr, updatedMeta.NextStartOffset)
				mmcMap.storeMetaPointer(versionPtr, version)
				mmcMap.storeMetaPointer(rootOffsetPtr, prevRootOffset)

				return false, writeNodesToMmapErr
			}
			
			mmcMap.storeMetaPointer(rootOffsetPtr, updatedMeta.RootOffset)
			mmcMap.signalFlush()
			
			return true, nil
		}
	}

	return false, nil
}