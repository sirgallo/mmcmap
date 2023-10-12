package mmap

import "errors"
import "os"
import "golang.org/x/sys/unix"


//============================================= MMap


// Map 
//	Memory maps an entire file.
//
// Parameters:
//	file: the file to be memory mapped
//	prot: the protection level on the file (RDONLY, RDWR, COPY, EXEC)
//	flags: if ANON is set in flags, file is ignored and memory is anonymously mapped
//
// Returns:
//	The byte array representation of the memory mapped file or an error
func Map(file *os.File, prot, flags int) (MMap, error) {
	return MapRegion(file, -1, prot, flags, 0)
}

// MapRegion 
//	Memory maps a region of a file.
//
// Parameters:
// 	file: the file to be memory mapped
//	length: the length in bytes to be mapped
//	prot: the protection level on the file (RDONLY, RDWR, COPY, EXEC)
//	flags: if ANON is set in flags, file is ignored and memory is anonymously mapped
//	
// Returns:
//	The byte array representation of the memory mapped file or an error
func MapRegion(file *os.File, length int, prot, flags int, offset int64) (MMap, error) {
	if offset % int64(os.Getpagesize()) != 0 {
		return nil, errors.New("offset parameter must be a multiple of the system's page size")
	}

	var fileDescriptor uintptr
	
	if flags & ANON == 0 {
		fileDescriptor = uintptr(file.Fd())
		
		if length < 0 {
			fileStat, statErr := file.Stat()
			if statErr != nil { return nil, statErr }
			
			length = int(fileStat.Size())
		}
	} else {
		if length <= 0 { return nil, errors.New("anonymous mapping requires non-zero length") }
		fileDescriptor = ^uintptr(0)
	}

	return mmapHelper(length, uintptr(prot), uintptr(flags), fileDescriptor, offset)
}

// mmapHelper 
//	Utility function for mmap.
//
// Parameters:
//	length: the length in bytes to be mapped
//	inprot: the protection level on the file (RDONLY, RDWR, COPY, EXEC) --> if COPY, set the flag from MAP_SHARED (so shared between processes) to MAP_PRIVATE (used by one process)
//	inflags: if ANON is set in flags, file is ignored and memory is anonymously mapped
//	fileDescriptor: the file descriptor for the open file
//	offset: the offset from the start mapped file to begin mapping
//	
// Returns:
//	byte slice, which is the memory mapped file and what will be operated on or an error
func mmapHelper(length int, inprot, inflags, fileDescriptor uintptr, offset int64) ([]byte, error) {
	flags := unix.MAP_SHARED
	prot := unix.PROT_READ
	
	switch {
		case inprot & COPY != 0:
			prot |= unix.PROT_WRITE
			flags = unix.MAP_PRIVATE
		case inprot & RDWR != 0:
			prot |= unix.PROT_WRITE
	}
	
	if inprot & EXEC != 0 { prot |= unix.PROT_EXEC }
	if inflags & ANON != 0 { flags |= unix.MAP_ANON }

	bytes, mmapErr := unix.Mmap(int(fileDescriptor), offset, length, prot, flags)
	if mmapErr != nil { return nil, mmapErr }
	
	return bytes, nil
}

// Flush
//	Writes the byte slice from the mmap to disk.
//
// Returns:
//	nil or error
func (mapped MMap) Flush() error {
	return unix.Msync(mapped, unix.MS_SYNC)
}

// Unmap 
//	Unmaps the byte slice from the memory mapped file.
//
// Returns:
// nil or error
func (mapped MMap) Unmap() error {
	return unix.Munmap(mapped)
}