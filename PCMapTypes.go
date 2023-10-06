package pcmap

import "os"
import "unsafe"

import "github.com/sirgallo/pcmap/common/mmap"


type PCMapOpts struct {
	Filepath string
	// amount of space allocated when db needs to create new pages
	AllocSize *int
	// initial mmap size of pcmap in bytes
	InitialMmapSize *int
	// overrides default OS page size (normally 4KB)
	PageSize *int
}


// PCMapNode represents a singular node within the hash array mapped trie data structure. This is the 32 bit implementation
type PCMapNode struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	Version int
	// PageId: the identifier for the page where the PCMapNode is located
	PageId PageId
	// Offset: the offset from the beginning of the page where the serialized node is located
	Offset int
	// Key: The key associated with a value. Keys are 32 bit hashes in byte array representation. Keys are only stored within leaf nodes of the hamt
	Key []byte
	// Value: The value associated with a key, in byte array representation. Values are only stored within leaf nodes
	Value []byte
	// IsLeaf: flag indicating if the current node is a leaf node or an internal node
	IsLeaf bool
	// Bitmap: a 32 bit sparse index that indicates the location of each hashed key within the array of child nodes. Only stored in internal nodes
	Bitmap uint32
	// Children: an array of child nodes, which are CMapNodes. Location in the array is determined by the sparse index
	Children []*PCMapNode
}

// CMap is the root of the hash array mapped trie
type PCMap struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	Version int
	// Root: the root PCMapNode within the hash array mapped trie. Stored as a pointer to the location in memory of the root
	Root unsafe.Pointer
	// HashChunks: the total chunks of the 32 bit hash determining the levels within the hash array mapped trie
	HashChunks int
	// BitChunkSize: the size of each chunk in the 32 bit hash. Since a 32 bit hash is 2^5, each chunk will be 5 bits long
	BitChunkSize int
	// Filepath: path to the PCMap file
	Filepath string
	// File: the PCMap file
	File *os.File
	// Opened: flag indicating if the file has been opened
	Opened bool
	// Data: the memory mapped file as a byte slice
	Data *mmap.MMap
	// PageSize: the size of each page in in bytes. 4KiB is typical for most OS
	PageSize int
	// Pages: a map of the current pages in the PCMap
	Pages map[PageId] *Page
	// CurrentPageId: a reference to the current page in the hash array mapped trie where new data can be written
	CurrentPageId PageId
	// CurrentPageOffset: the offset from the start of the current page. When CMapNodes are serialized, the offset is increased by the size of new node
	CurrentPageOffset int
	// NextPageId: the id of the next page to be allocated once the current page has been filled
	NextPageId PageId
	// AllocSize
	AllocSize int
}

// PageId is the unique identifier associated with a page
type PageId uint64


// PageOpts represents a options when allocating a new page:
type PageOpts struct {
	// Id: The id of the current page
	Id PageId
	// PageSize: the size of the page, usually 4KiB by default, set by the OS
	PageSize int
}

// Page represents a page in the memory mapped file
type Page struct {
	// Id: The id of the current page
	Id PageId
	// PageSize: the size of the page, usually 4KiB by default, set by the OS
	PageSize int
	// Data: a representation of the data currently in the page in byte array form. Data is serialized to byte form before being added to a page
	Data []byte
	// Count: the number of references currently holding a reference to the page. Count keeps track of when it is safe to release resources
	Count uint32
	// Overflow: for when data cannot fit into a single page. Stores a reference or identifier to a location with additional data
	Overflow uint32
}

// DefaultPageSize is the default page size set by the underlying OS. Usually will be 4KiB
var DefaultPageSize = os.Getpagesize()
const DefaultAllocSize = 16 * 1024 * 1024

const MaxMapSize = 0xFFFFFFFFFFFF // ARM, 256TB
const MaxAllocSize = 0x7FFFFFFF // ARM