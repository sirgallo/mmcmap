package pcmap


func (pcMap *PCMap) allocatePage() *Page {
	newPage := &Page {
		Id: pcMap.NextPageId,
		Count: 0,
		Overflow: 0,
	}

	pcMap.CurrentPageId = newPage.Id
	
	pcMap.NextPageId++
	pcMap.Pages[newPage.Id] = newPage

	return newPage
}

func (pcMap *PCMap) writeNodeToPage(node *PCMapNode) error {
	serializedNode, serializeErr := node.SerializeNode()
	if serializeErr != nil { return serializeErr }

	targetPage, exists := pcMap.Pages[node.PageId] 
	if ! exists || pcMap.CurrentPageOffset + len(serializedNode) > targetPage.PageSize {
		targetPage = pcMap.allocatePage()
		node.PageId = targetPage.Id
	}

	targetPage.Data = append(targetPage.Data, serializedNode...)
	node.Offset = getCurrentOffset(targetPage)

	return nil
}

func (pcMap *PCMap) assignNodeToPage(node *PCMapNode) error {
	targetPage, exists := pcMap.Pages[pcMap.CurrentPageId] 
	if ! exists { targetPage = pcMap.allocatePage() }

	node.PageId = targetPage.Id
	node.Offset = getCurrentOffset(targetPage)

	serializedNode, serializeErr := node.SerializeNode()
	if serializeErr != nil { return serializeErr }

	if pcMap.CurrentPageOffset + len(serializedNode) > targetPage.PageSize { targetPage = pcMap.allocatePage() }

	offset := getCurrentOffset(targetPage)

	targetPage.Data = append(targetPage.Data, serializedNode...)
	
	node.Offset = offset
	// node.EndOffset = offset + len(serializedNode) 

	return nil
}

func getCurrentOffset(Page *Page) int {
	return len(Page.Data)
}

func (pcMap *PCMap) readNodeFromPage(snode []byte) (*PCMapNode, error) {
	pcMapNode, deserializeErr := DeserializeNode(snode)
	if deserializeErr != nil { return nil, deserializeErr }
	
	return pcMapNode, nil
}