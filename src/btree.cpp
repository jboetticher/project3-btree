/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------



BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	bufMgr = bufMgrIn;
	attributeType = attrType; // should just be INTEGER
	BTreeIndex::attrByteOffset = attrByteOffset;

	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str(); // indexName is the name of the index file
	outIndexName = indexName;

	// If indexName exists, then the file is opened.
	try {
		BlobFile bf = BlobFile::open(indexName);
		file = &bf;
		// ERROR: undefined. meant to be a field?
		headerPageNum = file->getFirstPageNo();
		
		// read meta info (btree.h:108)
		Page *metaPage;
		bufMgr->readPage(file, headerPageNum, metaPage);
		IndexMetaInfo metaInfo = *reinterpret_cast<IndexMetaInfo*>(metaPage);
		headerPageNum = metaInfo.rootPageNo;
	}
	// If index file doesn't already exist, create new file.
	catch(FileNotFoundException const&) {
		BlobFile bf = BlobFile::create(indexName);
		file = &bf;
		
		// initialize meta info page
		IndexMetaInfo newInfo;
		strcpy(newInfo.relationName, relationName.c_str());
		newInfo.attrByteOffset = attrByteOffset;
		newInfo.attrType = attrType;
		newInfo.rootPageNo = 1;
		rootPageNum = 1;

		// read meta info page to file
		Page* metaPage = reinterpret_cast<Page*>(&newInfo);
		file->writePage(0, *metaPage);

		// initialize root node
		NonLeafNodeInt* rootNode;
		rootNode->level = 1;
		
		Page* rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage);
		*rootPage = *reinterpret_cast<Page*>(rootNode);

		// = (Page*) &rootNode;
		// file->writePage(1, *rootPage);
	}



	// the constructor should scan relationName and insert entries
	// for all of the tuples in the relation into the index
	FileScan scan(relationName, bufMgr);
	try {
		RecordId nextRec;

		while(true) {
			scan.scanNext(nextRec);
			
			// --- The following is taken from main.cpp:121 ---
			// Assuming RECORD.keyIndex is our key, lets extract the key, which we know is 
			// INTEGER and whose byte offset is also know inside the record. 
			std::string recordStr = scan.getRecord();
			const char *record = recordStr.c_str();
			int key = attrByteOffset + *reinterpret_cast<const int*>(record); // offsetof (attributeType, keyIndex)));
			
			insertEntry(&key, nextRec);
		}
	}
	catch(EndOfFileException const&) {
		std::cout << "Initial file scan of " << indexName << " finished.";
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------


BTreeIndex::~BTreeIndex()
{
	/*
	The destructor. Perform any cleanup that may be necessary, including clearing up
	any state variables, unpinning any B+ Tree pages that are pinned, and flushing the
	index file (by calling bufMgr->flushFile()). Note that this method does not
	delete the index file! But, deletion of the file object is required, which will call the
	destructor of File class causing the index file to be closed.
	*/
	
	// ends scan if it is in progress
	if(scanExecuting) {
		endScan();
	}

	// unpin the root page.
	bufMgr->unPinPage(file, rootPageNum, false);

	// flushing the index
	bufMgr->flushFile(file);

	// clearing up any state variables
	delete bufMgr;
	delete currentPageData;
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

NonLeafNodeInt BTreeIndex::getNonLeafNodeFromPage(PageId pageId) {
	Page* p;
	bufMgr->readPage(file, pageId, p);
	NonLeafNodeInt* node = reinterpret_cast<NonLeafNodeInt*>(p);
	return *node;
}

NonLeafNodeInt BTreeIndex::getRootNode() {
	return getNonLeafNodeFromPage(rootPageNum);
}

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	/*
	Start from root and recursively search for which leaf key belongs to
	If leaf is full then split leaf, update parent non-leaf, and if root needs splitting then update metadata
	*/

	// Creates the RIDKeyPair that we're going to enter into the tree.
	RIDKeyPair<int> current_data_to_enter;
	current_data_to_enter.set(rid, *((int *)key));

	// We start our search at the root.
	Page* rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);
	bool is_leaf = getRootNode().level == 1;
  	PageKeyPair<int> *child_data = nullptr;

	// Begin search & insert
	search(rootPage, is_leaf, current_data_to_enter, child_data);
	
}

void BTreeIndex::search(
	Page* Page_currently, 
	bool is_leaf, 
	const RIDKeyPair<int> current_data_to_enter, 
	PageKeyPair<int> *&child_data)
{
	// NOTE: 	should be potentially renamed search_and_insert, or something along those lines. Readers
	// 			may initially assume that this is just scanning, instead of also writing.
	PageId pageId_currently = Page_currently->page_number();
	
	// If this is a nonleaf node, then we have to drill down through its children to keep searching 
	// for the right leaf node.
	if(!is_leaf) {
		NonLeafNodeInt *Node_currently = reinterpret_cast<NonLeafNodeInt *>(Page_currently);
		Page *page_next;
		PageId node_next_number;
		
		// Finds which child node key belongs to and sets it to node_next_number
		nextNonLeafNode(Node_currently, node_next_number, current_data_to_enter.key);
		bufMgr->readPage(file, node_next_number, page_next);
		is_leaf = Node_currently->level == 1;

		// Recursive search to until the leaf node
		search(page_next, is_leaf, current_data_to_enter, child_data);

		// If child_data is null, then there is no pushup from a split_leaf_node.
		// Otherwise, the child_data must be updated.
		if(child_data != nullptr) {
			
			// If it's not full then insert in child data
			if(Node_currently->pageNoArray[INTARRAYNONLEAFSIZE - 1] == 0){
				insert_into_nonleaf(Node_currently, child_data);
				child_data == nullptr;
				bufMgr -> unPinPage(file, pageId_currently, true);
			}

			// Otherwise we will need to use the split algorithm
			else {
				split_nonleaf_node(*Node_currently, pageId_currently, child_data);
			}
			
		}
		else {
			bufMgr->unPinPage(file, pageId_currently, false);
		}
	}

	// When we find the proper leaf node, we insert into it
	else {
		LeafNodeInt *leaf_node = reinterpret_cast<LeafNodeInt *>(pageId_currently);
		
		// If it's not full then just insert into leaf node
		if (leaf_node->ridArray[INTARRAYLEAFSIZE - 1].page_number == 0)
		{
			insert_into_leaf(*leaf_node, current_data_to_enter);
			bufMgr->unPinPage(file, pageId_currently, true);

			// Here we set the child_data to null, indicating that we do not have to split
			// nonleaf nodes, and there is nothing being pushed up.
			child_data = nullptr;
		}

		// Otherwise we will need to use the split algorithm. This will cause a value to be
		// pushed up, meaning that child_data will not be null.
		else {
			split_and_insert_leaf_node(*leaf_node, pageId_currently, child_data, current_data_to_enter);
		}
	}
}

void BTreeIndex::nextNonLeafNode(NonLeafNodeInt *Node_currently, PageId &child_pageId, int key){ // get non leaf child
	int keyIndex = nodeOccupancy - 1;
	// We traverse through the current node to find the last full key slot
	while((keyIndex >= 0) && (Node_currently->pageNoArray[keyIndex] == 0)) {
		keyIndex--;
	}
	// Then we traverse through the non-empty current node to find the last key that is greater than key we want to enter in
	while((keyIndex > 0) && (Node_currently->keyArray[keyIndex] >= key)) {
		keyIndex--;
	}
	child_pageId = Node_currently->pageNoArray[keyIndex]; // child page id
}

void BTreeIndex::insert_into_nonleaf(NonLeafNodeInt *Node_nonleaf, PageKeyPair<int> *key_and_page){
	// keyIndex = last key index
	int keyIndex = INTARRAYNONLEAFSIZE - 1;

	// Traverse node's page array from end to front to find last valid key
	while((keyIndex >= 0) && (Node_nonleaf->pageNoArray[keyIndex] == 0)){
		keyIndex--;
	}

	// Traverse through non leaf node from end to front to find which index of the key is the key 
	// bigger than the left keys of the node.
	// We also shift all the indeces to the right so that there is space to add the new key pair.
	while((keyIndex > 0) && (Node_nonleaf->keyArray[keyIndex - 1] > key_and_page->key)){
		Node_nonleaf->keyArray[keyIndex] = Node_nonleaf->keyArray[keyIndex - 1];
		Node_nonleaf->pageNoArray[keyIndex + 1] = Node_nonleaf->pageNoArray[keyIndex];
		keyIndex--;
	}
	
	// Insert in key and page number into that index
	Node_nonleaf -> keyArray[keyIndex] = key_and_page->key;
	Node_nonleaf -> pageNoArray[keyIndex + 1] = key_and_page->pageNo;
}

void BTreeIndex::insert_into_leaf(LeafNodeInt node, RIDKeyPair<int> data_to_enter){
	int i = INTARRAYLEAFSIZE;

	// Traverse the node to find the first slot
	while((i > 0) && (node.ridArray[i].page_number == 0)){
		i--;
	}

	// Shifts all keys and records to the right to make space for the data being entered
	while((i >= 0) && (data_to_enter.key <= node.keyArray[i])){
		node.keyArray[i + 1] = node.keyArray[i];
		node.ridArray[i + 1] = node.ridArray[i];
		i--;
	}

	// Insert the data into the leaf node
	node.keyArray[i + 1] = data_to_enter.key;
	node.ridArray[i + 1] = data_to_enter.rid;
}

void BTreeIndex::split_nonleaf_node(NonLeafNodeInt node_old, PageId page_num_old, PageKeyPair<int> *&child_data){
	// We are initializing
	PageId newNum;
	Page* newP;
	bufMgr->allocPage(file, newNum, newP);
	NonLeafNodeInt *node_new = reinterpret_cast<NonLeafNodeInt *>(newP);
	// INTARRAYNONLEAFSIZE might be accessible form namespace. We have to check through compiler
	int middle_key = INTARRAYNONLEAFSIZE / 2;
	PageKeyPair<int> pagekeypair_for_root;

	// NOTE: could you explain why node_old.keyArray[middle_key] is the right value?

	
	/*
		NOTE: 	nodeOccupancy is never assigned. I don't know how/why it's being used in this code.
		NOTE: 	I'm a little concerned about the pageNoArray. node_old keeps the right number of pageIds,
				but node_new has one less than its supposed to. I'm not sure where the pageId is supposed to
				come from.
					
				 2, 3, 5, 7
				  5 pId				
		 2, 3				5, 7
		3 pId				3 pId

		middle_key = (4? / 2) = 2;

		node_old becomes	2, 3	keeps 3 pointers
		node_new becomes	5, 7	only gets 2 pointers
	*/
	// middle_key + 1 for odd nodes
	// INTARRAYNONLEAFSIZE 
	node_new->pageNoArray[middle_key]=node_old.pageNoArray[INTARRAYNONLEAFSIZE];
	// looping through a new node made and assigning the second half of the old node to the start of this node.
	for(int i = middle_key; i < INTARRAYNONLEAFSIZE; i++){
		// node new is a new node created
		// we will assign anything that is moved from old node to new --> 0
		node_new->keyArray[i - middle_key] = node_old.keyArray[i];
		node_old.keyArray[i] = 0;
		node_new->pageNoArray[i - middle_key] = node_old.pageNoArray[i];
		node_old.pageNoArray[i + 1] = static_cast<PageId>(0);
	}
	node_new->level = node_old.level;
	child_data->pageNo = node_old.pageNoArray[middle_key];
	child_data->key = node_old.keyArray[middle_key-1];
	node_old.pageNoArray[middle_key] = static_cast<PageId>(0);
	node_old.keyArray[middle_key] = 0;
	// child_data = &pagekeypair_for_root;

	// Unpins the pages that we don't need anymore, including the new page we just made.
	bufMgr->unPinPage(file, page_num_old, true);
	bufMgr->unPinPage(file, newP->page_number(), true);

	//splitting becomes tricky when we are at root.
	//if we split at root then the old root (node_old) is not the true root
	//if we are splitting at the root, then we must update root
	if(page_num_old == rootPageNum)
	{
		rootChange(page_num_old, newNum, child_data->key);
	}
}

// Switch this to root only and make another for other cases
void BTreeIndex::rootChange(PageId left_page_id, PageId right_page_id, int key)
{
	// create a new root page
	Page *new_root;
	PageId new_root_id; 
	bufMgr->allocPage(file, new_root_id, new_root);
	NonLeafNodeInt *new_parent_node = (NonLeafNodeInt *)new_root;
	
	// integrate into tree
	new_parent_node->pageNoArray[0]= left_page_id;
	new_parent_node->pageNoArray[1] = right_page_id;
	new_parent_node->keyArray[0] = key;
	new_parent_node->level = 0;
	
	// book keeping 
	rootPageNum = new_root_id;
	Page *meta_page;
	bufMgr->readPage(file, headerPageNum, meta_page);
	IndexMetaInfo *metaInfoPage = (IndexMetaInfo *)meta_page;
	metaInfoPage->rootPageNo = new_root_id;

	// unpin pages
	bufMgr->unPinPage(file, headerPageNum, true);
	bufMgr->unPinPage(file, rootPageNum, true);
	
}

// This needs to be updated to how nonleaf is made
void BTreeIndex::split_and_insert_leaf_node(
	LeafNodeInt node_old, 
	PageId page_num_old, 
	PageKeyPair<int> *&child_data, 
	const RIDKeyPair<int> current_data_to_enter)
{
	// We are initializing
	PageId newNum;
	Page* newP;
	bufMgr->allocPage(file, newNum, newP);
	LeafNodeInt *node_new = reinterpret_cast<LeafNodeInt *>(newP);
	// INTARRAYLEAFSIZE might be accessible form namespace. We have to check through compiler
	int middle_key = INTARRAYLEAFSIZE / 2;
	PageKeyPair<int> pagekeypair_for_root;

	// NOTE: could you explain why node_old.keyArray[middle_key] is the right value?
	pagekeypair_for_root.set(newNum, node_old.keyArray[middle_key]);

	for(int i = middle_key; i < INTARRAYLEAFSIZE; i++){
		// the last full pointer in node_new is missing
		// node new is a new node created
		// we will assign anything that is moved from old node to new --> 0
		node_new->keyArray[i - middle_key] = node_old.keyArray[i];
		node_old.keyArray[i] = 0;
		node_new->ridArray[i - middle_key] = node_old.ridArray[i+1];

		RecordId nullRecord;
		nullRecord.page_number = 0;
		nullRecord.slot_number = 0;
		nullRecord.padding = 0;
		node_old.ridArray[i] = nullRecord;
	}

	// If the key we want to insert is less than the 1st entry in node_new then it belongs to node_old.
	// Otherwise it belongs to node_new.
	// We use insert_into_nonleaf to find the right spot in the node to put in the key/pageid
	if(current_data_to_enter.key < node_new->keyArray[0]){
		insert_into_leaf(node_old, current_data_to_enter);
		child_data->pageNo = newNum;
		child_data->key = node_old.keyArray[middle_key-1];
	}
	else if(current_data_to_enter.key >= node_new->keyArray[0]){
		insert_into_leaf(*node_new, current_data_to_enter);
		child_data->pageNo = newNum;
		child_data->key=node_new->keyArray[0];
		// then shift everything to the left
	}
	
	node_new->rightSibPageNo = node_old.rightSibPageNo;
	node_old.rightSibPageNo = newNum;

	// Unpins the pages that we don't need anymore, including the new page we just made.
	bufMgr->unPinPage(file, page_num_old, true);
	bufMgr->unPinPage(file, newP->page_number(), true);
}

// took out operator parameter
PageId BTreeIndex::findLeastPageId(NonLeafNodeInt node, int lowValParam) {

	int keyArrLength = sizeof(node.keyArray) / sizeof(node.keyArray[0]);
	int key; 
	
	for (int keyIndex = 0; keyIndex < keyArrLength; keyIndex++)
	{		
		key = node.keyArray[keyIndex];
		if(lowValParam < key) return node.pageNoArray[keyIndex];
	}
	return node.pageNoArray[keyArrLength];
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	/*
	This method is used to begin a “filtered scan” of the index. For example, if the
	method is called using arguments (1,GT,100,LTE), then the scan should seek all
	entries greater than 1 and less than or equal to 100.
	*/

	if(lowValParm > highValParm) throw BadScanrangeException();
	if(lowOpParm != Operator::GT && lowOpParm != Operator::GTE) throw BadOpcodesException();
	if(highOpParm != Operator::LT && highOpParm != Operator::LTE) throw BadOpcodesException();

	scanExecuting = true;

	// Sets data in the provided parameters for scanNext()
	lowValInt	= *reinterpret_cast<const int*>(lowValParm);
	highValInt	= *reinterpret_cast<const int*>(highValParm);
	lowOp 		= lowOpParm;
	highOp		= highOpParm;

	// Get the root to start the scan
	NonLeafNodeInt nextNode = getRootNode();
	LeafNodeInt leaf;
	PageId traversalPageId = rootPageNum;

	// scan continues until a proper leaf node is found
	while (1)
	{
		// if the next node is the last level, then the next level has leaf nodes.
		PageId pageId = findLeastPageId(nextNode, lowValInt);
		
		if(nextNode.level) {
			Page* p;
			bufMgr->readPage(file, pageId, p);
			leaf = *reinterpret_cast<LeafNodeInt*>(p);

			// if it's not the root, unpin the last nonleafnode
			if(traversalPageId != rootPageNum) {
				bufMgr->unPinPage(file, traversalPageId, false);
			}

			// this is the pageNum we're looking for
			currentPageNum = pageId;
			currentPageData = p;
		}
		else {
			// if it's not the root, unpin the last nonleafnode
			if(traversalPageId != rootPageNum) {
				bufMgr->unPinPage(file, traversalPageId, false);
			}
			traversalPageId = pageId;
			nextNode = getNonLeafNodeFromPage(pageId);
		}
	}

	// scan continues until a proper leaf node is found for scanNext
	int leafRidLength = sizeof(leaf.ridArray) / sizeof(leaf.ridArray[0]);
	for (int keyIndex = 0; keyIndex < leafRidLength; keyIndex++)
	{
		// Determine correct operator comparison
		int key = leaf.keyArray[keyIndex];
		bool comparison;
		if(lowOp == Operator::GT) 
			comparison = key > lowValInt;
		else if(lowOp == Operator::GTE)
			comparison = key >= lowValInt;

		// sets the next entry + sets page data
		if(comparison) {
			nextEntry = keyIndex;
			break;
		}
	}

	// nothing satisifies the scan
	nextEntry = -1;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
	/*
	This method fetches the record id of the next tuple that matches the scan crite-
	ria. If the scan has reached the end, then it should throw the following excep-
	tion: IndexScanCompletedException. For instance, if there are two data en-
	tries that need to be returned in a scan, then the third call to scanNext must throw
	IndexScanCompletedException. A leaf page that has been read into the buffer
	pool for the purpose of scanning, should not be unpinned from buffer pool unless
	all records from it are read or the scan has reached its end. Use the right sibling
	page number value from the current leaf to move on to the next leaf which holds
	successive key values for the scan.
	*/
	
	if(nextEntry < 0) throw IndexScanCompletedException();
	if(!scanExecuting) throw ScanNotInitializedException();

	LeafNodeInt leaf = *reinterpret_cast<LeafNodeInt*>(currentPageData);
	int leafRidLength = sizeof(leaf.ridArray) / sizeof(leaf.ridArray[0]);

	// if it's out of bounds of the array, move on to the next page, or throw an error
	if(nextEntry >= leafRidLength) {
		// I assume that if rightSibPageNo = 0, then it's "null", indicating that there
		// is no next page. If that is the case, the scan must be done.
		if(leaf.rightSibPageNo == 0) {
			throw IndexScanCompletedException();
		}

		// unpins a page when all records from it are read
		bufMgr->unPinPage(file, currentPageNum, false);

		PageId nextPage = leaf.rightSibPageNo;
		currentPageNum = nextPage;
		bufMgr->readPage(file, nextPage, currentPageData);
		nextEntry = 0;

		// The idea is we redo the process now that the next page is set.
		scanNext(outRid); 
		return;
	}

	// No need to check for greater than, because that has already happened in the
	// startScan function. We only need to check lesser than.
	int key = leaf.keyArray[nextEntry];
	bool comparison;
	if(highOp == Operator::LT) 
		comparison = key < highValInt;
	else if(highOp == Operator::LTE)
		comparison = key <= highValInt;

	// if the comparison holds true, return it and go to the next entry.
	// otherwise, our scan is completed.
	if(comparison) {
		outRid = leaf.ridArray[nextEntry];
		nextEntry++;
	}
	else {
		// unpins a page when scan has reached its end
		throw IndexScanCompletedException();
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
	/*
	This method terminates the current scan and unpins all the pages that have been
	pinned for the purpose of the scan. It throws ScanNotInitializedException
	when called before a successful startScan call.  
	*/

	if(!scanExecuting) {
		throw ScanNotInitializedException();
	}

	/* The only file that is pinned is the currrentPageNum, which we won't need anymore.
	 * The only places that currentPageNum is unpinned is in endScan and nextScan.
	 * In nextScan, the page is NOT UNPINNED when IndexScanCompletedException is thrown.
	 * It is only unpinned in nextScan when there is for sure a next page to get.
	 *
	 * This is a design choice because it is:
	 *	1) lazy
	 * 	2) makes the complexity of page pinning easier among the two functions
	 * 	3) keeps the "end" in the endScan function
	*/
	bufMgr->unPinPage(file, currentPageNum, false);

	// in our current implementation, root is kept pinned until destructor is called
	// no other pages are kept pinned throughout entirety of scan
	// bufMgr->unPinPage(file, rootPageNum, false);

	scanExecuting = false;
}

}

