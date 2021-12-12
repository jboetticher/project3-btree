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

	// if indexName exists, then the file is opened. Else, a new index file is created.
	try {
		BlobFile bf = BlobFile::open(indexName);
		file = &bf;
		firstPageNo = file->getFirstPageNo();
		// index file already exists:
		
		// read meta info (btree.h:108)
		Page *metaPage;
		bufMgr->readPage(file, 0, metaPage);
		IndexMetaInfo metaInfo = *reinterpret_cast<IndexMetaInfo*>(metaPage);
		rootPageNum = metaInfo.rootPageNo;

		// TODO : this shouldn't be necessary after calling open:
		// file = &BlobFile::create(indexName); 
	}
	catch(FileNotFoundException const&) {
		// index file doesn't already exist:

		file = &BlobFile::create(indexName);
		
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

	// unpin the root page. It should be the only page still pinned.
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
	Might have to keep track of height here
	*/
	// You have to add the node page to the buffer manager if there is space. Maybe use allocPage???
	// NonLeafNodeInt root = getRootNode();

	RIDKeyPair<int> current_data_to_enter;
	current_data_to_enter.set(rid, *((int *)key));

	Page* rootPage;

	bufMgr->readPage(file, rootPageNum, rootPage);
  	PageKeyPair<int> *child_data = nullptr;

	bool is_leaf;
	// have to make something for initialRootPageNum
	if(initialRootPageNum == rootPageNum){
		is_leaf = true;
	}
	else{is_leaf=false;}

	search(rootPage, rootPageNum, isleaf, current_data_to_enter, child_data);
}
//Under Construction
void BTreeIndex::search(Page *Page_currently, PageId Page_number_currently, bool is_leaf, const RIDKeyPair<int> current_data_to_enter
, PageKeyPair<int> *&child_data){
	if(!is_leaf) {
		NonLeafNodeInt *Node_currently = reinterpret_cast<NonLeafNodeInt *>(Page_currently);
		Page *page_next;
		PageId node_next_number;
		NextNonLeafNode(Node_currently, node_next_number, current_data_to_enter.key);
		bufMgr->readPage(file, node_next_number, page_next);
		is_leaf = Node_currently->level == 1;

		// Recursive step
		search(page_next, node_next_number, is_leaf, current_data_to_enter, child_data);

		if (child_data == nullptr){
			bufMgr->unPinPage(file,Page_number_currently,false);
		}
		else{
			if(Node_currently->pageNoArray[nodeOccupancy]==0){
				insert_into_nonleaf(Node_currently, child_data);
				child_data == nullptr;
				bufMgr -> unPinPage(file,Page_number_currently,true);
			}
			else {
				// making this now
				split_nonleaf_node();
			}
		}

	}
	else {
		LeafNodeInt *leaf_node = reinterpret_cast<LeafNodeInt *>(Page_number_currently);
		if (leaf_node->ridArray[leafOccupancy - 1].page_number == 0)
			{
			// Have to make this function
			insert_into_leaf(leaf_node, current_data_to_enter);
			bufMgr->unPinPage(file, Page_number_currently, true);
			child_data = nullptr;
			}
		else {
			// have to make this function
			splitter_leaf(leaf_node, Page_number_currently, child_data, current_data_to_enter);
		}
	}
}
void BTreeIndex::NextNonLeafNode(NonLeafNodeInt *Node_currently, PageId &child_pageId, int key){ // get non leaf child
	int keyIndex = nodeOccupancy - 1;
	// We traverse through the current node to find the last full key slot
	while((keyIndex>=0) && (Node_currently->pageNoArray[keyIndex] == 0)) {
		keyIndex--;
	}
	// Then we traverse through the non-empty current node to find the last key that is greater than key we want to enter in
	while((keyIndex>0) && (Node_currently->keyArray[keyIndex] >= key)) {
		keyIndex--;
	}
	while()
	child_pageId = Node_currently->pageNoArray[keyIndex]; // child page id
}

void BTreeIndex::insert_into_nonleaf(NonLeafNodeInt *Node_nonleaf, PageKeyPair<int> *key_and_page){
	int keyIndex = nodeOccupancy - 1;
	while((keyIndex >= 0) && (Node_nonleaf->pageNoArray[keyIndex] == 0)){
		keyIndex--;
	}
	while((keyIndex > 0) && (Node_leaf->keyArray[keyIndex - 1] > key_and_page->key)){
		Node_nonleaf -> keyArray[keyIndex] = Node_nonleaf -> keyArray[keyIndex - 1];
		Node_nonleaf -> pageNoArray[keyIndex + 1] = Node_nonleaf -> pageNoArray[keyIndex];
		keyIndex--;
	}
	Node_nonleaf -> keyArray[keyIndex] = key_and_page->key;
	Node_nonleaf -> pageNoArray[keyIndex + 1] = key_and_page->pageNo;
}

// under construction
void BTreeIndex::split_nonleaf_node(NonLeafNodeInt node_old, PageId page_num_old, PageKeyPair<int> *&child_data){
	// We are initializing
	PageId newNum;
	Page newP;
	BufMgr->allocPage(file, newNum, newP);
	NonLeafNodeInt *node_new = reinterpret_cast<NonLeafNodeInt *>(newP);
	// INTARRAYNONLEAFSIZE might be accessible form namespace. We have to check through compiler
	int middle_key = INTARRAYNONLEAFSIZE / 2;
	PageKeyPair<int> pagekeypair_for_root;

	// NOTE: could you explain why node_old->keyArray[middle_key] is the right value?
	pagekeypair_for_root.set(newNum, node_old->keyArray[middle_key]);

	
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

	// looping through a new node made and assigning the second half of the old node to the start of this node.
	for(int i = middle_key, i < nodeOccupancy, i++){
		// node new is a new node created
		// we will assign anything that is moved from old node to new --> 0
		node_new->keyArray[i - middle_key] = node_old->keyArray[i];
		node_old->keyArray[i] = 0;
		node_new->pageNoArray[i - middle_key] = node_old->pageNoArray[i + 1];
		node_old->pageNoArray[i + 1] = reinterpret_cast<PageId>(0);
	}
	node_new->level = node_old->level;

	// If the key we want to insert is less than the 1st entry in node_new then it belongs to node_old.
	// Otherwise it belongs to node_new.
	// We use insert_into_nonleaf to find the right spot in the node to put in the key/pageid
	if(key < node_new->keyArray[0]){
		insert_into_nonleaf(node_old, child_data);
	}
	else if(key >= node_new->keyArray[0]){
		insert_into_nonleaf(node_new, child_data);
	}
	child_data = &pagekeypair_for_root;

	// Unpins the pages that we don't need anymore, including the new page we just made.
	BufMgr->unPinPage(file, page_num_old, true);
	BufMgr->unPinPage(file, newP, true);

	//splitting becomes tricky when we are at root.
	//if we split at root then the old root (node_old) is not the true root
	//if we are splitting at the root, then we must update root

	if(page_num_old == rootPageNum) {
		//gotta make this
		root_change(page_num_old, child_data);
	}
	
}

void BTreeIndex::root_change(PageId root_pageid, PageKeyPair<int> child_data){
	Page *newrootpage;
	PageId newrootpageid;
	BufMgr->allocPage(file, newrootpageid, newrootpage);
	NonLeafNodeInt *newrootnode = (NonLeafNodeInt *)newrootpage;
	
	if(initialRootPageNum == newrootpageid){
		initialRootPageNum->level = 1;
	}
	else{
		initialRootPageNum->level = 0;
	}
	newrootnode->pageNoArray[0]= root_pageid;
	newRootPage->pageNoArray[1] = child_data->pageNo;
	newRootPage->keyArray[0] = newchildEntry->key;

	Page *meta_page;
	bufMgr->readPage(file, firstPageNo, meta_page);
	IndexMetaInfo *metaInfoPage = (IndexMetaInfo *)meta_page;
  	metaInfoPage->rootPageNo = newrootpageid;
  	rootPageNum = newrootpageid;
	bufMgr->unPinPage(file,firstPageNo,true);
	bufMgr->unPinPage(file,newrootpageid,true);
}

// This might be able to replace find next nonleaf node function
PageId BTreeIndex::findLeastPageId(NonLeafNodeInt node, int lowValParam, Operator greaterThan) {
	// Is this nodeOccupancy?
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
		PageId pageId = findLeastPageId(nextNode, lowValInt, lowOpParm);
		
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

	// NOTE: I'm not sure how to "unpin" a page. I assume if there were a funciton that
	// did that, then you would do it in the second if statement (nextEntry >= leafRidLength).
	
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

		// I'm getting recursive here. Not sure if I did the & right
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

