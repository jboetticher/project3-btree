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
		// index file already exists:
		file = &BlobFile::open(indexName);

		// read meta info (btree.h:108)
		IndexMetaInfo metaInfo = *(IndexMetaInfo*)(&file->readPage(0));
		rootPageNum = metaInfo.rootPageNo;
		file = &BlobFile::create(indexName);
	}
	catch(FileNotFoundException exists) {
		// index file doesn't already exist:
		IndexMetaInfo newInfo;
		newInfo.relationName = relationName;
		newInfo.attrByteOffset = attrByteOffset;
		newInfo.attrType = attrType;
		newInfo.rootPageNo = 1;

		// hmmm
		IndexMetaInfo *metaPtr = &newInfo;
		Page* metaPage = (Page*) metaPtr;
		file->writePage(0, metaPage);

		NonLeafNodeInt root;
		root.level = 0;
		Page rootPage = (Page) root;
		file->writePage(1, rootPage);

		rootPageNum = 1;
	}



	// the constructor should scan relationName and insert entries
	// for all of the tuples in the relation into the index
	FileScan scan(relationName, bufMgr);
	try {
		RecordId nextRec;

		while(true) {
			scan.scanNext(nextRec);
			
			// --- The following is taken from main.cpp:121 ---
			// Assuming RECORD.i is our key, lets extract the key, which we know is 
			// INTEGER and whose byte offset is also know inside the record. 
			std::string recordStr = scan.getRecord();
			const char *record = recordStr.c_str();
			int key = *((int *)(record + attrByteOffset))); // offsetof (attributeType, i)));
			
			insertEntry(key, nextRec);
		}
	}
	catch(EndOfFileException end) {
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

	// clearing up any state variables
	

	// unpinning any B+ tree pages that are pinned
	startScan();

	// flushing the index
	bufMgr->flushFile();
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

NonLeafNodeInt BTreeIndex::getNonLeafNodeFromPage(PageId pageId) {
	Page* p = &file->readPage(pageId);
	NonLeafNodeInt* node = (NonLeafNodeInt*)p;
	return *node;
}

NonLeafNodeInt BTreeIndex::getRootNode() {
	return getNonLeafNodeFromPage(rootPageNum);
}

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	/*
	Start from root and recursively search for which leaf, key belongs to
	If leaf is full then split leaf, update parent non-leaf, and if root needs splitting then update metadata
	Might have to keep track of height here
	*/
	// You have to add the node page to the buffer manager if there is space
	NonLeafNodeInt root = getRootNode();
	
	LeafNodeInt key_node;
	key_node.keyArray[0] = *(int*)key;
	key_node.ridArray[0] = rid;

	Page key_page = (Page) key_node;
	file->writePage(####, key_node);

	if (key < root.keyArray[0]) {
		for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
			if (root.pageNoArray[i] == NULL){
				root.pageNoArray[i] = key_page.page_number();
				break;}
			else if(root.pageNoArray[INTARRAYLEAFSIZE] != NULL) {
				
			}
		}
	}
	else if (key > rootPageNum)
	{
		
	}
}

PageId BTreeIndex::findLeastPageId(NonLeafNodeInt node, int lowValParam, Operator greaterThan) {
	int keyArrLength = sizeof(node.keyArray) / sizeof(node.keyArray[0]);
	for (int i = 0; i < keyArrLength; i++)
	{
		int key = node.keyArray[i];
		bool comparison;
		
		// Determine correct operator comparison
		if(greaterThan == Operator::GT) 
			comparison = key > lowValParam;
		else if(greaterThan == Operator::GTE)
			comparison = key >= lowValParam;
		else {
			throw BadOpcodesException();
		}

		// if i = 0, then it can only return key[0].
		// if i > 0, then it can return key[i-1], because there may be elements before.
		// Example:	10 >=					12
		//				8, 23		  	  29, 35	       42, 50
		//		8, 9, 10	23, 25	 29, 31    35, 40   42, 45   50, 51	
		// If we chose 23 on level 1, then we would have skipped 10 on level 2.
		if(comparison) {
			if(i > 0) return node.pageNoArray[i - 1];
			else return node.pageNoArray[i];
		}
		else if(i == keyArrLength - 1) {
			// if it's the very last in the b+ tree, then we can only go to the very right.
			return node.pageNoArray[i];
		}
	}
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

	// TODO: check if scan has already started?

	scanExecuting = true;

	// Sets data in the provided parameters for scanNext()
	lowValInt	= *(int*)lowValParm;
	highValInt	= *(int*)highValParm;
	lowOp 		= lowOpParm;
	highOp		= highOpParm;

	// Get the root to start the scan
	NonLeafNodeInt nextNode = getRootNode();
	int highestLevel = 3; // TODO: find the highest level so that it can cast to leaf node
	LeafNodeInt leaf;

	// scan continues until a proper leaf node is found
	while (1)
	{
		if(nextNode.level + 1 == highestLevel) {
			// if the next node is the last level, then the next level has leaf nodes.
			PageId pageId = findLeastPageId(nextNode, lowValInt, lowOpParm);
			Page* p = &file->readPage(pageId);
			leaf = *(LeafNodeInt*)p;

			// it also happens to have the pageNum we're looking for
			currentPageNum = pageId;
			currentPageData = p;
			break;
		}
		else {
			PageId pageId = findLeastPageId(nextNode, lowValInt, lowOpParm);
			nextNode = getNonLeafNodeFromPage(pageId);
		}
	}

	// scan continues until a proper leaf node is found for scanNext
	int leafRidLength = sizeof(leaf.ridArray) / sizeof(leaf.ridArray[0]);
	for (int i = 0; i < leafRidLength; i++)
	{
		// Determine correct operator comparison
		int key = leaf.keyArray[i];
		bool comparison;
		if(lowOp == Operator::GT) 
			comparison = key > lowValInt;
		else if(lowOp == Operator::GTE)
			comparison = key >= lowValInt;

		// sets the next entry + sets page data
		if(comparison) {
			nextEntry = i;
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

	LeafNodeInt leaf = *(LeafNodeInt*)(currentPageData);
	int leafRidLength = sizeof(leaf.ridArray) / sizeof(leaf.ridArray[0]);

	// move on to the next page, or throw an error
	if(nextEntry >= leafRidLength) {
		// I assume that if rightSibPageNo = 0, then it's "null", indicating that there
		// is no next page. If that is the case, the scan must be done.
		if(leaf.rightSibPageNo == 0) throw IndexScanCompletedException();

		PageId nextPage = leaf.rightSibPageNo;
		currentPageNum = nextPage;
		currentPageData = &(file->readPage(nextPage));
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

	// TODO: unpin all the pages

	scanExecuting = false;
}

}
