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
	this.attrByteOffset = attrByteOffset;

	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str(); // indexName is the name of the index file
	outIndexName = indexName;

	// if indexName exists, then the file is opened. Else, a new index file is created.
	try {
		file = &(BlobFile.create(indexName));

		// index file doesn't already exist:
		IndexMetaInfo newInfo;
		newInfo.relationName = relationName;
		newInfo.attrByteOffset = attrByteOffset;
		newInfo.attrType = attrType;
		newInfo.rootPageNo = 1;
		
		Page metaPage = (Page) newInfo;
		file->writePage(0, metaPage);

		NonLeafNodeInt root;
		root.level = 0;
		Page rootPage = (Page) root;
		file->writePage(1, rootPage);

		rootPageNum = 1;
	}
	catch(FileExistsException exists) {
		// index file already exists:
		file = &(BlobFile.open(indexName));

		// read meta info (btree.h:108)
		IndexMetaInfo metaInfo = (IndexMetaInfo)(file->readPage(0));
		rootPageNum = metaInfo.rootPageNo;
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

/**
 * @brief Gets a leaf node, assumes that it's a non leaf node.
 * 
 * @param pageId the pageId of the non leaf node
 * @return NonLeafNodeInt the struct representing the non leaf node
 */
NonLeafNodeInt BTreeIndex::getNonLeafNodeFromPage(PageId pageId) {
	Page* p = &(blobFile.readPage(pageId));
	NonLeafNodeInt node = (NonLeafNodeInt)(p);
	return node;
}

/**
 * @brief Gets the root node
 * 
 * @return NonLeafNodeInt 
 */
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
	NonLeafNodeInt root = getRootNode();
	
	if (BTreeIndex rootPageNum == NULL) {
	NonLeafNodeInt root = getRootNode();
	}
	
	if (key < rootPageNum) {
		insertEntry()
	}
	else if (key > rootPageNum)
	{
		
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
	if(lowValParm > highValParm) throw BadScanrangeException();
	if(lowOpParm != Operator.LT && lowOpParm != Operator.LTE) throw BadOpcodesException();
	if(highOpParm != Operator.GT && highOpParm != Operator.GTE) throw BadOpcodesException();

	// TODO: check if scan has already started?

	scanExecuting = true;
	

	// Sets data in the provided parameters for scanNext()
	lowValInt	= *(int*)lowValParm;
	highValInt	= *(int*)highValParm;
	lowOp 		= lowOpParm;
	highOp		= highOpParm;

	// Get the root to start the scan
	NonLeafNodeInt root = getRootNode();

	/*
	This method is used to begin a “filtered scan” of the index. For example, if the
	method is called using arguments (1,GT,100,LTE), then the scan should seek all
	entries greater than 1 and less than or equal to 100.
	*/
	
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
	
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
