#include "DBFile.h"
#include <fstream>
#include <string>
#include <iostream>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
// stub file .. replace it with your own DBFile.cc
#define TRUE 1
#define FALSE 0

GeneralDBFile::GeneralDBFile(){
	printf("base con is called\n");
	page_number = 0;
}

GeneralDBFile::~GeneralDBFile(){
	printf("base decon is called\n");
}
//Heap
Heap::Heap(): GeneralDBFile(){

}

Heap::~Heap(){
	//asjndn
}


int Heap::Create (char *f_path, fType f_type, void *startup) {

	File file;

	ofstream header_file;

	// construct path of the header meta-data file
	char header_path[100];
	sprintf (header_path, "%s.header", f_path);
	// write meta-data file depending on type of file
	header_file.open(header_path);
	if(!header_file.is_open())
		return 0;
	header_file <<"heap"<<endl;
	header_file.close();


	file.Open(0,f_path);
	file.Close();
	opened_file.Open(1, f_path);
	return 1;
}


int Heap::Load (Schema &f_schema, char *loadpath) {

	if( !opened_file.isOpen())
		return 0;


	Page page_buffer = Page();

	
	FILE *tableFile = fopen (loadpath, "r");
	Record tmp;
	//open file to write records
   	while(tmp.SuckNextRecord(&f_schema, tableFile) ){
		//tmp.Print(&lineitem);
		if(page_buffer.Append(&tmp)){
		
		}
		else{
			opened_file.AddPageToEnd(&page_buffer);
			page_buffer.EmptyItOut();
			page_buffer.Append(&tmp);
		}
	}
	opened_file.AddPageToEnd(&page_buffer);
	page_buffer.EmptyItOut();
	// cout<<"file has "<<opened_file.GetLength()<<" pages"<<endl;
	return 1;

}

int Heap::Open (char *f_path) {
	
	opened_file.Open(1, f_path);
	//cout<<"there are "<<opened_file.GetLength()<<" pages in the file"<<endl;
	if( !opened_file.isOpen())
		return 0;

	return 1;
}

void Heap::MoveFirst () {
	page_number = 0;
	if(opened_file.GetLength()==0)
		return;
	if(curr_page.numRecs > 0) curr_page.EmptyItOut();
	opened_file.GetPage(&curr_page, page_number);
}

int Heap::Close () {
	//cout<<"Closing DBFile";
	opened_file.Close();
	return 1;
}

int Heap::Add (Record &rec) {
	//rec.Print( new Schema ("/Users/Migue/Documents/workspace/database-implementation/source/catalog", "part")  );
	Page oPage = Page();

	opened_file.GetPage(&oPage, opened_file.GetLength() - 2);

	//if the last page is full
	if( !oPage.Append(&rec) ){
		Page newPage = Page();
		newPage.Append(&rec);
		opened_file.AddPageToEnd(&newPage);
	}else{
		opened_file.AddPage(&oPage, opened_file.GetLength() - 2);
	}
	return 1;
}

int Heap::GetNext (Record &fetchme) {
	if(curr_page.numRecs > 0) {
		curr_page.GetFirst(&fetchme);
	}
	else{
		page_number++;
		if(opened_file.GetLength()> (page_number + 1) ){
			opened_file.GetPage(&curr_page, page_number);
			curr_page.GetFirst(&fetchme);
		}
		else return 0;
	}
	return 1;
}

int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//??? consume
	ComparisonEngine comp;
	while (GetNext(fetchme)){
		if (comp.Compare(&fetchme, &literal, &cnf)){
			return 1;
		}
	}

	return 0;
}

//Sorted
Sorted::Sorted() {
	printf("sorted con is callled\n");
	state = reading;
}

Sorted::~Sorted(){
	printf("sort decno is called\n");
	if(bigQ != NULL)
		delete bigQ;
	if(inpipe != NULL)
		delete inpipe;
	if(outpipe!= NULL)
		delete outpipe;
}

int Sorted::Create (char *f_path, fType f_type, void *startup) {
	



	
	File 			file;
	ofstream 		header_file;
	SortInfo 		*si = (SortInfo *) startup;

	// construct path of the header meta-data file
	char header_path[100];
	sprintf (header_path, "%s.header", f_path);
	// write meta-data file depending on type of file

	header_file.open(header_path);
	if(!header_file.is_open())
		return 0;
	header_file <<"sorted"<<endl;
	header_file <<si->runLength<<endl;
	header_file <<si->myOrder->numAtts<<endl;

	for(int i=0;i<si->myOrder->numAtts;i++)
		header_file <<si->myOrder->whichAtts[i]<<endl;

	for(int i=0;i<si->myOrder->numAtts;i++)
		header_file <<si->myOrder->whichTypes[i]<<endl;

	header_file.close();


	file.Open(0,f_path);
	file.Close();
	opened_file.Open(1, f_path);
	return 1;

}

int Sorted::Open (char *f_path) {

	// printf("im still here\n");
	// si->myOrder->Print();
	printf("run len: %d\n", si->runLength);
	
	opened_file.Open(1, f_path);
	state = reading;
	MoveFirst();



	// printf("im still here 2\n");
	// si->myOrder->Print();
	// cout<<"there are "<<opened_file.GetLength()<<" pages in the file"<<endl;
	return 1;

}


void Sorted::MoveFirst () {
	switchToReadMode();
  	page_number = 0;

// printf("the length is %d\n", opened_file.GetLength());
	if(opened_file.GetLength()==0)
		return;
	if(curr_page.numRecs > 0) curr_page.EmptyItOut();
	opened_file.GetPage(&curr_page, page_number);
// printf("move first done\n");
}

int Sorted::Close () {
	switchToReadMode();
	opened_file.Close();
	return 1;
}

int Sorted::switchToReadMode() {
	if(state == writting){
		state = reading;
// 		printf("Am I still here?\n");
// 	    si->myOrder->Print();
/*
printf("here\n");
		Record temp;
	    if(opened_file.GetLength() != 0){
	      MoveFirst();
	      while(GetNext(temp)){
	        inpipe->Insert(&temp);
	      }
	    }
printf("what the fuck\n");
	    si->myOrder->Print();
	    inpipe->ShutDown();
	    opened_file.Close();
	    opened_file.Open(0,cur_path);

		curr_page.EmptyItOut();
		page_number = 0;
printf("begin to consume\n");
		while(outpipe->Remove(&temp)){
			printf("consume from outpipe\n");
	      if(!curr_page.Append(&temp)){
	        opened_file.AddPage(&curr_page, page_number++);
	        curr_page.EmptyItOut();

	        curr_page.Append(&temp);
	      }
	      
	    }
	    
	    opened_file.AddPage(&curr_page, page_number);
    	curr_page.EmptyItOut();
	    delete inpipe;
	    delete outpipe;
	    delete bigQ;
	    cout<<"deleted pointers"<<endl;

	    inpipe = NULL;
	    outpipe = NULL;
	    bigQ = NULL;
		    //close in_pipe
	    
	}
*/


		//close inpipe, then the bigq will start phase 2
		inpipe->ShutDown();
// printf("ShutDown done\n");
		//do two way merge, merge records from putpipe and current dbfile into a new file.
		//create a new file
		File newFile;
		char new_path[100];
		sprintf (new_path, "%s.newFile", cur_path);
// printf("before open\n");
		newFile.Open(0, new_path);
// printf("new fiel opended\n");
		//begin merging
		Page page;
		Record pipeRec;
		Record fileRec;
		Heap heapFile;
		opened_file.Close();
		// play a little trick, treat this current file as a heap dbfile, whose interface will make life easier.
		heapFile.Open(cur_path);
// printf("begin to mvoe first\n");
// printf("before\n");
		heapFile.MoveFirst();
		while(true){
// printf("here\n");
			Record* tmp = NULL;
			if(pipeRec.isNULL())
				outpipe->Remove (&pipeRec);
			if(fileRec.isNULL())
				heapFile.GetNext(fileRec);
			if(  pipeRec.isNULL() && fileRec.isNULL()){
				if(!page.isEmpty())
					newFile.AddPageToEnd(&page);
				break;
			}
			if( pipeRec.isNULL())
				tmp = &fileRec;
			if(fileRec.isNULL())
				tmp = &pipeRec;
			if(tmp == NULL){
				tmp = ceng.Compare(&pipeRec, &fileRec, si->myOrder) == 1 ? &fileRec : &pipeRec;
			}
			//add tmp to the new file;
			if( !page.Append(tmp) ){
				newFile.AddPageToEnd(&page);
				page.EmptyItOut();
				page.Append(tmp);
			}
		}
		newFile.Close();
		opened_file.Close();
		remove( cur_path );
		rename(new_path, cur_path);
		opened_file.Open(1, cur_path);


		delete inpipe;
	    delete outpipe;
	    delete bigQ;

	    inpipe = NULL;
	    outpipe = NULL;
	    bigQ = NULL;
	 }

	
	return 1;
}


int Sorted::switchToWriteMode(){
	if(state == reading){
		state = writting;
		int buffsz = 128;
		// printf("creating new staff\n");
		inpipe = new Pipe(buffsz);
		outpipe = new Pipe(buffsz);
		// printf("was I passed to bigq?\n");
		// si->myOrder->Print();
		bigQ = new BigQ(*inpipe, *outpipe, *(si->myOrder), si->runLength);
	}
	return 1;
}
int Sorted::Add (Record &rec) {
// printf("add function is called... current state is %d\n",state );
	// printf("before i went to switch to write mode\n");
	// si->myOrder->Print();
	switchToWriteMode();
	inpipe->Insert (&rec);
	return 1;
}

int Sorted::Load (Schema &f_schema, char *loadpath) {

	if( !opened_file.isOpen())
		return 0;


	FILE *tableFile = fopen (loadpath, "r");
	Record tmp;
	//open file to write records

   	while(tmp.SuckNextRecord(&f_schema, tableFile) ){
		Add(tmp);
	}
	// cout<<"file has "<<opened_file.GetLength()<<" pages"<<endl;
	return 1;

}


int Sorted::GetNext (Record &fetchme) {
// printf("get next called\n");
	switchToReadMode();
	// printf("get next called 2\n");
	if(curr_page.numRecs > 0) {
		curr_page.GetFirst(&fetchme);
	}
	else{
		page_number++;
		if(opened_file.GetLength()> (page_number + 1) ){
			opened_file.GetPage(&curr_page, page_number);
			curr_page.GetFirst(&fetchme);
		}
		else return 0;
	}
	return 1;
}

int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
	// check if the file is in writing mode, if yes,
        // merge the records from the file and the pipe!
  //   if (state = writting) {
		// switchToReadMode();     
  //   }

	// check if the OrderMaker for the sorted file can be used to
	// GetNext the record by evaulating the CNF passed alongwith
	// if no, continue the linear scan and spit out results, if yes,
	// build a "query" OrderMaker based on the CNF that is passed
	OrderMaker cnfOrder, literalOrder;
	bool usable = cnf.GetSortOrders(*(si->myOrder), cnfOrder, literalOrder);

	//cout << "CNF Order: " << endl;
	//cnfOrder.Print();
	//cout << "File Order: " << endl;
	//fileOrder.Print();

	// result gives the number of attributes in cnfOrder, if none,
	// scan sequentially
	//cout << "Result: " << usable << endl;
	if (false == usable) {
		// get the record pointed by the current record which satisfies
		// the cnf passed on by the function
		ComparisonEngine comp;

		// GetNext returns all records whether they satisfy the CNF or not
		while (GetNext(fetchme)) {
			// apply cnf and filter out incorrect records
			if (comp.Compare(&fetchme, &literal, &cnf)) {
				return TRUE;
			}
		}

		return FALSE;
	}

	// perform binary search
	if (opened_file.IsFileEmpty()) {
		cout << "Trying to search on an empty sorted file!" << endl;
		return FALSE;
	}

	// we need to perform binary search from the current record
	int start = (((int)page_number)+1);
	int end = opened_file.LastUsedPageNum();
	int mid;
	int recFoundOnPageNum = (int)page_number;
	int initialPageNum = (int)page_number;
	bool scanCurrentPage = true;

	ComparisonEngine comp;

	Page pageBuffer;
	Record recBuffer;

	//cout << "Start: "<< start << " End: " << end << endl;

	// first we would check if the records in the current page should be
	// scanned or not, the idea is to check the first record on the next page
	// if that record is < than the CNF order, that means chances of finding
	// the records that may match the CNF order and in the second-half, and 
	// hence binary search should be performed. If its == or > that means
	// we should scan until we reach the next page
	if (start <= end) { // (= means that start is the last page and curpage is the second last)
		opened_file.GetPage(&pageBuffer, start);
		if (0 != pageBuffer.GetFirst(&recBuffer)) {
			// found a record, compare
			//recBuffer.Print(new Schema("catalog", "customer"));
			if ((-1) == comp.Compare(&recBuffer, &cnfOrder, &literal, &literalOrder)) {
				scanCurrentPage = false;
			}
		}
		else {
			cout << "Should have found record here!" << endl;
			return FALSE;
		}

		pageBuffer.EmptyItOut();
	}

	//cout << "Scan current page?: " << scanCurrentPage << endl;

	int result;
	if (scanCurrentPage) {
		// GetNext returns all records whether they satisfy the CNF or not
		while (GetNext(fetchme))  {
			// apply cnf and filter out incorrect records
			//if (comp.Compare(&fetchme, &literal, &cnf)) {
			//	return TRUE;
			//}
			result = comp.Compare(&fetchme, &cnfOrder, &literal, &literalOrder);
			if (0 == result) {
				return TRUE;
			}
			// if result is 1 that means you have already crossed
			// all possible records
			else if (1 == result) {
				return FALSE;
			}
		}

		// if the record was not found on the current page, there are no records
		return FALSE;
	}

	// now as the record is not present in the current page, start binary search
	while (end >= start) {

		mid = start + ((end - start)/2);
		//cout << "Mid: " << mid << endl;

		// get the record from middle page and compare with the literal
		// record, based on the comparision result move either upward
		// or downward
		curr_page.EmptyItOut();
		opened_file.GetPage(&curr_page, mid);
		page_number = mid;
		if (0 == curr_page.GetFirst(&recBuffer)) {
			cout << "No records found on page " << mid  <<endl;
			return FALSE;
		}

		result = comp.Compare(&recBuffer, &cnfOrder, &literal, &literalOrder);
		//recBuffer.Print(new Schema("catalog", "customer"));
		if ((-1) == result) {
			start = mid + 1;
			if (start > end) {
				recFoundOnPageNum = end;
				break;
			}
		} else if (1 == result) {
			end = mid - 1;
			if (end < initialPageNum) {
				recFoundOnPageNum = initialPageNum;
				break;
			}
		} else {
			recFoundOnPageNum = mid;
			end = mid - 1;
		}
	}

	//cout << "S: " << start << " E: " << end << " M: " << mid << endl;

	// we need to scan the last page which we narrowed down
	if (start > end) {
		recFoundOnPageNum = (start - 1);
	}

	//cout << "Rec found on page num: " << recFoundOnPageNum << endl;

	// the inital page was already scanned, so if the record in found on some
	// other page, scan the entire page so see if you find any record on that
	// page, if not there are no more records which satisfy the CNF
	if (recFoundOnPageNum > initialPageNum) {
		curr_page.EmptyItOut();
		opened_file.GetPage(&curr_page, recFoundOnPageNum);
		page_number = recFoundOnPageNum;

		// GetNext returns all records whether they satisfy the CNF or not
		while (GetNext(fetchme)) {
			// apply cnf and filter out incorrect records
			//if (comp.Compare(&fetchme, &literal, &cnf)) {
			//	return TRUE;
			//}

			result = comp.Compare(&fetchme, &cnfOrder, &literal, &literalOrder);
			if (0 == result) {
				return TRUE;
			}
			// if result is 1 that means you have already crossed
			// all possible records
			else if (1 == result) {
				return FALSE;
			}
		}

		// if the record was not found on the current page, there are no records
		return FALSE;
	}

	return FALSE;
}

//DBFile



DBFile::DBFile () {
	generalVar = NULL;
}

DBFile::~DBFile () {
	delete generalVar;
	//printf("dbfile decon is called\n");
}


int  DBFile::Create (char *f_path, fType f_type, void *startup) {
	switch(f_type){
		case heap:
			generalVar = new Heap();
			break;
		case sorted:
			generalVar = new Sorted();
			break;
		default:
			//do nothing
			break;
	}
	return generalVar->Create(f_path, f_type, startup);
}


int DBFile::Load (Schema &f_schema, char *loadpath) {
	return generalVar->Load(f_schema, loadpath);
}

int DBFile::Open (char *f_path) {

	string line;
	ifstream header_file;


	// construct path of the header meta-data file
	char header_path[100];
	sprintf (header_path, "%s.header", f_path);
	// write meta-data file depending on type of file
	header_file.open(header_path);
	if(!header_file.is_open())
		return 0;
	getline(header_file, line);
	// header_file.close();
	if( generalVar == NULL){
		if(line=="heap")
			generalVar = new Heap();

		else if(line == "sorted"){
			Sorted *sorted = new Sorted();
			OrderMaker *order = new OrderMaker();

			//Get Run Length from header file
			getline(header_file, line);
			int runLen = atoi(line.c_str());

			//Get Number of sorting attributes
			getline(header_file, line);
			order->numAtts = atoi(line.c_str());

			//Get Number of sorting attributes
			for(int i=0;i<order->numAtts;i++){
				getline(header_file, line);
				order->whichAtts[i] = atoi(line.c_str());
			}

			//Get Number of sorting attributes
			for(int i=0;i<order->numAtts;i++){
				getline(header_file, line);
				order->whichTypes[i] = (Type) atoi(line.c_str());
			}

			SortInfo *startup = new SortInfo();
			startup->myOrder = order;
			startup->runLength = runLen;
			sorted->si = startup;
			sorted->cur_path = f_path;
			generalVar = sorted;
		}

	}
	header_file.close();
	// printf("file opened");

	return generalVar->Open(f_path);
	//cout<<"there are "<<opened_file.GetLength()<<" pages in the file"<<endl;
}



void DBFile::MoveFirst () {
	generalVar->MoveFirst();
}

int DBFile::Close () {
	return generalVar->Close();
}

int DBFile::Add (Record &rec) {
	return generalVar->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
	return generalVar->GetNext(fetchme);
	
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return generalVar->GetNext(fetchme, cnf, literal);
}
