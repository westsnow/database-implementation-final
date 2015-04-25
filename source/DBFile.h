#ifndef DBFILE_H
#define DBFILE_H

#include <fstream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "Pipe.h"
#include "BigQ.h"

struct SortInfo{
	OrderMaker 	*myOrder;
	int 		runLength;
};

struct Pipes
{
	Pipe* inpipe;
	Pipe* outpipe;
	SortInfo* si;
};

typedef enum {heap, sorted, tree} fType;
typedef enum {reading, writting} rwState;//whether dbfile is in reading or writting mode



// stub DBFile header..replace it with your own DBFile.h 
class GeneralDBFile{
public:
	ComparisonEngine 	ceng;
	File opened_file;
	Page curr_page;
	int page_number;
	SortInfo *si;
public:
	GeneralDBFile();
	virtual ~GeneralDBFile();
	virtual int Create (char *fpath, fType file_type, void *startup) = 0;
	virtual int Open (char *fpath) = 0;
	virtual int Close () = 0;
	virtual int Load (Schema &myschema, char *loadpath) = 0;
	virtual void MoveFirst () = 0;
	virtual int Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
};

class Heap: public GeneralDBFile{
public:
	Heap();
	~Heap();
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath) ;
	int Close ();
	int Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	int Add (Record &addme) ;
	int GetNext (Record &fetchme) ;
	int GetNext (Record &fetchme, CNF &cnf, Record &literal) ;
};

class Sorted : public GeneralDBFile{
private:
	int runLength;

	rwState state;
	BigQ* bigQ;
	Pipe* inpipe;
	Pipe* outpipe;
	int switchToReadMode();
	int switchToWriteMode();
	// pthread_t bigQThread;


public:
	Sorted();
	~Sorted();
	
	char* cur_path;
	//to run bigQ
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
	int Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	int Add (Record &addme) ;
	int GetNext (Record &fetchme) ;
	int GetNext (Record &fetchme, CNF &cnf, Record &literal) ;
};


class DBFile {
public:
	GeneralDBFile* generalVar;
public:
	DBFile (); 
	~DBFile();

	//it's always a good idea to check if file exists before other operations.
	inline bool FileExists (const char* name) {
		ifstream f(name);
		if (f.good()) {
			f.close();
			return true;
		} else {
			f.close();
			return false;
		}
	}

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
	int Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	int Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
