#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <pthread.h>
#include <vector>

struct SelectFileStruct{
	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
};

struct SelectPipeStruct{
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
};

struct DuplicateRemovalStruct{
	Pipe *inPipe;
	Pipe *outPipe;
	Schema *schema;
};

struct WriteOutStruct{
	Pipe *inPipe;
	FILE *file;
	Schema *schema;
};

struct ProjectStruct{
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
};
//	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)

struct JoinStruct
{
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *cnf;
	Record *literal;
};

struct SumStruct{

	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;	

};

struct GroupByStruct{

	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *groupAtts;
	Function *computeMe;
	int runLen;

};


class RelationalOp {
protected:
	pthread_t worker_thread;
	//int runLen;
public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone ();

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};


class SelectFile : public RelationalOp { 
	 //Record *buffer;
public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void Use_n_Pages (int n);
};


class SelectPipe : public RelationalOp {

private:
	pthread_t worker_thread;

public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void Use_n_Pages (int n);
};


class Project : public RelationalOp { 
public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void Use_n_Pages (int n);
};
class Join : public RelationalOp { 
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void Use_n_Pages (int n) { }
};
class DuplicateRemoval : public RelationalOp {
public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void Use_n_Pages (int n) { }
};
class Sum : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	//void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	//void WaitUntilDone () { }
	void Use_n_Pages (int n){ };
};
class WriteOut : public RelationalOp {
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void Use_n_Pages (int n) { }
};
#endif
