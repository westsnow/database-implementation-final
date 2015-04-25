#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <sstream>
#include <iostream>
#include <vector>
#include <string.h>
#include <string>
#include "Pipe.h"
#include "File.h"
#include "Record.h"



struct thread_info {    /* Used as argument to thread_start() */
   Pipe *runBuffer;
   int runLen;
   int index;
   int pageCount;
   int id;
   char* fileName;
};

struct InputInfo{
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *order;
	int runLen;
	char *fileName;
};

struct InnerPipeStruct
{
	Pipe* pipe;
	char *fileName;
	std::vector<int> *recordCount;
	std::vector<int> *pageCount;
};

using namespace std;



class BigQ {

public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

private:
	ComparisonEngine 	ceng;
	std::string fileName;
};

#endif
