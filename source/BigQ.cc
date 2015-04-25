#include "BigQ.h"

using namespace std;

ComparisonEngine 	ceng;

//std::vector<int> pageCount;

void swap(std::vector<Record*> &run, int i, int j){
	if(i == j)
		return;
	Record* tmp = run[i];
	run[i] = run[j];
	run[j] = tmp;
}

int partition(std::vector<Record*> &run, int start, int end, OrderMaker &sortorder){
	int i = start -1;
	int j = start;
	swap(run, start, end);
	while(j < end){
		if( ceng.Compare(run[j], run[end], &sortorder) == -1 ){
			swap(run, ++i, j++);
		}else{
			j++;
		}
	}
	swap(run, ++i, end);
	return i;
}

void myQuicksort(std::vector<Record*> &run, int start, int end, OrderMaker &sortorder){
	if(start >= end){
		return;
	}
	int index = partition(run, start, end, sortorder);
	myQuicksort(run, start, index -1, sortorder);
	myQuicksort(run, index + 1, end, sortorder);
}

// 3 2 1 5
void sortVector(std::vector<Record*> &run, OrderMaker &sortorder){
	if(run.size() < 2)
		return;
	myQuicksort(run, 0, run.size()-1,  sortorder);
}



void* producerRunPipe(void *arg){
	thread_info *ti = (thread_info *) arg;

	char* fileName = ti->fileName;
	File tmpFile;
	
	tmpFile.Open(1, fileName );


	int currPage = 0;

	//number pages in file, used in case I hit the last page
	int fileSize = tmpFile.GetLength();
	Page p;
	int recCount = 0;
	//While there are pages left to produce on this run
	while(  currPage < ti->pageCount
		 ){
		//Get current page
		tmpFile.GetPage(&p, (ti->index + currPage ));
		Record *r = new Record();
		//printf("run %d, page %d, size: %d \n", ti->id, currPage, p.numRecs);
		recCount +=p.numRecs;

		while(p.GetFirst(r)){
			// printf("run %d inserting into pipe \n", ti->index);

			ti->runBuffer->Insert(r);
		}

		currPage++;
	}
	tmpFile.Close();


	// printf("file has %d pages", fileSize);
	ti->runBuffer->ShutDown();
}

void initializeHeap(std::vector<Pipe*> *runBuffers, std::vector<Record*> *heap, std::vector<int> &pageCount, 
	int numRuns, int runLength, char* fileName){
	// printf("initializing \n");
	int start = 0;
	for (int i=0;i<numRuns;i++){
		int buffsz = 16;
		// printf("Initializing pipe %d, with buffer_size %d \n", i,buffsz);
		Pipe *runPipe = new Pipe(buffsz);

		// printf("Initializing Producer Thread \n");
		pthread_t runThread;

		thread_info *ti = new thread_info();
		ti->runBuffer = runPipe;
		ti->runLen = runLength;
		ti->index = start;
		ti->id = i;
		ti->pageCount = pageCount[i];
		ti->fileName = fileName;

		pthread_create (&runThread, NULL, producerRunPipe, (void *)ti);

		// printf("Storing pipe pointer \n");

		runBuffers->push_back( runPipe) ;

		//Check here if segfault
		Record *r = new Record;
		runPipe->Remove(r);
		heap->push_back(r);
		start +=  pageCount[i];
	}

}




void* consumeInnerPipe (void *arg) {
		InnerPipeStruct* ips = (InnerPipeStruct*)arg;

        Pipe *pipe = ips->pipe;
        char* fileName = ips->fileName;
        std::vector<int> *recordCount = ips->recordCount;

        File tmpFile;
        Record tmp;
        Page page;

		tmpFile.Open(0, fileName );

        ips->pageCount->clear();
        int recordNum = 0;
        int pageNum = 1;

        int count = 0;
    while(pipe->Remove(&tmp)){
                if(!page.Append(&tmp)){
                        pageNum++;
                        tmpFile.AddPageToEnd(&page);
                        page.EmptyItOut();
                        page.Append(&tmp);
                        recordNum++;
                }else{
                        recordNum++;
                }
                if(recordNum == (*recordCount)[ips->pageCount->size()]){
                        recordNum = 0;
                        if( !page.isEmpty() ){
                                //pageNum++;
                                tmpFile.AddPageToEnd(&page);
                                page.EmptyItOut();
                        }
                        ips->pageCount->push_back(pageNum);

                        pageNum = 1;
                }
        }
        if( !page.isEmpty())
                tmpFile.AddPageToEnd(&page);
        tmpFile.Close();
}


void* WorkerThread(void * arg)	{
	// printf("big q working...\n");
	// read data from in pipe sort them into runlen pages, STEP 1, run generation.
	InputInfo* info = (InputInfo*) arg;
	int runSize =0;
	int runlen = info->runLen;
	Pipe *in = info->inPipe;
	Pipe *out = info->outPipe;
	OrderMaker *order = info->order;
	char* fileName = info->fileName;

	int buffsz = 100; // pipe cache size
	Pipe innerPipe (buffsz);

	std::vector<int> recordCount;
	std::vector<int> pageCount;


	InnerPipeStruct *ips = new InnerPipeStruct();
	ips->pipe = &innerPipe;
	ips->fileName = fileName;
	ips->recordCount = &recordCount;
	ips->pageCount = &pageCount;

	pthread_t thread1;
	pthread_create (&thread1, NULL, consumeInnerPipe, (void *)ips);

	std::vector<Record*> run;

	Page page;
	Record tmp;
	int pageNum = 0;
	int totalPageNum = 0;
	int count = 0;
// Schema* sch = new Schema ("/Users/westsnow/GitHub/database-implementation/source/catalog", "nation");
	while(in->Remove(&tmp)){
		if(!page.Append(&tmp)){
				// tmp.Print(sch);
				Record recRetrive;
				//current page is full, put it to vector, waiting for sort.
				// printf("this page has %d records\n", page.getRecNum());
				while( page.GetFirst(&recRetrive)){
					// recRetrive.Print( new Schema ("/Users/westsnow/GitHub/database-implementation/source/catalog", "nation")  );
					// printf("get one rec\n");
					// printf("run's length is %d\n", run.size());
					Record *r = new Record();
					r->Copy(&recRetrive);
					run.push_back(r);
				}
				page.EmptyItOut();
				page.Append(&tmp);
				pageNum++;
				totalPageNum++;
				//printf("page num is now %d\n", pageNum);
				if(pageNum >= runlen){
					pageNum = 0;
					sortVector(run, *order);
					recordCount.push_back((int)run.size());
					// printf("vector's size is %d now\n", run.size());
					for(int i = 0; i < run.size(); ++i){
						// run[i]->Print(sch);
						innerPipe.Insert(run[i]);
						// vec[1] = run.size();
					}
					runSize++;
					run.clear();
				}
		}else{
			//printf("Appended\n");
		}
	}
	//retrive records from last page;
	Record recRetrive;
	while( page.GetFirst(&recRetrive)){
		Record *r = new Record();
		r->Copy(&recRetrive);
		run.push_back(r);
	}
	sortVector(run, *order);
	if(run.size() > 0){
		recordCount.push_back((int)run.size());
		runSize++;
		totalPageNum++;
	}
	for(int i = 0; i < run.size(); ++i){
		innerPipe.Insert(run[i]);
	}
	innerPipe.ShutDown();
    pthread_join (thread1, NULL);

    // printf("%d runs has been generated, %d runs?... there are %d total pages\n", runSize, recordCount.size(), totalPageNum);


    /*
    //try to read a page from generated file
    File myfile;
    myfile.Open(1, "./generatedRuns.tmp");
    Page p;
    int pn;
    myfile.GetPage(&p, totalPageNum-1);
    Record recP;
    while( p.GetFirst(&recP)){
		recP.Print( new Schema ("/Users/Migue/Documents/workspace/database-implementation/source/catalog", "part")  );
	}*/


	// printf("\ndone... phase one there are %d runs\n", runSize);



	/*
		PHASE TWO -  MERGE RUNS
	*/

	printf("\nstart... phase two\n");
	std::vector<Record*>	heap;
	std::vector<Pipe*> 		runBuffers;


	//Initialize one pipe per run, and creates a producer for each pipe
	//Each producer will extract rows from its run in the file and feed the pipe
	//Heap is initialized with first row of each run (consume from its runBuffer[i])
	initializeHeap(&runBuffers, &heap, pageCount, runSize, runlen, fileName);


	Record 				*min;
	int 				minIndex;

	int 				cont = 0;
	while(heap.size()>0){ //While there is something to extract

		min = heap[0];
		minIndex = 0;
		for(int i=1; i<heap.size(); i++){
			//printf("comp %d \n",ceng.Compare(min, heap[i], &sortorder));
			if(ceng.Compare(min, heap[i], order) == 1){
				min = heap[i];
				minIndex = i;
			}
		}

		cont++;
		out->Insert(min);

		if(runBuffers[minIndex]->Remove( heap[minIndex] ) == 0){
			// printf("Consumed Run \n");
			runBuffers.erase(runBuffers.begin()+minIndex);
			heap.erase(heap.begin()+minIndex);

		}






	}

	printf("file name is %s\n", fileName);
	printf("phase two finished \n");

	remove(fileName);
	out->ShutDown ();
	printf("out pipe shut down\n");

	pthread_exit(NULL); 

}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen){


	std::ostringstream address;
	address << (void const *)this;
	fileName = address.str();
	char *cstr = new char[fileName.length() + 1];
	strcpy(cstr, fileName.c_str());

	InputInfo *inputInfo = new InputInfo();
	inputInfo->inPipe = &in;
	inputInfo->outPipe = &out;
	inputInfo->order = &sortorder;
	inputInfo->runLen = runlen;
	inputInfo->fileName = cstr;
	pthread_t worker;

	pthread_create(&worker, NULL, &WorkerThread, (void*)inputInfo );
}

BigQ::~BigQ () {
	// remove(fileName);
}
