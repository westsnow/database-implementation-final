#include "RelOp.h"

using namespace std;
int countI = 0;


void* SelectFileWorkerThread(void *arg){
	SelectFileStruct *sf = (SelectFileStruct *) arg;
	Record r;
	ComparisonEngine comp;
	printf("select file work start\n");
	sf->inFile->MoveFirst();
	printf("move first\n");
	while(sf->inFile->GetNext(r)){
		if (comp.Compare(&r, sf->literal, sf->selOp)){
			//r.Print(new Schema ("/Users/Migue/Development/workspace_cpp/database-implementation/source/catalog", "partsupp")) ;

			sf->outPipe->Insert(&r);
		}
	}
	sf->outPipe->ShutDown();

}

void RelationalOp::WaitUntilDone () {
	pthread_join (worker_thread, NULL);
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal){

	SelectFileStruct *sfs = new SelectFileStruct();

	sfs->inFile = &inFile;
	sfs->outPipe = &outPipe;
	sfs->selOp = &selOp;
	sfs->literal = &literal;
	pthread_create (&worker_thread, NULL, SelectFileWorkerThread, (void *) sfs);

}


void SelectFile::Use_n_Pages (int runlen) {

}



void* SelectPipeWorkerThread(void *arg){
	SelectPipeStruct *sp = (SelectPipeStruct *) arg;
	Record r;
	ComparisonEngine comp;
	while(sp->inPipe->Remove(&r)){
		if (comp.Compare(&r, sp->literal, sp->selOp)){
					sp->outPipe->Insert(&r);
		}
	}
	sp->outPipe->ShutDown();
}


void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
	SelectPipeStruct *sps = new SelectPipeStruct();

	sps->inPipe = &inPipe;
	sps->outPipe = &outPipe;
	sps->selOp = &selOp;
	sps->literal = &literal;
	pthread_create (&worker_thread, NULL, SelectPipeWorkerThread, (void *)sps);
}

void SelectPipe::Use_n_Pages (int runlen) {

}

void* DuplicateRemovalThread(void *arg){

	DuplicateRemovalStruct *drs = (DuplicateRemovalStruct *) arg;

	ComparisonEngine comp;
	OrderMaker *order = new OrderMaker(drs->schema);
	Pipe tmp(100);


	BigQ bigQ( *(drs->inPipe), tmp, *(order), 10);

	Record *record = new Record();
	Record *previous = new Record();
	previous = NULL;

	while(tmp.Remove(record)){
		// record->Print(drs->schema);
		// comp.Compare(previous, record,  drs->orderMaker) != 0 
		if(previous == NULL ){
			previous = new Record();
			previous->Copy(record);
		}
		if( comp.Compare(previous, record,  order) != 0 ){
			drs->outPipe->Insert(previous);
			previous->Copy(record);
		}
	}

	if(!previous->isNULL())
		drs->outPipe->Insert(previous);
	drs->outPipe->ShutDown();
}


void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	DuplicateRemovalStruct *drs = new DuplicateRemovalStruct();

	drs->inPipe = &inPipe;
	drs->outPipe = &outPipe;
	drs->schema = &mySchema;

	pthread_create (&worker_thread, NULL, DuplicateRemovalThread, (void *)drs);
}


void* WriteOutWorkerThread(void *arg){
	WriteOutStruct *wos = (WriteOutStruct *) arg;
	Schema *schema = wos->schema;
	Record lTempRecord;
	while( wos->inPipe->Remove(&lTempRecord)){
		int lNumOfAttr = schema->GetNumAtts();
		Attribute *lAttr = schema->GetAtts();
		for (int i = 0; i < lNumOfAttr; i++) {
			fprintf(wos->file, "%s%s",lAttr[i].name,":[");
			int lRecordValue = ((int *) lTempRecord.bits)[i + 1];
			if (lAttr[i].myType == Int) {
	  			int *mInt = (int *) &(lTempRecord.bits[lRecordValue]);
	  			fprintf(wos->file, "%d", *mInt);
			}
			else if (lAttr[i].myType == Double) {
	  			double *mDouble = (double *) &(lTempRecord.bits[lRecordValue]);
	  			fprintf(wos->file, "%f", *mDouble);
			}
			else if (lAttr[i].myType == String) {
	   			char *mString = (char *) &(lTempRecord.bits[lRecordValue]);
	   			fprintf(wos->file, "%s", mString);
			}
			fprintf(wos->file, "%s", "]");
			if (i != lNumOfAttr - 1) {
	    		fprintf(wos->file, "%s", ",");
	    	}
		}
		fprintf(wos->file, "\n");
	}
	fclose(wos->file);
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
	WriteOutStruct *wos = new WriteOutStruct();

	wos->inPipe = &inPipe;
	wos->file = outFile;
	wos->schema = &mySchema;

	pthread_create (&worker_thread, NULL, WriteOutWorkerThread, (void *)wos);
}


/*
	
	PROJECT METHODS

*/

void* ProjectWorkerThread(void *arg){
	
	ProjectStruct *ps = (ProjectStruct *) arg;
	Record r;
	ComparisonEngine comp;
	printf("Project worked started \n");	
	while(ps->inPipe->Remove(&r)){
		
		r.Project(ps->keepMe, ps->numAttsOutput, ps->numAttsInput);
		ps->outPipe->Insert(&r);
	}
	ps->outPipe->ShutDown();
}


void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
	
	ProjectStruct *ps = new ProjectStruct();
	ps->inPipe = &inPipe;
	ps->outPipe = &outPipe;
	ps->keepMe = keepMe;
	ps->numAttsInput = numAttsInput;
	ps->numAttsOutput = numAttsOutput;

	pthread_create (&worker_thread, NULL, ProjectWorkerThread, (void *)ps);
}

void Project::Use_n_Pages (int runlen) {

}

//take two records, merge them , then insert into pipe
void mergeRecordsIntoPipe(Record& record1, Record& record2, Pipe* pipe){
	Record mergeRec;
	int numAttsLeft = ((((int*)(record1.bits))[1])/sizeof(int)) - 1;
	int numAttsRight = ((((int*)(record2.bits))[1])/sizeof(int)) - 1;
	int numAttsToKeep = numAttsLeft + numAttsRight;
	int attsToKeep[numAttsToKeep];
	int i;
	for(i = 0; i < numAttsLeft; i++) {
	    attsToKeep[i] = i;
	}
	int startOfRight = i;
	for(int j = 0; j < numAttsRight; j++,i++) {
	    attsToKeep[i] = j;
	}
	mergeRec.MergeRecords(&record1, &record2, numAttsLeft,
	        numAttsRight, attsToKeep, numAttsToKeep, startOfRight);
	pipe->Insert(&mergeRec);
}


void nestedJoin(Pipe* inPipeL, Pipe* inPipeR, Pipe* outPipe){
	//cout<<"Inside Nested Join START"<<endl;
	ComparisonEngine comp;
	Record lRecord, rRecord;
	DBFile temp;
	temp.Create("jointemp.bin", heap, NULL );
	inPipeR->Remove(&rRecord);
	inPipeL->Remove(&lRecord);

	// Get the parameters for the merge record functions
	int numAttsLeft     =  lRecord.numOfAttInRecord();
	int numAttsRight    =  rRecord.numOfAttInRecord();
	int numAttsToKeep   =  numAttsLeft + numAttsRight;
	int* attsToKeep     =  new int[numAttsToKeep];

	for(int i =0;i<numAttsLeft;i++)
		attsToKeep[i] = i;
	int startOfRight = numAttsLeft;
	for(int i=numAttsLeft; i<(numAttsRight+numAttsLeft);i++)
		attsToKeep[i] = i-numAttsLeft;

	// Write the records of right pipe to a DBfile
	do{
		temp.Add(rRecord);
	}while(inPipeR->Remove(&rRecord));
	temp.MoveFirst();

	// Now do the nested join
	do{
		while( temp.GetNext(rRecord)){
			mergeRecordsIntoPipe(lRecord, rRecord, outPipe);
		}
		temp.MoveFirst();
	}while(inPipeL->Remove( &lRecord ));
	// Clean up
	temp.Close();
	remove("jointemp.bin");
    remove("jointemp.bin.meta");

    inPipeL->ShutDown();
	inPipeR->ShutDown();
	outPipe->ShutDown();
	//cout<<"Inside Nested Join END"<<endl;
}


void* JoinWorkerThread(void * arg){
	int runLen = 1000;
	JoinStruct *js = (JoinStruct *) arg;
	OrderMaker leftorder;
	OrderMaker rightorder;
	ComparisonEngine comp;
	

	Record left;
	Record right;

	//perform merge-sort join
	if( js->cnf->GetSortOrders(leftorder, rightorder) != 0 ){
		Pipe leftPipe(100);
		Pipe rightPipe(100);
		
		BigQ bigQ1( *(js->inPipeL), leftPipe, leftorder, runLen);
		BigQ bigQ2( *(js->inPipeR), rightPipe, rightorder, runLen);


		

		// printf("left info\n");
		// while(leftPipe.Remove(&left)){
		// 	left.Print(&s);
		// }
		// printf("right info\n");
		// while(rightPipe.Remove(&right)){
		// 	right.Print(&ps);
		// }

		bool leftPipeAlive = leftPipe.Remove(&left);
		bool rightPipeAlive = rightPipe.Remove(&right);
		
		while( leftPipeAlive && rightPipeAlive ){
			int res = comp.Compare(&left, &leftorder, &right, &rightorder);
			//left < right
			if( res == -1){
				if(leftPipe.Remove(&left))
					continue;
				else{
					leftPipeAlive = false;
				}
			}else if(res == 1){
				if(rightPipe.Remove(&right))
					continue;
				else
					rightPipeAlive = false;
			}else{
				//mergeRecordsIntoPipe(left, right, js->outPipe);
				vector<Record*> leftRecs;
				Record *tmpRec = new Record();
				tmpRec->Copy(&left);
				leftRecs.push_back(tmpRec);
				//put every record in leftPipe with the same attribute values into vector
				while(true){
					if( !leftPipe.Remove(&left) ){
						leftPipeAlive = false;
						break;
					}
					if( comp.Compare(&left, &leftorder, &right, &rightorder) == 0 ){
						Record *tmpRec2 = new Record();
						tmpRec2->Copy(&left);
						leftRecs.push_back(tmpRec2);
					}else{
						break;
					}
				}
				while( true ){
					for(int i = 0; i < leftRecs.size(); ++i){
						mergeRecordsIntoPipe( *(leftRecs[i]), right, js->outPipe);
					}
					if( !rightPipe.Remove(&right) ){
						rightPipeAlive = false;
						break;
					}
					if( comp.Compare(leftRecs[0], &leftorder, &right, &rightorder) != 0 ){
						break;
					}
				}
			}
		}
		js->outPipe->ShutDown();
		// if( !left.isNULL() && !right.isNULL() && comp.Compare(&left, &leftorder, &right, &rightorder) == 0){
		// 	mergeRecordsIntoPipe( left, right, js->outPipe);
		// }
	}else{
		nestedJoin(js->inPipeL, js->inPipeR, js->outPipe);
	}
}


void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
	// Schema ps("/Users/westsnow/Documents/DBIDATA/catalog", "partsupp");
	// Schema s("/Users/westsnow/Documents/DBIDATA/catalog", "supplier");
	// Record r;
	// while(inPipeL.Remove(&r)){
	// 	r.Print(&s);
	// }
	// while(inPipeR.Remove(&r)){
	// 	r.Print(&ps);
	// }



	JoinStruct *js = new JoinStruct();
	js->inPipeL = &inPipeL;
	js->inPipeR = &inPipeR;
	js->outPipe = &outPipe;
	js->cnf = &selOp;
	js->literal = &literal;

	pthread_create(&worker_thread, NULL, JoinWorkerThread, (void*)js);
}

/*
	
		SUM METHODS

*/

void* SumWorkerThread(void *arg){
	Attribute DA = {"double", Double};

	SumStruct *ss = (SumStruct *) arg;
	double result_doble = 0.0;
	int result_int = 0;
	Record r;
	Type t;
	while(ss->inPipe->Remove(&r)){
		double inner_result_double;
		int inner_result_int;
		t = ss->computeMe->Apply(r, inner_result_int, inner_result_double);
		if (Int == t)
			result_int += inner_result_int;
		else
			result_doble += inner_result_double;

	}
	
	char String[20];
	if(t == Int){
		 sprintf(String,"%d|", result_int);
	}else{
		 sprintf(String,"%f|", result_doble);		
	}
	Attribute at = {"double", Double};

	Schema sum_sch ("sum_sch", 1, &at);
	Record r2;
	r2.ComposeRecord(&sum_sch, String);
	ss->outPipe->Insert(&r2);
	ss->outPipe->ShutDown();
}


void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	
	SumStruct *ss = new SumStruct();
	ss->inPipe = &inPipe;
	ss->outPipe = &outPipe;
	ss->computeMe = &computeMe;

	pthread_create (&worker_thread, NULL, SumWorkerThread, (void *)ss);
}

/*
	
	GROUP BY METHODS

*/

void* GroupByWorkerThread(void *arg){
	GroupByStruct *gbs = (GroupByStruct *) arg;
	Pipe *tempPipe = new Pipe(gbs->runLen);
	BigQ bigQ( *(gbs->inPipe), *(tempPipe), *(gbs->groupAtts), gbs->runLen);
	Record r;
	Record *prev = new Record();
	ComparisonEngine comp;
	int result_int = 0;
	double result_doble = 0.0;
	Type t;

	while(tempPipe->Remove(&r)){

		if(!prev->isNULL()){
			if(comp.Compare(prev, &r, gbs->groupAtts) != 0){

				Attribute at;
				at.name = "SUM";
				char String[20];
				if(t == Int){
					sprintf(String,"%d|", result_int);
					at.myType = Double;
				}else{
					sprintf(String,"%f|", result_doble);
					at.myType = Double;
				}
				
				Schema sum_sch ("sum_sch", 1, &at);
				Record r2;
				r2.ComposeRecord(&sum_sch, String);
				
				//Not quite yet
				int numAttsNow = ((((int*)(prev->bits))[1])/sizeof(int)) - 1;
				
				prev->Project(gbs->groupAtts->whichAtts, gbs->groupAtts->numAtts, numAttsNow);
				
				
				//Attribute DA = {"double", Double};
				


				int attsToKeep[gbs->groupAtts->numAtts + 1];
				int i = 1;
				attsToKeep[0] = 0;
				for(int j=0;j<gbs->groupAtts->numAtts;j++){
					attsToKeep[i] = j;
					i += 1;
				}

				

				Record mergeRec;
				mergeRec.MergeRecords(&r2, prev, 1, gbs->groupAtts->numAtts, attsToKeep, gbs->groupAtts->numAtts + 1, 1);
				
				//Now we are ready
				gbs->outPipe->Insert(&mergeRec);

				result_int = 0;
				result_doble = 0.0;
				
			}

		}	

		double inner_result_double;
		int inner_result_int;
		t = gbs->computeMe->Apply(r, inner_result_int, inner_result_double);
		if (Int == t)
			result_int += inner_result_int;
		else
			result_doble += inner_result_double;

		prev->Copy(&r);

	}


	if(!prev->isNULL()){
		Attribute at;
		at.name = "SUM";
		char String[20];
		if(t == Int){
			sprintf(String,"%d|", result_int);
			at.myType = Double;
		}else{
			sprintf(String,"%f|", result_doble);
			at.myType = Double;
		}
		
		Schema sum_sch ("sum_sch", 1, &at);
		Record r2;
		r2.ComposeRecord(&sum_sch, String);
		
		//Not quite yet
		int numAttsNow = ((((int*)(prev->bits))[1])/sizeof(int)) - 1;
		prev->Project(gbs->groupAtts->whichAtts, gbs->groupAtts->numAtts, numAttsNow);
		
		int attsToKeep[gbs->groupAtts->numAtts + 1];
		int i = 1;
		attsToKeep[0] = 0;
		for(int j=0;j<gbs->groupAtts->numAtts;j++){
			attsToKeep[i] = j;
			i += 1;
		}
		Record mergeRec;
		mergeRec.MergeRecords(&r2, prev, 1, gbs->groupAtts->numAtts, attsToKeep, gbs->groupAtts->numAtts + 1, 1);
		
		//Now we are ready
		gbs->outPipe->Insert(&mergeRec);

	}

	gbs->outPipe->ShutDown();
}


void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	
	GroupByStruct *gbs = new GroupByStruct();
	gbs->inPipe = &inPipe;
	gbs->outPipe = &outPipe;
	gbs->groupAtts = &groupAtts;
	gbs->computeMe = &computeMe;
	gbs->runLen = 1000;
	
	pthread_create (&worker_thread, NULL, GroupByWorkerThread, (void *)gbs);
}









