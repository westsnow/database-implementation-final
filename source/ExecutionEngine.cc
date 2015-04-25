#include "DBFile.h"
#include "ExecutionEngine.h"
#include "Optimizer.h"
#include "Statistics.h"
#include <string>
#include <stdio.h>
#include <fstream>


extern char *newtable;
extern char *newfile;
extern char *oldtable;
extern char *deoutput;
extern struct NameList *attsToSelect;
extern struct FuncOperator *finalFunction; 
extern struct TableList* tables;
extern struct AndList* boolean;
extern struct NameList* groupingAtts;
extern struct AttrList *newattrs;
extern int distinctAtts;
extern int distinctFunc;

extern char *catalog_path;
extern char *dbfile_dir;

const char* myTypes[3] = {"Int", "Double", "String"};

ExecutionEngine::ExecutionEngine(Statistics *st){
	s = st;
}

void ExecutionEngine::execute(){
	if (newtable) {
      createTable();
    } else if (oldtable && newfile) {
      insertInto();
    } else if (oldtable && !newfile) {
      dropTable();
    } else if (deoutput) {
      setOutput(deoutput);
    } else if (attsToSelect || finalFunction) {
      select();
    } 
    //clear();
}

bool ExecutionEngine::existance(char *tableName){
	ifstream fin (catalog_path);
	string name(tableName);
	string line;
	while (getline(fin, line)){
		if (trim(line).compare(name) == 0) {
			fin.close(); 
			return true;
		}
	}
	fin.close();  
	return false;
}

void ExecutionEngine::createTable(){
	
	if(!existance(newtable)){
		DBFile dbfile;
		
		char rpath[100];
		sprintf (rpath, "%s%s.bin", dbfile_dir, newtable);
		
		cout << "DBFile will be created at " << rpath << endl;
		
		dbfile.Create (rpath, heap, NULL);
		
		dbfile.Close();
		
		s->AddRel(newtable, 1);
		//Add to Catalog and statistics
		ofstream out;
		out.open(catalog_path, std::ios::app);
		out <<endl<<"BEGIN"<<endl;
		out <<newtable<<endl;
		out <<newtable<<".tbl"<<endl;
		while(newattrs != NULL){
			out<<newattrs->name<<" "<<myTypes[newattrs->type]<<endl;
			s->AddAtt(newtable, newattrs->name, 1);
			newattrs = newattrs->next;
		}
		out << "END"<<endl;
		
	}else{
		cout<<"ERROR!!!! THIS TABLE AREADY EXISTS"<<endl;
	}

}

void ExecutionEngine::insertInto(){
	

	char rpath[100];
	Schema *sh;
	if( existance(oldtable) ){
		DBFile dbfile;
		
		sprintf (rpath, "%s%s.bin", dbfile_dir, oldtable);
		
		if(dbfile.Open(rpath)){
			sh = new Schema(catalog_path, oldtable);
			//sh->Print();
			dbfile.Load (*(sh), newfile);
			dbfile.Close ();
		}
		else{
			cout<<"ERROR!!!! UNABLE TO OPEN DBFILE!"<<endl;	
			cout<<"Path: "<<dbfile_dir;
		}

	}
	else{
		cout<<"ERROR!!!! THIS TABLE DOES NOT EXISTS"<<endl;
	}

	DBFile dbfile;
	dbfile.Open (rpath);
	dbfile.MoveFirst();

	Record temp;

	int counter = 0;
	while (dbfile.GetNext (temp) == 1) {
		counter += 1;
		temp.Print (sh);
		if (counter % 10000 == 0) {
			cout << counter << "\n";
		}
	}
	cout << " scanned " << counter << " recs \n";
	dbfile.Close ();
	
}

void ExecutionEngine::dropTable(){
	if(existance(oldtable)){
		string table(oldtable);
		//Delete from statistics
		s->deleteTable(oldtable);

		// Delete from schema
		ifstream catalog;
		catalog.open(catalog_path);

		ofstream tmpFile;
		tmpFile.open("tmpFile.tmp");

		string line;
		string newLine = "";
		bool found = true;
		while(getline(catalog, line)){
			if (trim(line).empty()) continue;
		    if (line == oldtable)  found = true;
		    newLine += trim(line) + '\n';
		    if (line == "END") {
		      if (!found) tmpFile << newLine << endl;
		      found = false;
		      newLine.clear();
		    }			
		}
		tmpFile.close();
		catalog.close();
		rename ("tmpFile.tmp", catalog_path);


		//Delete DBFiles

		remove ((table+".bin").c_str());
    	remove ((table+".bin.header").c_str());

	}
}

void ExecutionEngine::setOutput(char *mode){
	cout<<" setOutput";
}

void ExecutionEngine::select(){

	Optimizer *optimizer = new Optimizer(s);
	optimizer->planQuery();
  optimizer->executeQuery();

}
 void ExecutionEngine::clear(){
 	
 	newtable = "";
	newfile = "";
	oldtable = "";
	deoutput = "";
	NameList *attsToSelect = NULL;
	FuncOperator *finalFunction = NULL; 
	TableList* tables = NULL;
	AndList* boolean = NULL;
	NameList* groupingAtts = NULL;
	distinctAtts = NULL;
	distinctFunc = NULL;
	
 }



// from parser