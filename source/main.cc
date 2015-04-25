#include <iostream>
#include <string>
#include "ParseTree.h"
#include "Optimizer.h"
#include "ExecutionEngine.h"

using namespace std;

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" {
	int yyparse(void);
}
extern bool bye;
char *catalog_path, *dbfile_dir, *tpch_dir = NULL, *statistics_dir;
bool run;

void settings(char *settings_path){
	FILE *fp = fopen (settings_path, "r");
	if (fp) {
		char *mem = (char *) malloc (80 * 4);
		catalog_path = &mem[0];
		dbfile_dir = &mem[80];
		tpch_dir = &mem[160];
		statistics_dir = &mem[240];
		char line[80];
		fgets (line, 80, fp);
		sscanf (line, "%s\n", catalog_path);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", dbfile_dir);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", tpch_dir);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", statistics_dir);
		fclose (fp);
		if (! (catalog_path && dbfile_dir && tpch_dir)) {
			cerr << " Test settings file is not in correct format.\n";
			free (mem);
			exit (1);
		}
	}
	else {
		exit (1);
	}

}

int main()
{

	run = true;
	char *settings_path = "settings.txt";
	settings(settings_path);
	Statistics *s = new Statistics();
	//s->init();
	s->Read(statistics_dir);
	ExecutionEngine *engine = new ExecutionEngine(s);
	Optimizer *optimizer = new Optimizer(s);
	cout<<"Write your SQL query, to execute press enter ant then ctrl + D"<<endl;
	while(run){
		
		cout<<"\nsql>> ";
		yyparse();
		
		engine->execute();
		//engine->select();
		//optimizer->planQuery();
		
		//if(bye){
			run = false;
		//}

	}
	//s->clearStats();
	//s->init();
	s->Write(statistics_dir);

	return 1;
}
