#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>

// #ifdef __GNUC__
// #include <ext/hash_map>
// #else
// #include <hash_map>
// #endif

// namespace std
// {
// using namespace __gnu_cxx;
// }
	
using namespace std;

class RelStat{
public:
	double numTuples;
	unordered_map<string, int> attInfo;
	RelStat(){}
	RelStat(RelStat &copyMe){
		numTuples = copyMe.numTuples;
		attInfo = copyMe.attInfo;
	}
	RelStat& operator=(const RelStat& other){
		numTuples = other.numTuples;
		attInfo = other.attInfo;
		return *this;
	}
	bool attrExists(string attrName);
	int getValue(string attName);
	void init();
};



class Statistics
{
public:
	unordered_map<string, RelStat*> relInfo;
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();

	bool RelExists(string relName);

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	void init();
	string getTableNameFromAttr(string attrName);
	void clearStats();
	void deleteTable(char *oldTableName);
	
private:
	

};

#endif
