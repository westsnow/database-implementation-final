#ifndef OPTIMIZER_H
#define OPTIMIZER_H
#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <math.h>
#include <algorithm>
#include <climits>
#include <iostream>


#include "Comparison.h"
#include "Function.h"
#include "Statistics.h"
#include "RelOp.h"


using namespace std;

class QueryPlanNode; 
class TableNode;

class Optimizer{
	private:
		int pipeid;
		QueryPlanNode *planRoot;
		Statistics *s;
		vector<TableNode*> tableNodes;
		void clearNodes();
		void clearTableNodes();//close dbfile in table nodes.

	public:

		FILE* output;
		Optimizer(Statistics *st);
		~Optimizer();

		void initialize();
		void traverse(QueryPlanNode *root);
		void planQuery();
		void executeQuery();
		void createTableNodes();
		void createJoinNodes();
		void createSumNodes();	
		void createProjectNodes();	
		void createDistinctNodes();
		void createWriteOutNodes(FILE* f);
};


class QueryPlanNode{
	
	public:
		vector <QueryPlanNode*> children;
		double cost;
		Schema *outSchema;
		int outPipeID;
  		virtual void execute(Pipe** pipes, RelationalOp** relops) = 0;
		virtual void toString() = 0;
		~QueryPlanNode();
};

//Leaf node select from file!
class TableNode : public QueryPlanNode { 
	
	public:
		CNF cond;
		Record literal;
		char *tableName;
		char *tableAlias;
		string fileName;
		DBFile dbFile;

		void execute(Pipe** pipes, RelationalOp** relops);
		void relatedSelectCNF(AndList *boolean, Statistics *s, vector<bool> &used);
		TableNode(char *name, char *alias, int outPipeID);
		void toString();
};

//Project Node
class ProjectNode : public QueryPlanNode { 
	public:
		int keepMe[100];
		int numAttsIn;
		int numAttsOut;	

		void execute(Pipe** pipes, RelationalOp** relops);
		int inPipeID;
		ProjectNode(NameList* atts, QueryPlanNode* root, int pipeid);
		void toString();
};


class JoinNode : public QueryPlanNode { 
	public:
		int rightPipeID;
		int leftPipeID;
		CNF cond;
		Record literal;
		
		void execute(Pipe** pipes, RelationalOp** relops);

		void relatedJoinCNF(AndList *boolean, Statistics *s, vector<bool> &used);
		JoinNode(int leftPipeID, int rightPipeID, int outPipeID);
		void toString();

};

class DuplicateRemovalNode : public QueryPlanNode {
	public:
		int inPipeID;
		DuplicateRemovalNode(QueryPlanNode* root, int outPipeID);
		void execute(Pipe** pipes, RelationalOp** relops);

		void toString();		
};

class SumNode : public QueryPlanNode {
	public:
		int inPipeID;
		Function computeMe;
		SumNode(struct FuncOperator* parseTree, QueryPlanNode* root, int outPipeID);
		void toString();	
		void execute(Pipe** pipes, RelationalOp** relops);
};

class GroupByNode : public QueryPlanNode {
	
	public:
		int inPipeID;
		OrderMaker groupOrder;
		Function computeMe;
		GroupByNode(struct NameList* nameList, struct FuncOperator* parseTree, QueryPlanNode* root, int outPipeID);
		void toString();
		void execute(Pipe** pipes, RelationalOp** relops) ;

};

class WriteOutNode : public QueryPlanNode {
	public:
		int inPipe;

		FILE* output;

		WriteOutNode(QueryPlanNode* root, int outPipeID, FILE* output);
		void execute(Pipe** pipes, RelationalOp** relops) ;

		void toString();
	
};


#endif /* OPTIMIZER_H */
