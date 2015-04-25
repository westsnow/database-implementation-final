#include "Optimizer.h"
#include "Schema.h"
#include <iostream>
#include <vector>

int const PIPE_SIZE = 100;

void PrintOperand(struct Operand *pOperand)
{
        if(pOperand!=NULL)
        {
                cout<<pOperand->value<<" ";
                cout<<" ("<<pOperand->code<<") ";
        }
        else
                return;
}

void PrintComparisonOp(struct ComparisonOp *pCom)
{
        if(pCom!=NULL)
        {
                PrintOperand(pCom->left);
                switch(pCom->code)
                {
                        case 1:
                                cout<<" < "; break;
                        case 2:
                                cout<<" > "; break;
                        case 3:
                                cout<<" = ";
                }
                PrintOperand(pCom->right);

        }
        else
        {
                return;
        }
}
void PrintOrList(struct OrList *pOr)
{
        if(pOr !=NULL)
        {
                struct ComparisonOp *pCom = pOr->left;
                PrintComparisonOp(pCom);

                if(pOr->rightOr)
                {
                        cout<<" OR ";
                        PrintOrList(pOr->rightOr);
                }
        }
        else
        {
                return;
        }
}
void PrintAndList(struct AndList *pAnd)
{
        if(pAnd !=NULL)
        {
                struct OrList *pOr = pAnd->left;
                PrintOrList(pOr);
                if(pAnd->rightAnd)
                {
                        cout<<" AND ";
                        PrintAndList(pAnd->rightAnd);
                }
        }
        else
        {
                return;
        }
}
void PrintNameList(struct  NameList* list){
	if(list == NULL){
		cout<<"print done"<<endl;
	}else{
		cout<<list->name<<endl;
		list = list->next;
		PrintNameList(list);
	}
}


// from parser
extern FuncOperator* finalFunction;
extern TableList* tables;
extern AndList* boolean;
extern NameList* groupingAtts;
extern NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;
extern char* dbfile_dir;
//from main
extern char *catalog_path;


int pipeid = 1;
using namespace std;

Optimizer::Optimizer(Statistics *st){

	s = new Statistics( *(st) );
	// s->init();
	planRoot = NULL;	
	output = NULL;
}
void Optimizer::executeQuery(){
	int numNodes = planRoot->outPipeID;
    Pipe** pipes = new Pipe*[numNodes+5];
    RelationalOp** relops = new RelationalOp*[numNodes+5];
    planRoot->execute(pipes, relops);
    for (int i=1; i<=numNodes; ++i)
      relops[i] -> WaitUntilDone();


  	if(output != stdout)
  		fclose(output);
}

void Optimizer::planQuery(){
	
	// // PrintAndList(boolean);
	// cout<<distinctAtts<<endl;
	// cout<<distinctFunc<<endl;
	// // PrintOperand(finalFunction->code);


	//read readMode from outputMode.txt
	ifstream f;
	f.open("outputMode.txt");
	string outMode;
	f>>outMode;
	cout<<"the outMode is "<<outMode<<endl;
	f.close();

	if(outMode == "STDOUT")
		output = stdout;
	else if(outMode != "NONE"){
		output = fopen ((char*)outMode.c_str(), "w");
	}else{
		output = NULL;
	}

	createTableNodes();
	createJoinNodes();
	createSumNodes();
	createProjectNodes();
	createDistinctNodes();
	createWriteOutNodes(output);
	// cout<<"before printing"<<endl;
	// cout<<distinctAtts<<endl;
	// cout<<distinctFunc<<endl;
	// if(finalFunction){
	// 	cout<<"has final function"<<endl;

	// }else{
	// 	cout<<"no finalFunction"<<endl;
	// }
	// PrintNameList(groupingAtts);
	// // Function f;
	// // f.GrowFromParseTree (finalFunction, *(planRoot->outSchema)) ;
	// // f.Print();
	// cout<<"after printing"<<endl;

	traverse(planRoot);

	if(output != NULL)
		executeQuery();
}

//Work left to do!!!!!!! 
double evalOrder(vector<TableNode*> tableNodes, Statistics *s, int minCost){
	
	Statistics *temp_st = new Statistics(*(s));
		int pipe = tableNodes.size();
		if(tableNodes.size()>1){
			TableNode* left = tableNodes[0];
			TableNode* right = tableNodes[1];

			JoinNode *prev = new JoinNode(left->outPipeID, right->outPipeID, pipe);
			pipe += 1;
			prev->children.push_back(left);
			prev->children.push_back(right);
			prev->relatedJoinCNF(boolean, temp_st);
			//join->toString();
			//join->children[0]->toString();
			//join->children[1]->toString();
			for(int i=2; i<tableNodes.size(); i++){
				TableNode* right = tableNodes[i];
				JoinNode *join = new JoinNode(prev->outPipeID, right->outPipeID, pipe);
				join->children.push_back(prev);
				join->children.push_back(right);
				//RelatedJoinCNF uses children! set them before use, I dont check that
				join->relatedJoinCNF(boolean, temp_st);	
				pipe += 1;
				prev = join;
			}
			
			return prev->cost;
		
		}

	return 1;

}

void Optimizer::createJoinNodes(){

	std::vector<TableNode*> tableList(tableNodes);
	sort (tableList.begin(),tableList.end());
	int minCost = INT_MAX;
	int cost;
	do {
		cost=evalOrder(tableList, s, minCost);
		
		// for(int i=0; i<tableList.size(); i++){
		// 	cout<<tableList[i]->tableName<<"-";
		// }
		//cout<<endl;
		if(cost < minCost && cost>0) {
			minCost = cost; 
			tableNodes = tableList; 
		}
	} while ( next_permutation(tableList.begin(), tableList.end()) );

	if(tableNodes.size()>1){
		TableNode* left = tableNodes[0];
		TableNode* right = tableNodes[1];

		JoinNode *prev = new JoinNode(left->outPipeID, right->outPipeID, pipeid);
		pipeid += 1;
		prev->children.push_back(left);
		prev->children.push_back(right);
		prev->relatedJoinCNF(boolean, s);
		//join->toString();
		//join->children[0]->toString();
		//join->children[1]->toString();
		for(int i=2; i<tableNodes.size(); i++){
			TableNode* right = tableNodes[i];
			JoinNode *join = new JoinNode(prev->outPipeID, right->outPipeID, pipeid);
			join->children.push_back(prev);
			join->children.push_back(right);
			//RelatedJoinCNF uses children! set them before use, I dont check that
			join->relatedJoinCNF(boolean, s);	
			pipeid += 1;
			prev = join;
		}
		
		planRoot = prev;
	}
	//If root is still NULL after Join Nodes There is only one table in the query!
	if(planRoot == NULL){
		planRoot = tableNodes[0];
	}
}

void Optimizer::createWriteOutNodes(FILE* output) {
	planRoot = new WriteOutNode(planRoot, pipeid, output);
}

void Optimizer::createDistinctNodes() {
  if (distinctAtts) 
  	planRoot = new DuplicateRemovalNode(planRoot, pipeid++);
}

void Optimizer::createProjectNodes() {
  if (attsToSelect && !finalFunction && !groupingAtts) 
  	planRoot = new ProjectNode(attsToSelect, planRoot, pipeid++);
}


void Optimizer::createTableNodes(){
	
	
	TableList *tmp = tables;
	while(tmp != NULL){

		s->CopyRel(tmp->tableName, tmp->aliasAs);

		TableNode *t = new TableNode(tmp->tableName, tmp->aliasAs, pipeid);
		
		t->relatedSelectCNF(boolean, s);
		//t->toString();
		
		tableNodes.push_back(t);
		
		tmp = tmp->next;
		pipeid += 1;
	} 

}

// void QueryPlan::makeSums() {
//   if (groupingAtts) {
//     FATALIF (!finalFunction, "Grouping without aggregation functions!");
//     FATALIF (distinctAtts, "No dedup after aggregate!");
//     if (distinctFunc) root = new DedupNode(root);
//     root = new GroupByNode(groupingAtts, finalFunction, root);
//   } else if (finalFunction) {
//     root = new SumNode(finalFunction, root);
//   }
// }



void Optimizer::createSumNodes(){
	if(groupingAtts){
		if(!finalFunction){
			cout<<"no grouping attributes error"<<endl;
			exit(1);
		}
		planRoot = new GroupByNode(groupingAtts, finalFunction, planRoot, pipeid++);
	}else if(finalFunction){
		cout<<"create sum"<<endl;
		planRoot = new SumNode(finalFunction, planRoot, pipeid++);
	}
}

void Optimizer::traverse(QueryPlanNode *root){
	root->toString();
	for(int i = 0; i < root->children.size(); ++i){
		traverse(root->children[i]);
	}
}

// void Optimizer::traverse(QueryPlanNode *root){
// 	cout<<"traverse"<<endl;
// 	if(!root->children.empty()){
// 		if(root->children.size() == 2){
// 			traverse(root->children[0]);
// 			root->toString();
// 			traverse(root->children[1]);
// 		}
// 	}else{
// 		root->toString();
// 	}
// }


DuplicateRemovalNode::DuplicateRemovalNode(QueryPlanNode* root, int outPipeID){
	this->outPipeID = outPipeID;
	children.push_back(root);
	outSchema = new Schema(*(root->outSchema));
}

ProjectNode::ProjectNode(NameList* atts, QueryPlanNode* root, int pipeid){
	this->outPipeID = pipeid;
	children.push_back(root);
	numAttsIn = root->outSchema->GetNumAtts();
	numAttsOut = 0;

	Schema* cSchema = root->outSchema;
  	Attribute resultAtts[100];
  	for (; atts; atts=atts->next, numAttsOut++) {
  		if(  (keepMe[numAttsOut]=cSchema->Find(atts->name))==-1 ){
  			printf("Projected attributes not exist\n");
		}
   	 resultAtts[numAttsOut].name = atts->name;
   	 resultAtts[numAttsOut].myType = cSchema->FindType(atts->name);
  	}
 	outSchema = new Schema ("", numAttsOut, resultAtts);
}

// void printParseTree(struct FuncOperator* parseTree){
// 	if(parseTree == NULL){
// 		return;
// 	}else{
// 		printParseTree(parseTree->leftOperator);
// 		printParseTree(parseTree->right);
// 	}
// 	if(parseTree->leftOperand == NULL){
// 			cout<<"leftOperand is null"<<endl;
// 	}else
// 		cout<<parseTree->leftOperand->code<<" "<<parseTree->leftOperand->value<<endl;
// }
SumNode::SumNode(struct FuncOperator* parseTree, QueryPlanNode* root, int outPipeID){
	computeMe.GrowFromParseTree (parseTree, *root->outSchema);
	this->outPipeID = outPipeID;
	children.push_back(root);
  	Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  	outSchema = new Schema ("", 1, atts[computeMe.resultType()]);
}

WriteOutNode::WriteOutNode(QueryPlanNode* root, int outPipeID, FILE* output){
	this->outPipeID = outPipeID;
	children.push_back(root);
	outSchema = new Schema(*(root->outSchema));
	this->output = output;
}

GroupByNode::GroupByNode(struct NameList* nameList, struct FuncOperator* parseTree, QueryPlanNode* root, int outPipeID){
	groupOrder.growFromParseTree(nameList, root->outSchema);
	computeMe.GrowFromParseTree (parseTree, *(root->outSchema));
	this->outPipeID = outPipeID;
	children.push_back(root);



	vector<char*>input;

	struct NameList * tempgroup=groupingAtts;

	while(tempgroup!=NULL)
	{
		input.push_back(tempgroup->name);
		tempgroup=tempgroup->next;
	}
	// temp->nodeAndList = CreateAndList(input);
	// temp->nodeCnf = new CNF();
	// temp->nodeLiteral = new Record();
	// OrderMaker dummy;

	// temp->nodeCnf->GrowFromParseTree(temp->nodeAndList, root->nodeSchema, *(temp->nodeLiteral));
	// temp->groupAtts = new OrderMaker;
	// temp->nodeCnf->GetSortOrders(*(temp->groupAtts), dummy);

	int totatts=input.size();
	totatts++;
	Attribute * attrlist=new Attribute[totatts];
	attrlist[0].myType=Double;
	attrlist[0].name="SUM";
	tempgroup=groupingAtts;
	int i=1;
	while(tempgroup!=NULL)
	{
		attrlist[i].name=tempgroup->name;
		attrlist[i].myType=root->outSchema->FindType(tempgroup->name);
		i++;
		tempgroup=tempgroup->next;
	}

	outSchema=new Schema("catalog",totatts,attrlist);


	// outSchema = createShema(nameList, parseTree, root);
}


/*
	TableNode Methods
*/

//TableNode::TableNode(CNF cond, string tableName, string tableAlias, outPipeID, string fileName){
TableNode::TableNode(char *name, char *alias, int outPipeID){
	
	tableName = name;
	tableAlias =  alias;
	this->outPipeID = outPipeID;
	fileName = "";
}

void TableNode::relatedSelectCNF(AndList *boolean, Statistics *s){
	string name(tableName);
	string alias(tableAlias);
	AndList *andTmp = boolean;
	AndList *andFinal = NULL;
	

	while(andTmp != NULL){

		OrList *orList = andTmp->left;

		while(orList != NULL){
			struct ComparisonOp *pCom = orList->left;
			struct Operand* leftOperand = pCom->left;
			//printf("%d\n", leftOperand->code);
			struct Operand* rightOperand = pCom->right;
			//printf("%d\n", rightOperand->code);
			string leftValue(leftOperand->value);
			string rightValue(rightOperand->value);
			string tableNameOfLeft = s->getTableNameFromAttr(leftOperand->value);
			string tableNameOfRight = s->getTableNameFromAttr(rightOperand->value);

			if((leftOperand->code == NAME && rightOperand->code != NAME) || (leftOperand->code != NAME && rightOperand->code == NAME)){
				if((tableNameOfLeft.compare(alias) == 0) || (tableNameOfRight.compare(alias) == 0)){
					AndList *node = new AndList();
					if(andFinal == NULL){
						node->left = andTmp->left;
						node->rightAnd = NULL;
						
					}
					else{
						node->rightAnd = andFinal;
						node->left = andTmp->left;
					}
					andFinal = node;
					break;
				}
			}
			orList = orList->rightOr;
		}
		andTmp = andTmp->rightAnd;
	}

	outSchema = new Schema(catalog_path, tableName, tableAlias);
	
	if(andFinal != NULL){
		
		
		char* relName[1];
		relName[0] =  tableAlias;
		//cout<<relName[0];
		cost = s->Estimate(andFinal, relName, 1);	
		s->Apply(andFinal, relName, 1);
		cond.GrowFromParseTree (andFinal, outSchema, literal);
		
		
		

	}else{
		//s->Write("a.txt");
		//cout<<alias;
		cost = s->relInfo[alias]->numTuples;
		/*
			Do we need to create dummy CNF for all rows in table?
		*/
		// Attribute *dummy = outSchema->GetAtts();
		// AndList *final = new AndList();
		// OrList *only = new OrList();
		// only->left->code = EQUALS;	
		// only->left->left->code = dummy->myType;
		// only->left->left->value = dummy->name;
		// only->left->right->code = dummy->myType;
		// only->left->right->value = dummy->name;
		// only->rightOr = NULL;
		// final->left = only;
		// final->rightAnd = NULL;
		// cond.GrowFromParseTree (final, outSchema, literal);
	}
		

}




/*
	JoinNode Methods
*/

JoinNode::JoinNode(int leftPipeID, int rightPipeID, int outPipeID){
	
	this->leftPipeID = leftPipeID;
	this->rightPipeID = rightPipeID;
	this->outPipeID = outPipeID;
}

void JoinNode::relatedJoinCNF(AndList *boolean, Statistics *s){
	Schema *leftSchema = children[0]->outSchema;
	Schema *rightSchema = children[1]->outSchema;

	AndList *andTmp = boolean;
	AndList *andFinal = NULL;
	while(andTmp != NULL){

		OrList *orList = andTmp->left;

		while(orList != NULL){
			struct ComparisonOp *pCom = orList->left;
			struct Operand* leftOperand = pCom->left;
			struct Operand* rightOperand = pCom->right;
			

			if(leftOperand->code == NAME && rightOperand->code == NAME){
				// cout<<leftOperand->value<<endl;
				// leftSchema->Print();
				// cout<<rightOperand->value<<endl;
				// rightSchema->Print();

				if((leftSchema->Find(leftOperand->value) != -1 && rightSchema->Find(rightOperand->value)!= -1) || (leftSchema->Find(rightOperand->value) != -1 && rightSchema->Find(leftOperand->value)!= -1)){
					//cout<<"Aqui 2"<<endl;
					AndList *node = new AndList();
					if(andFinal == NULL){
						node->left = andTmp->left;
						node->rightAnd = NULL;	
					}
					else{
						node->rightAnd = andFinal;
						node->left = andTmp->left;
					}
					andFinal = node;
					break;
				}
			}
			orList = orList->rightOr;
		}
		andTmp = andTmp->rightAnd;
	}

	//Merge Left and Right Schemas!

	int leftNumAtts = children[0]->outSchema->GetNumAtts();
	int rightNumAtts = children[1]->outSchema->GetNumAtts();
	
	int numAtts = leftNumAtts + rightNumAtts;
	
	Attribute* attsToKeep =  new Attribute[numAtts];

	int i = 0;
	Attribute *leftAtts = children[0]->outSchema->GetAtts();
 	for(int j = 0; j< leftNumAtts; j++){
		attsToKeep[i] = leftAtts[j];
		i += 1;
	}
	Attribute *rightAtts = children[1]->outSchema->GetAtts();
 	for(int j = 0; j< rightNumAtts; j++){
		attsToKeep[i] = rightAtts[j];
		i += 1;
	}

	outSchema = new Schema( "join" , numAtts, attsToKeep);
	
	if(andFinal != NULL){
		
		
		cost = s->Estimate(andFinal, NULL, 2);
		s->Apply(andFinal, NULL, 2);

		cond.GrowFromParseTree (andFinal, children[0]->outSchema, children[1]->outSchema, literal);
	
	}else{
		char *tokens[2];
		tokens[0] = leftSchema->GetAtts()[0].name;
		tokens[1] = rightSchema->GetAtts()[0].name;

		cost = s->Estimate(andFinal, tokens, 2);
		s->Apply(andFinal, tokens, 2);
	}



}
void GroupByNode::toString(){
	cout<<"***************************"<<endl;
	cout<<"GroupBy Operation"<<endl;
	cout<<"child Input: "<<children[0]->outPipeID<<endl;
	cout<<"Out pipe: "<<outPipeID<<endl;
	// cout<<"Cost: "<<cost<<endl;
	cout<<"Function: "<<endl;
	computeMe.Print();
	cout<<"OrderMaker "<<endl;
	groupOrder.Print();
	cout<<"outSchema "<<endl;
	outSchema->Print();
}
void SumNode::toString(){
	cout<<"***************************"<<endl;
	cout<<"Sum Operation"<<endl;
	cout<<"child Input: "<<children[0]->outPipeID<<endl;
	cout<<"Out pipe: "<<outPipeID<<endl;
	// cout<<"Cost: "<<cost<<endl;
	cout<<"Function: "<<endl;
	computeMe.Print();
	cout<<"outSchema "<<endl;
	outSchema->Print();

}
void ProjectNode::toString(){
	cout<<"***************************"<<endl;
	cout<<"Project Operation"<<endl;
	cout<<"child Input: "<<children[0]->outPipeID<<endl;
	cout<<"Out pipe: "<<outPipeID<<endl;
	// cout<<"Cost: "<<cost<<endl;
	cout<<"outSchema "<<endl;
	outSchema->Print();
}
void DuplicateRemovalNode::toString(){
	cout<<"***************************"<<endl;
	cout<<"Distinct Operation"<<endl;
	cout<<"child Input: "<<children[0]->outPipeID<<endl;
	cout<<"Out pipe: "<<outPipeID<<endl;
	// cout<<"Cost: "<<cost<<endl;
	cout<<"outSchema "<<endl;
	outSchema->Print();
}
void WriteOutNode::toString(){
	cout<<"***************************"<<endl;
	cout<<"WriteOut Operation"<<endl;
	cout<<"child Input: "<<children[0]->outPipeID<<endl;
	cout<<"Out pipe: "<<outPipeID<<endl;
	// cout<<"Cost: "<<cost<<endl;
	cout<<"outSchema "<<endl;
	outSchema->Print();
}
void TableNode::toString(){

	cout<<"***************************"<<endl;
	cout<<"Select From File Operation"<<endl;
	cout<<"Table "<<tableName<<" AS "<<tableAlias<<endl;
	cout<<"Cost: "<<cost<<endl;
	cout<<"Input file: "<<endl;
	cout<<"Output pipe Id: "<<outPipeID<<endl;
	cout<<"Applied CNF: "<<endl;
		cond.Print();
	cout<<"Output Schema:"<<endl;
		outSchema->Print();
}
void JoinNode::toString(){

	cout<<"***************************"<<endl;
	cout<<"Join Operation"<<endl;
	cout<<"Left Input: "<<leftPipeID<<endl;
	cout<<"Right Input: "<<rightPipeID<<endl;
	cout<<"Out pipe: "<<outPipeID<<endl;
	cout<<"Cost: "<<cost<<endl;
	cout<<"Join CNF: "<<endl;
		cond.Print();
	cout<<"Output Schema: "<<endl;
		outSchema->Print();
}

void TableNode::execute(Pipe** pipes, RelationalOp** relops){
  cout<<"exe...select file"<<endl;

  string dbName = string(tableName) + ".bin";
  string finalPath = string(dbfile_dir) + dbName;
  cout<<finalPath<<endl;
  cout<<"open dbfile"<<endl;
  dbFile.Open((char*)finalPath.c_str());
    cout<<"opened dbfile"<<endl;

  SelectFile* sf = new SelectFile();
  pipes[outPipeID] = new Pipe(PIPE_SIZE);
  relops[outPipeID] = sf;
  cout<<"the outPipeid is "<<outPipeID<<endl;
  sf->Run(dbFile, *(pipes[outPipeID]), cond, literal);
}

void ProjectNode::execute(Pipe** pipes, RelationalOp** relops){
	cout<<"exe...project"<<endl;
	Project* p = new Project();
	children[0] -> execute(pipes, relops);
	pipes[outPipeID] = new Pipe(PIPE_SIZE);
	relops[outPipeID] = p;
	p -> Run( *(pipes[children[0]->outPipeID]), *(pipes[outPipeID]), keepMe, numAttsIn, numAttsOut );
}


void JoinNode::execute(Pipe** pipes, RelationalOp** relops){
	cout<<"exe...join"<<endl;
	children[0]->execute(pipes, relops);
	children[1]->execute(pipes, relops);
	Join * j = new Join();
	pipes[outPipeID] = new Pipe(PIPE_SIZE);
  	relops[outPipeID] = j;
  	j -> Run(*(pipes[children[0]->outPipeID]), *(pipes[children[1]->outPipeID]), *(pipes[outPipeID]), cond, literal);
}



void DuplicateRemovalNode::execute(Pipe** pipes, RelationalOp** relops){
	children[0]->execute(pipes, relops);
	DuplicateRemoval * dr = new DuplicateRemoval();
	pipes[outPipeID] = new Pipe(PIPE_SIZE);
	relops[outPipeID] = dr;
	dr->Run(*pipes[children[0]->outPipeID], *pipes[outPipeID], *outSchema);
}


void SumNode::execute(Pipe** pipes, RelationalOp** relops){
	cout<<"exe...sum"<<endl;
	children[0]->execute(pipes, relops);
	Sum * s = new Sum();
	pipes[outPipeID] = new Pipe(PIPE_SIZE);
	relops[outPipeID] = s;
	s->Run(*pipes[children[0]->outPipeID], *pipes[outPipeID], computeMe);
}


//void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
void GroupByNode::execute(Pipe** pipes, RelationalOp** relops){
	children[0]->execute(pipes, relops);
	GroupBy * gb = new GroupBy();
	pipes[outPipeID] = new Pipe(PIPE_SIZE);
	relops[outPipeID] = gb;
	gb->Run(*pipes[children[0]->outPipeID], *pipes[outPipeID], groupOrder,computeMe);
}

//	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
void WriteOutNode::execute(Pipe** pipes, RelationalOp** relops){
	cout<<"exe...writeout"<<endl;
	children[0]->execute(pipes, relops);
	if(output == NULL){
		cout<<"--------------------------OUTPUT IS SET TO NULL--------------------------"<<endl;
		return;
	}
	WriteOut * wo = new WriteOut();
	relops[outPipeID] = wo;	
	wo->Run(*(pipes[children[0]->outPipeID]), output, *outSchema);
}

