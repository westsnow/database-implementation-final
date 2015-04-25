CC = g++ -O2 -Wno-deprecated -std=c++0x

GoogleTestDir = ./GTestLib

tag = -i
llflag = -ll

ifdef linux
tag = -n
llflag = -lfl
endif


gtestlib = ./lib

BIN = ./bin/
SRC = ./source/
GSRC = ./gtest/


GLIST = GtestLoad.o
GOBJ_FILES = $(addprefix $(BIN), $(GLIST))

#LIST = Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o Pipe.o BigQ.o
LIST = Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o Function.o RelOp.o Statistics.o Optimizer.o ExecutionEngine.o
OBJ_FILES = $(addprefix $(BIN), $(LIST))


maintest:
	$(CC) ./source/main.cc -o main.o

gtest: $(OBJ_FILES) $(GOBJ_FILES) $(BIN)gtest.o 
	$(CC) -o $(BIN)$@ $(OBJ_FILES) $(GOBJ_FILES) $(BIN)gtest.o -I$(GoogleTestDir)/include -L$(GoogleTestDir)/lib -lgtest -lpthread

test: test-1 test-2-1 test-2-2 test-3

test-1: $(OBJ_FILES) $(BIN)test-1.o
	$(CC) -o $(BIN)$@ $(OBJ_FILES) ./bin/test-1.o $(llflag) -lpthread

test-2-1: $(OBJ_FILES) $(BIN)test-2-1.o
	$(CC) -o $(BIN)$@ $(OBJ_FILES) ./bin/test-2-1.o $(llflag) -lpthread

test-2-2: $(OBJ_FILES) $(BIN)test-2-2.o
	$(CC) -o $(BIN)$@ $(OBJ_FILES) ./bin/test-2-2.o $(llflag) -lpthread
	
test-3: $(OBJ_FILES) $(BIN)test-3.o
	$(CC) -o $(BIN)$@ $(OBJ_FILES) ./bin/test-3.o $(llflag) -lpthread

test-4: $(OBJ_FILES) $(BIN)test-4.o
	$(CC) -o $(BIN)$@ $(OBJ_FILES) ./bin/test-4.o $(llflag) -lpthread

main: $(OBJ_FILES) $(BIN)main.o
	$(CC) -o $(BIN)$@ $(OBJ_FILES) ./bin/main.o $(llflag) -lpthread


bin/%.o: $(SRC)%.cc
	$(CC) -c -g $< -o $@

bin/%.o: $(GSRC)%.cc
	$(CC) -c -g $< -o $@ -I$(GoogleTestDir)/include -L$(GoogleTestDir)/lib -lgtest -lpthread

bin/y.tab.o: $(SRC)Parser.y
	yacc -d $(SRC)Parser.y -o $(SRC)y.tab.c
	g++ -c $(SRC)y.tab.c -o $@

bin/lex.yy.o: $(SRC)Lexer.l
	lex  $(SRC)Lexer.l
	mv lex.yy.c $(SRC)lex.yy.c
	gcc  -c $(SRC)lex.yy.c -o $@

bin/lex.yyfunc.o: $(SRC)LexerFunc.l
	lex -Pyyfunc $(SRC)LexerFunc.l
	mv lex.yyfunc.c $(SRC)lex.yyfunc.c
	gcc  -c $(SRC)lex.yyfunc.c -o $@

bin/yyfunc.tab.o: $(SRC)ParserFunc.y
	#yacc -d $(SRC)ParserFunc.y -o $(SRC)yyfunc.tab.c
	yacc -p "yyfunc" -b "yyfunc" -d $(SRC)ParserFunc.y -o $(SRC)yyfunc.tab.c
	g++ -c $(SRC)yyfunc.tab.c -o $@

clean: 
	rm -f $(BIN)*
	rm -f $(SRC)*tab.c
	rm -f $(SRC)lex.*
	rm -f $(SRC)*tab.h
	rm -f ./data/testTable
	rm -f ./data/itemTable
	rm -f ./data/caritem
	rm -f ./data/*.bin
