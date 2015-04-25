Team Members(name + ufid):
shuai wu		    78892935
Miguel Rodriguez	81984646


Organazation:
./
./readme.txt
./Makefile
./source/
./bin/
./gtest
./GTestLib

1. code files are in source directory.
2. XXX.o and executable files are in bin directory
3. gtest contains codes for google test.
4. GTestLib contains some .h and lib files that google test file may need.

Run & Test my program
1. To run the test.cc, just type "make test-3”, then do “./bin/test".
	before running the test.cc, you need to modify the test.cc file, set the three file paths. 
2. To run google test, type "make gtest", then go to ./bin -> ".gtest"
	However, GTestLib can be used under OSX, if you wanna run it on linux, you need to modify line 3 and set GoogleTestDir = "your google test's lib and header file path"

IMPORTANT NOTICE:
before compiling, you need to set the file path. You need to (1)set test.cat file and (2)go to test-3.h and modify line 17, set the path of test.cat file.