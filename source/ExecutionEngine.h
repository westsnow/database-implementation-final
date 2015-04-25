#ifndef EXECUTIONENGINE_H
#define EXECUTIONENGINE_H

#include "Statistics.h"
#include <algorithm>
#include <iostream>

using namespace std;

class ExecutionEngine {
	private:
		Statistics *s;


	public:

		ExecutionEngine(Statistics *st);
		~ExecutionEngine();	

		void execute();
		void select();
		void createTable();
		void insertInto();
		void dropTable();
		void setOutput(char *mode);
		void clear();
		bool existance(char *tableName);	

		static inline std::string &ltrim(std::string &s) {
			s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
			return s;
		}

		static inline std::string &rtrim(std::string &s) {
			s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
			return s;
		}

		static inline std::string &trim(std::string &s) {
			return ltrim(rtrim(s));
		}	

		
};



#endif
