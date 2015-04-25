#include "gtest/gtest.h"
#include "../source/DBFile.h"
#include "../source/Record.h"
#include <string>

using namespace std;

// my first fancy addition test
TEST (DBFile, load)
{
    DBFile dbfile = DBFile();
    
    string dbfile_dir = "/Users/westsnow/Documents/DBIDATA/output/"; // dir where binary heap files should be stored
    string tpch_dir = "/Users/westsnow/Documents/DBIDATA/input/"; // dir where dbgen tpch files (extension *.tbl) can be found
    string catalog_path = "/Users/westsnow/GitHub/dbi/source/catalog"; // full path of the catalog file
    char* tablename = "nation";

    char *fpath = (char*)((dbfile_dir+tablename+".b").c_str());
    
    dbfile.Create(fpath, heap, NULL);
    dbfile.Open(fpath);

    Schema schema( (char*)(catalog_path.c_str()), tablename);
    char *tblFile = (char*)((tpch_dir + tablename + ".tbl").c_str());
    //dbfile.Load( &schema,  tblFile );
    dbfile.Load(*(new Schema ("/Users/westsnow/GitHub/dbi/source/catalog", "nation")),  "/Users/westsnow/Documents/DBIDATA/input/nation.tbl");

//cout<<dbfile.getPageNumber()<<endl;
    
    EXPECT_EQ(dbfile.getPageNumber(), 4);

    dbfile.Close();

}
