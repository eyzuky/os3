//
//  Search.cpp
//  OS3
//
//  Created by Eyal Silberman on 30/04/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//

#include "Search.hpp"
#include "MapReduceFramework.h"
#include "SearchKeyValues.hpp"

#define MULTI_THREADED_LEVEL 10
int main(int argc, char* argv[]){
    
    using namespace std;
    if (argc < 2){
        
        cout<< "Please enter atlist 2 parameters, string to search and folders paths"<< endl;
        exit(1);
    }
    string toSearch(argv[1]);
    SearchMapReduce searcher = SearchMapReduce(toSearch);
    
    
    IN_ITEMS_VEC inputVector;
    
    for (int i = 2; i < argc; ++i)
    {
        SearchK1* directory = new SearchK1(argv[i]);
        inputVector.push_back(make_pair((k1Base*)directory, (v1Base*)nullptr));
        
    }
    
    OUT_ITEMS_VEC outputVector = RunMapReduceFramework(searcher, inputVector, MULTI_THREADED_LEVEL, true);
    //todo: print the files 
    return 0;
    
    
}

