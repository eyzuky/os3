//
//  Search.cpp
//  OS3
//
//  Created by Eyal Silberman on 30/04/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//

#include "Search.hpp"
#include "MapReduceFramework.h"

int main(int argc, char* argv[]){
    
    using namespace std;
    if (argc < 2){
        
        cout<< "Please enter atlist 2 parameters, string to search and folders paths"<< endl;
        exit(1);
    }
    string toSearch(argv[1]);
    SearchMapReduce searcher = SearchMapReduce(toSearch);
    
    

    
    
    
    
    
    
    
}

