//
//  SearchMapReduce.cpp
//  OS3
//
//  Created by Eyal Silberman on 30/04/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//


#include "Search.hpp"
#include <dirent.h>
#include <string>
#include "SearchKeyValues.hpp"
using namespace std;
SearchMapReduce::SearchMapReduce(std::string toSearch){
    this->toSearch = toSearch;
}


SearchMapReduce::~SearchMapReduce(){}

void SearchMapReduce::Map(const k1Base *const key, const v1Base *const val) const
{
    
    
    DIR *dir;
    struct dirent *ent;
    const char* dirName = ((const SearchK1* const)key)->directory_name.c_str();
    dir = opendir(dirName);
    SearchV2* searchv2 = new SearchV2();
    if (dir != nullptr)
    {
        ent = readdir(dir);
        while (ent != nullptr)
        {
            searchv2->files.push_back(string(ent->d_name));
            ent = readdir(dir); //todo - test this actually reads everything
        }
        SearchK2 *name = new SearchK2(((const SearchK1 *const)key)->directory_name);
        Emit2(name, searchv2);
    }
    
}

void SearchMapReduce::Reduce(const k2Base *const key, const V2_VEC &vals) const
{
    for (auto f = vals.begin(); f != vals.end(); f++)
    {
        SearchV2* files = (SearchV2*)(*f);
        for (auto name = files->files.begin(); name != files->files.end(); name++)
        {
            if((*name).find(toSearch) != string::npos){
                SearchK3 *file = new SearchK3(*name);
                Emit3(file, nullptr);
            }
        }
    }
    
}
