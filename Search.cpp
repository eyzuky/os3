//
//  Search.cpp
//  OS3
//
//  Created by Eyal Silberman on 30/04/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//

#include "Search.hpp"
#include <dirent.h>
#define MULTI_THREADED_LEVEL 10


class SearchK1: public k1Base
{
public:
    SearchK1(const string name);
    ~SearchK1();
    string directory_name;
    virtual bool operator<(const k1Base &other) const override;
};
//-------- k2Base ------------------
class SearchK2: public k2Base
{
public:
    SearchK2(const string name);
    ~SearchK2();
    string file_name;
    virtual bool operator<(const k2Base &other) const override;
};
//-------- k3Base ------------------
class SearchK3: public k3Base
{
public:
    SearchK3(string name);
    ~SearchK3();
    string name;
    virtual bool operator<(const k3Base &other) const override;
};
//-------- v2Base ------------------
class SearchV2: public v2Base
{
public:
    vector<string> files;
    SearchV2();
    ~SearchV2();
};
//===============================
SearchK1::SearchK1(const string name): directory_name(name) {}
SearchK2::SearchK2(const string name): file_name(name) {}
SearchK3::SearchK3(const string name): name(name) {}
SearchV2::SearchV2(){}
//-------- destructors ------------------
SearchK1::~SearchK1(){}
SearchK2::~SearchK2(){}
SearchK3::~SearchK3(){}
SearchV2::~SearchV2(){}

//-------- operator overloads -----------
bool SearchK1::operator<(const k1Base &other) const
{
    SearchK1 *castedK1 = ((SearchK1*) &other);
    return this->directory_name < castedK1->directory_name;
}
bool SearchK2::operator<(const k2Base &other) const
{
    SearchK2 *castedK2 = ((SearchK2*) &other);
    return this->file_name < castedK2->file_name;
}
bool SearchK3::operator<(const k3Base &other) const
{
    SearchK3 *castedK3 = ((SearchK3*) &other);
    return this->name < castedK3->name;
}

int main(int argc, char* argv[]){
    
    using namespace std;
    if (argc < 2){
        
        cout<< "Please enter at list 2 parameters, string to search and folders paths"<< endl;
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
    for(auto f = outputVector.begin(); f != outputVector.end(); ++f)
    {

    }
    return 0;
    
    
}

SearchMapReduce::SearchMapReduce(std::string toSearch){
    this->toSearch = toSearch;
}


SearchMapReduce::~SearchMapReduce(){}

void SearchMapReduce::Map(const k1Base *const key, const v1Base *const val) const
{
    if(val != NULL){
       exit(-1);
    }
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
    if(key != NULL){

    }
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
