//
//  SearchKeyValues.hpp
//  OS3
//
//  Created by Eyal Silberman on 02/05/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//

#ifndef SearchKeyValues_hpp
#define SearchKeyValues_hpp

#include <stdio.h>
#include "SearchKeyValues.hpp"
#include "MapReduceClient.h"
#include <string>
#include <vector>
using namespace std;
//----------------------------------
//-- key value types declarations --
//----------------------------------
//--------- k1Base -----------------
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


#endif /* SearchKeyValues_hpp */
