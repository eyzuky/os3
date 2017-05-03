//
//  SearchKeyValues.cpp
//  OS3
//
//  Created by Eyal Silberman on 02/05/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//

#include "SearchKeyValues.hpp"
#include "MapReduceClient.h"
#include <string>
#include <vector>
using namespace std;


//---------------------------------------
//-- Search key values implementations --
//---------------------------------------
//-------- constructors -----------------
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

