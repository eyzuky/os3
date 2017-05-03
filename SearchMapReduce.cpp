//
//  SearchMapReduce.cpp
//  OS3
//
//  Created by Eyal Silberman on 30/04/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//


#include "Search.hpp"


SearchMapReduce::SearchMapReduce(std::string toSearch){
    this->toSearch = toSearch;
}


SearchMapReduce::~SearchMapReduce(){}

void SearchMapReduce::Map(const k1Base *const key, const v1Base *const val) const{
    
    
}

void SearchMapReduce::Reduce(const k2Base *const key, const V2_VEC &vals) const{
    
    
}
