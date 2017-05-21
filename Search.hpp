//
//  Search.hpp
//  OS3
//
//  Created by Eyal Silberman on 30/04/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//

#include <stdio.h>
#include "MapReduceFramework.h"
#include <iostream>
#include <string>

#ifndef Search_hpp
#define Search_hpp

class SearchMapReduce: public MapReduceBase {
public:
    //variables
    std::string toSearch;
    
    /*
     overrides
     */
    virtual void Map(const k1Base *const key, const v1Base *const val) const override;
    virtual void Reduce(const k2Base *const key, const V2_VEC &vals) const override;
    
    //constructors and destructors
    SearchMapReduce(std::string toSearch);
    ~SearchMapReduce();
    
};


#endif /* Search_hpp */
