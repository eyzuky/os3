//
//  MapReduceLogger.cpp
//  OS3
//
//  Created by Eyal Silberman on 11/05/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//

#include "MapReduceLogger.hpp"


MapReduceLogger::MapReduceLogger ()
{
    log_file.open(".MapReduceFramework.log");
    if(log_file.fail())
       {
        //todo - print the correct error message
       }
    
    
}

MapReduceLogger::~MapReduceLogger ()
{
    log_file.close();
}

