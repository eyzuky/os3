//
//  MapReduceLogger.hpp
//  OS3
//
//  Created by Eyal Silberman on 11/05/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//

#ifndef MapReduceLogger_hpp
//--------- Defines ----------------
#define MapReduceLogger_hpp
#define LOG_FILE .MapReduceFramework.log
//--------- Includes ----------------
#include <pthread.h>
#include <string>
#include <stdio.h>
#include <fstream>
using namespace std;

//-----------------------------------------------------
//Enum for sender and mapping for the logger into strings
enum Sender { ExecMap, Shuffle, ExecReduce };
pair<Sender, string> senderToString [] = {
    std::pair<Sender, string>(ExecMap, "ExecMap"),
    std::pair<Sender, string>(Shuffle, "Shuffle"),
    std::pair<Sender, string>(ExecReduce, "ExecReduce")
};
//-----------------------------------------------------
class MapReduceLogger
{
public:
    pthread_mutex_t log_mutex;
    fstream log_file;
    
    MapReduceLogger();
    ~MapReduceLogger();
    
    void logInitOfFramework(int multiThreadLevel);
    void threadCreated(Sender sender);
    void threadTerminated(Sender sender);
    void mapAndShuffleTime();
    void reduceAndOutputTime();
    void printFinished();
private:
    string getTime();
};
#endif /* MapReduceLogger_hpp */
