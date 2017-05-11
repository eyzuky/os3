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
#include <ctime>
#include <sys/time.h>
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
    void logThreadCreated(Sender sender);
    void logThreadTerminated(Sender sender);
    void logMapAndShuffleTime();
    void logReduceAndOutputTime();
    void logFinished();
    void startTimeMap();
    void endTimeMap();
    void startTimeReduce();
    void endTimeReduce();
private:
    struct timeval mapStartTime;
    struct timeval mapEndTime;
    struct timeval reduceStartTime;
    struct timeval reduceEndTime;
    string getTime();
};
#endif /* MapReduceLogger_hpp */
