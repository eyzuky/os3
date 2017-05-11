//
//  MapReduceLogger.cpp
//  OS3
//
//  Created by Eyal Silberman on 11/05/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//

#include "MapReduceLogger.hpp"
#include <iostream>

#define TO_NANO 1000000000
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

void MapReduceLogger::logInitOfFramework(int multiThreadLevel)
{
    pthread_mutex_lock(&log_mutex);
    log_file << "runMapReduceFramework started with " << std::to_string(multiThreadLevel) << " threads\n";
    pthread_mutex_unlock(&log_mutex);
}

void MapReduceLogger::logThreadCreated(Sender sender)
{
    pthread_mutex_lock(&log_mutex);
    string x;
    switch (sender) {
        case ExecMap:
            x = "ExecMap";
            break;
        case Shuffle:
            x = "Shuffle";
            break;
        case ExecReduce:
            x = "ExecReduce";
            break;
    }
    log_file << "Thread " << x << "created " << getTime() << endl;
    pthread_mutex_unlock(&log_mutex);
}

void MapReduceLogger::logThreadTerminated(Sender sender)
{
    pthread_mutex_lock(&log_mutex);
    string x;
    switch (sender) {
        case ExecMap:
            x = "ExecMap";
            break;
        case Shuffle:
            x = "Shuffle";
            break;
        case ExecReduce:
            x = "ExecReduce";
            break;
    }
    log_file << "Thread " << x << "terminated " << getTime() << endl;
    pthread_mutex_unlock(&log_mutex);
}

void MapReduceLogger::logMapAndShuffleTime()
{
    pthread_mutex_lock(&log_mutex);
    log_file << "Map and Shuffle took " << (mapEndTime.tv_sec - mapStartTime.tv_sec)*TO_NANO + (mapEndTime.tv_usec - mapStartTime.tv_usec)*TO_NANO << " ns\n";
    pthread_mutex_unlock(&log_mutex);
}

void MapReduceLogger::logReduceAndOutputTime()

{
    pthread_mutex_lock(&log_mutex);
    log_file << "Reduce took " << (reduceEndTime.tv_sec - reduceStartTime.tv_sec)*TO_NANO + (reduceEndTime.tv_usec - reduceStartTime.tv_usec)*TO_NANO << " ns\n";
    pthread_mutex_unlock(&log_mutex);
}

void MapReduceLogger::logFinished()
{
    pthread_mutex_lock(&log_mutex);
    log_file << "RunMapReduceFramework finished\n";
    pthread_mutex_unlock(&log_mutex);
}

void MapReduceLogger::startTimeMap()
{
    gettimeofday(&mapStartTime, NULL);
}
void MapReduceLogger::endTimeMap()
{
    gettimeofday(&mapEndTime, NULL);
}
void MapReduceLogger::startTimeReduce()
{
    gettimeofday(&reduceStartTime, NULL);
}
void MapReduceLogger::endTimeReduce()
{
    gettimeofday(&reduceEndTime, NULL);
}

string MapReduceLogger::getTime()
{
   string x = "";
   time_t now = time(0);
    struct tm * components = localtime(&now);
    x = "[" + to_string(components->tm_mday) + "." + to_string(components->tm_mon) + "." + to_string(components->tm_year) + " " + to_string(components->tm_hour) + ":" + to_string(components->tm_min) + ":" + to_string(components->tm_sec) + "]";
   return x;
}
