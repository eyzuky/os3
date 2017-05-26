//
//  MapReduceFramework.cpp
//  OS3
//
//  Created by Eyal Silberman on 04/05/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//
#ifndef MAPREDUCECLIENT_C
#define MAPREDUCECLIENT_C

#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <vector>
#include <pthread.h>
#include <list>
#include <map>
#include <stdio.h>
#include <ctime>
#include <sys/time.h>
#include <semaphore.h>
#include <iostream>
#include <fstream>
#include <ostream>
#include <algorithm>
#include <stdlib.h>
#include <mutex>
//=============
// ==============
//DEFINES
#define BULK 10
#define FAIL -1
#define TO_NANO 1000000000
#define LOG_FILE ".MapReduceFramework.log"

using namespace std;

//===============TYPEDEFS================
struct compk2{
    bool operator() (const k2Base* first, const k2Base* second) const{
        return *first < *second;
    }
};

typedef pair<k2Base*,v2Base*> map_pair;             // INTERMEDIATE_ITEM;
typedef pair<k2Base*, V2_VEC*> shuffled_pair;

typedef map<k2Base*, V2_VEC*, compk2> shuffled_map;        // REDUCE_ITEMS_LIST
typedef vector<OUT_ITEM> reduced_list;              // OUT_ITEMS_QUEUE
//typedef pair<pthread_t, map_pair_list> thread_and_list; // threadsOutput
//typedef vector<thread_and_list> threads_and_their_list; //

//=======================logger============================
//Enum for sender and mapping for the logger into strings
enum Sender { ExecMap, Shuffle, ExecReduce };
pair<Sender, string> senderToString [] = {
        std::pair<Sender, string>(ExecMap, "ExecMap"),
        std::pair<Sender, string>(Shuffle, "Shuffle"),
        std::pair<Sender, string>(ExecReduce, "ExecReduce")
};

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

struct compForThread{
    inline bool operator() (const pthread_t& first, const pthread_t& second) const{
        return (first < second);
    }
};
MapReduceLogger::MapReduceLogger ()
{
    log_file.open(".MapReduceFramework.log", fstream::out);
    pthread_mutex_init(&log_mutex, NULL);
    if(log_file.fail() == true)
    {
        cerr << "fail to open file";
        exit(FAIL);
    }
}

MapReduceLogger::~MapReduceLogger ()
{
    log_file.close();
    pthread_mutex_destroy(&log_mutex);
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
    log_file << "Thread " << x << " created " << getTime() << endl;
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
    log_file << "Thread " << x << " terminated " << getTime() << endl;
    pthread_mutex_unlock(&log_mutex);
}

void MapReduceLogger::logMapAndShuffleTime()
{
    pthread_mutex_lock(&log_mutex);
    log_file << "Map and Shuffle took " << (mapEndTime.tv_sec - mapStartTime.tv_sec)*TO_NANO + (mapEndTime.tv_usec - mapStartTime.tv_usec) << " ns\n";
    pthread_mutex_unlock(&log_mutex);
}

void MapReduceLogger::logReduceAndOutputTime()
{
    pthread_mutex_lock(&log_mutex);
    log_file << "Reduce took " << (reduceEndTime.tv_sec - reduceStartTime.tv_sec)*TO_NANO + (reduceEndTime.tv_usec - reduceStartTime.tv_usec) << " ns\n";
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
//===============================

map<pthread_t, int> thread_indicator;

//===============================
//GLOBALS AND MUTEX
unsigned int MultiThreadLevel;

sem_t sem;

struct thread_items_map {
    thread_items_map(pthread_mutex_t m, vector<pair<k2Base*, v2Base*>> * pairs) : mutex(m), pairs_vec(pairs){}
    pthread_mutex_t mutex;
    vector<pair<k2Base*, v2Base*>> * pairs_vec;
};

vector<thread_items_map*> thread_items_vec;

map<pthread_t, int> map_thread_to_index;

map<int, OUT_ITEMS_VEC> int_to_out_items_vec;

pthread_t shuffle_thread;

pthread_mutex_t map_mutex;
pthread_mutex_t index_mutex;
pthread_mutex_t mapOver_mutex;
map <pthread_t, pthread_mutex_t, compForThread> thread_mutex_map; //threadsItemsListsMutex
map <pthread_t, pthread_mutex_t, compForThread> thread_mutex_reduce; //threadsItemsListsMutex

map <pthread_t, vector<pair<k2Base*, v2Base*>>*> thread_list_map;//threadsItemsListsTemp
map<pthread_t, vector<OUT_ITEM>*> thread_list_reduce;

shuffled_map shuffled;
bool mapPartOver = false;

MapReduceLogger *logger;

static void exceptionCaller(string exc){
    cerr << exc;
    exit(FAIL);
}

class mapDataHandler {

private:
    unsigned int _bulkIndex;
    const IN_ITEMS_VEC _items;
    const MapReduceBase& _mapReduceBase;

public:
    mapDataHandler(IN_ITEMS_VEC &items_vec, const MapReduceBase& mapReduceBase): _items(items_vec), _mapReduceBase(mapReduceBase){
        _bulkIndex = 0;
    }
    void proceedToNextBulk()
    {
        this->_bulkIndex = BULK + _bulkIndex;
    }
    unsigned int getCurrentIndex()
    {
        return this->_bulkIndex;
    }
    unsigned long getSize() {
        return this->_items.size();
    }
    void applyMap(const k1Base *const keyOne, const v1Base *const valueOne)
    {
        this->_mapReduceBase.Map(keyOne,valueOne);
    }
    IN_ITEM getItem(unsigned int index){
        return this->_items[index];
    }
};

class reduceDataHandler {

private:
    unsigned int _bulkIndex;
    const MapReduceBase& _mapReduceBase;

public:
    vector<shuffled_pair> items;
    reduceDataHandler(vector<shuffled_pair> &items_vec, const MapReduceBase& mapReduceBase):  _mapReduceBase(mapReduceBase){
        _bulkIndex = 0;
        items = items_vec;
    }

    unsigned int nextBulkIndex() const { return _bulkIndex;}

    unsigned int proceedToNextBulk()
    {
        this->_bulkIndex = BULK + _bulkIndex;
        return this->_bulkIndex;
    }
    unsigned int getSize()
    {
        return this->items.size();
    }
    unsigned int getCurrentIndex()
    {
        return this->_bulkIndex;
    }
    void applyReduce(const k2Base *const key, const V2_VEC* vec)
    {
        this->_mapReduceBase.Reduce(key, *vec);
    }
    shuffled_pair getItem(unsigned int index){
        return this->items[index];
    }
};

void * frameworkInitialization(){

    logger = new MapReduceLogger();
    logger->startTimeReduce();
    if(sem_init(&sem, 0 ,0) == FAIL){
        exceptionCaller(" Sem init Fail");
    }
    // mutex init
    pthread_mutex_init(&map_mutex, NULL);
    pthread_mutex_init(&index_mutex, NULL);
    pthread_mutex_init(&mapOver_mutex, NULL);

    return nullptr;
}

void * frameworkDistraction(){

    delete logger;

    // mutex destroy
    pthread_mutex_destroy(&map_mutex);
    pthread_mutex_destroy(&index_mutex);
    pthread_mutex_destroy(&mapOver_mutex);

    sem_destroy(&sem);
    thread_mutex_map.clear();
    thread_list_map.clear(); //threadsItemsListsTemp
    thread_list_reduce.clear();
    shuffled.clear();
    //delete [] reduce_threads;
    return nullptr;
}

void * mapExec(void * data){

    pthread_mutex_lock(&map_mutex);
    pthread_mutex_unlock(&map_mutex);

    int test; //test will check for errors and exceptions
    IN_ITEM item;

    mapDataHandler * handler = (mapDataHandler*) data;
    unsigned int curIndex = 0;
    unsigned long size = handler->getSize();

    while (true) {

        test = pthread_mutex_lock(&index_mutex);

        if (test == FAIL){
            exceptionCaller("Failed to lock mutex");
        }
        curIndex = handler->getCurrentIndex();
        handler->proceedToNextBulk();
        test = pthread_mutex_unlock(&index_mutex);
        if (test == FAIL){
            exceptionCaller("Failed to unlock mutex");
        }
        if (curIndex >= size){
            break;
        }
        for (unsigned int i = 0; (i <  BULK) && (i + curIndex< size) ; ++i)
        {
            pthread_mutex_lock(&index_mutex);
            item = handler->getItem(i+curIndex);
            const k1Base *const key = item.first;
            const v1Base *const val = item.second;
//            cout << i << " map on pair: ( " << key << " , " << val << endl;
            handler->applyMap(key, val);
            pthread_mutex_unlock(&index_mutex);

        }
    }
    return nullptr;
}

void * joinQueues() {

    for(unsigned int i = 0; i < MultiThreadLevel; ++i)
    {
        thread_items_map items = *(thread_items_vec.at(i));
        if (items.pairs_vec->size() != 0)
        {
            pthread_mutex_lock(&items.mutex);
            k2Base * key = items.pairs_vec->back().first;
            v2Base * value = items.pairs_vec->back().second;
//            unsigned long tmp = shuffled.(pair.first);
            if(key == NULL || value == NULL){
                items.pairs_vec->pop_back();
                continue;
            }
            try {
                shuffled.at(key)->push_back(value);
            }
            catch(out_of_range)
            {
                V2_VEC *newVec = new V2_VEC;
                newVec->push_back(value);
                shuffled.insert(pair<k2Base*, V2_VEC*>(key,newVec));
            }
            items.pairs_vec->pop_back();
            pthread_mutex_unlock(&items.mutex);
        }

//        for(auto pair : *items.pairs_vec)
//        {
//            unsigned long tmp = shuffled.count(pair.first);
//            if(tmp > 0)
//            {
//                shuffled[pair.first].push_back(pair.second);
//            } else {
//                V2_VEC *newVec = new V2_VEC;
//                newVec->push_back(pair.second);
//                shuffled[pair.first] = *newVec;
//            }
//
//        }
//        items.pairs_vec->clear();
//        pthread_mutex_unlock(&items.mutex);
    }


    return nullptr;
}

void * shuffle(void * data){
    if (data == nullptr){

    }

    logger->logThreadCreated(Shuffle);
    int samphoreValue = 0;
    sem_getvalue(&sem, &samphoreValue);
    do {
        while (!mapPartOver || samphoreValue > 0) {
            sem_getvalue(&sem, &samphoreValue);
            //        if (samphoreValue == 0){
            //            continue;
            //        }
            if (sem_wait(&sem) == FAIL) {
                exceptionCaller("sem wait failure");
            }
            joinQueues();
            sem_getvalue(&sem, &samphoreValue);
        }
        sem_getvalue(&sem, &samphoreValue);
    } while(samphoreValue > 0);
    return nullptr;
}

void * reduceExec(void * data){
    pthread_mutex_lock(&map_mutex);
    pthread_mutex_unlock(&map_mutex);
    int test;
    reduceDataHandler *handler = (reduceDataHandler*)data;
    unsigned int curIndex = 0;
    unsigned int size = handler->getSize();
    while ( true ) {
        test = pthread_mutex_lock(&index_mutex);
        if (test == FAIL){
            exceptionCaller("Failed to lock mutex");
        }
        curIndex = handler->getCurrentIndex();
        handler->proceedToNextBulk();
        test = pthread_mutex_unlock(&index_mutex);
        if (test == FAIL){
            exceptionCaller("Failed to unlock mutex");
        }
        if (curIndex >= size){
            break;
        }
        for(unsigned int i = curIndex; i < curIndex + BULK; i++){
            if(i >= size)
            {
                break;
            }
            pthread_mutex_lock(&index_mutex);
            shuffled_pair pair = handler->getItem(i);
            const k2Base *const key = pair.first;
            const V2_VEC* vec = pair.second;
            handler->applyReduce(key, vec);
            pthread_mutex_unlock(&index_mutex);
        }
    }

    return nullptr;
}

struct compk3{
    bool operator() (OUT_ITEM first, OUT_ITEM second) const {
        return (*first.first < *second.first);
    }
};

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2) {
    MultiThreadLevel = multiThreadLevel;
    pthread_t *map_threads = new pthread_t[MultiThreadLevel];

    //map_threads = new pthread_t[MultiThreadLevel];

    frameworkInitialization();

    mapDataHandler map_handler = mapDataHandler(itemsVec, mapReduce);
    //create items for threads
    for (unsigned int i = 0; i < MultiThreadLevel; ++i) {
        pthread_mutex_t mutex;
        pthread_mutex_init(&mutex, NULL);
        thread_items_map * items = new thread_items_map(mutex, new vector<pair<k2Base *, v2Base *>>);
        thread_items_vec.push_back(items);
    }

    int test = pthread_mutex_lock(&map_mutex);
    //create threads

    for (unsigned int i = 0; i < MultiThreadLevel; ++i) {
        logger->logThreadCreated(ExecMap);
        pthread_create(&map_threads[i], NULL, mapExec, &map_handler);
        map_thread_to_index[map_threads[i]] = i;

    }

    test = pthread_mutex_unlock(&map_mutex);
    if (test == FAIL) {
        exceptionCaller("Failed to unlock mutex");
    }
    mapPartOver = false;
    test = pthread_create(&shuffle_thread, NULL, shuffle, nullptr);


    for (unsigned int i = 0; i < multiThreadLevel; ++i) {
        logger->logThreadTerminated(ExecMap);
        test = pthread_join(map_threads[i], NULL);
        if (test == FAIL) {
            exceptionCaller("Failed to join threads");
        }
    }
    mapPartOver = true;
    sem_post(&sem);
    pthread_join(shuffle_thread, NULL);
    cout << "shuffled.size():  " << shuffled.size() << endl;

    //DESTROY STUFF
    for (unsigned int i = 0; i < MultiThreadLevel; ++i) {
        thread_items_map items = *thread_items_vec.at(i);
        pthread_mutex_destroy(&(items.mutex));
    }
    for (auto iter = shuffled.begin(); iter != shuffled.end(); iter++) {
        cout << iter->first << " , " << (*iter->second).size() << endl;
    }

    //moving data to different container before reduce
    vector<shuffled_pair> shuffledVec;
    for (auto iter = shuffled.begin(); iter != shuffled.end(); iter++) {
        shuffledVec.push_back(pair<k2Base *, V2_VEC *>((*iter).first, (*iter).second));
    }
    cout << "shuffledVec.size():  " << shuffledVec.size() << endl;

    //creating reduce handler
    reduceDataHandler reduce_handler = reduceDataHandler(shuffledVec, mapReduce);

    test = pthread_mutex_lock(&map_mutex);

    map_thread_to_index.clear();
    for (unsigned int i = 0; i < MultiThreadLevel; i++) {
        OUT_ITEMS_VEC out_vec_vec;
        int_to_out_items_vec[i] = out_vec_vec;
    }
    for (unsigned int i = 0; i < MultiThreadLevel; i++) {
        logger->logThreadCreated(ExecReduce);
        test = pthread_create(&map_threads[i], NULL, reduceExec, &reduce_handler);
        map_thread_to_index[map_threads[i]] = i;
    }

    pthread_mutex_unlock(&map_mutex);

    OUT_ITEMS_VEC out_items_vec;

    for (int i = 0; i < multiThreadLevel; ++i) {
        logger->logThreadTerminated(ExecReduce);
        test = pthread_join(map_threads[i], NULL);
        if (test == FAIL) {
            exceptionCaller("Failed to join threads");
        }
        cout << "int_to_out_items_vec.size:  " << (int_to_out_items_vec[i]).size() << endl;
    }

//    for (unsigned int i = 0; i < MultiThreadLevel; ++i) {
//        while(thread_items_vec[i]->pairs_vec->empty())
//        {
//            auto todelete = thread_items_vec[i]->pairs_vec->back();
//            thread_items_vec[i]->pairs_vec->pop_back();
//            delete(&todelete);
//        }
//        delete(thread_items_vec[i]);
//    }

    for (unsigned int i = 0; i < MultiThreadLevel; ++i) {
        (out_items_vec).insert((out_items_vec).end(), (int_to_out_items_vec[i]).begin(),
                                (int_to_out_items_vec[i]).end()); // todo check1?
    }

    cout << "out_items_vec.size:  " << out_items_vec.size() << endl;

    for (auto iter = shuffled.begin(); iter != shuffled.end(); iter++) {
        delete (*iter).second;
    }

    cout << "out_items_vec.size:  " << out_items_vec.size() << endl;

    for (auto list = thread_list_reduce.begin(); list != thread_list_reduce.end(); ++list) {
        reduced_list reduced = *list->second;

        if (autoDeleteV2K2) {
            for (auto toDelete = reduced.begin(); toDelete != reduced.end(); ++toDelete) {
                toDelete->first = nullptr;
                toDelete->second = nullptr;
            }
        }
    }

    delete [] map_threads;
    logger->endTimeReduce();
    logger->logReduceAndOutputTime();
    logger->logFinished();
    frameworkDistraction();
    return out_items_vec;

}

//
//
//    ===========================================================================
//     log info
//    ===========================================================================
//    logger->logInitOfFramework(multiThreadLevel);
//    logger->startTimeMap();
    //===========================================================================
    // map and shuffle
//
//    test = pthread_mutex_lock(&map_mutex);
//    if (test == FAIL){
//        exceptionCaller("Failed to lock mutex");
//    }

//    for (unsigned long i = 0; i < MultiThreadLevel; i++)
//    {
//        logger->logThreadCreated(ExecMap);
//        test = pthread_create(&map_threads[i], NULL, mapExec, &map_handler);
//        thread_list_map[map_threads[i]] = new vector<pair<k2Base*, v2Base*>>;
//        pthread_mutex_init(&thread_mutex_map[map_threads[i]],NULL);
//        if (test == FAIL){
//            exceptionCaller("Failed to create thread");
//        }
//    }
//    test = pthread_mutex_unlock(&map_mutex);
//    if (test == FAIL){
//        exceptionCaller("Failed to unlock mutex");
//    }
//
//
//    test = pthread_create(&shuffle_thread, NULL, shuffle, nullptr);
//    if (test == FAIL){
//        exceptionCaller("Failed to create shuffle thread");
//    }
//
//    for (int i = 0; i < multiThreadLevel; ++i)
//    {
//        logger->logThreadTerminated(ExecMap);
//        test = pthread_join(map_threads[i], NULL);
//        if (test == FAIL){
//            exceptionCaller("Failed to join threads");
//        }
//    }
//    if(sem_post( &sem) == FAIL){
//        exceptionCaller("sem post fail");
//    }
//    mapPartOver = true;
//
//    logger->logThreadTerminated(Shuffle);
//    test = pthread_join(shuffle_thread, NULL);
//    if (test == FAIL){
//        exceptionCaller("Failed to join threads");
//    }
//    for (auto iter = thread_mutex_map.begin(); iter != thread_mutex_map.end(); ++iter)
//    {
//        test = pthread_mutex_destroy(&(iter->second));
//        if (test == FAIL){
//            exceptionCaller("Failed to destroy threads");
//        }
//    }
    //===========================================================================
    // end of map part - log info
    //============================================================================
//    logger->endTimeMap();
//    logger->logMapAndShuffleTime();
//    logger->startTimeReduce();


    //============================================================================
    //reduce and output
//
//    if (test == FAIL){
//        exceptionCaller("Failed to lock mutex");
//    }
    //cout << "shuffled.size() : " << shuffled.size() << endl ;
    //=== shuffle_map -> shuffle_vec
//    vector<shuffled_pair> shuffledVec;
//    for(auto iter = shuffled.begin(); iter !=shuffled.end(); iter++){
//        shuffledVec.push_back(make_pair(iter->first, iter->second));
//    }

//    reduceDataHandler reduce_handler = reduceDataHandler(shuffledVec, mapReduce);
//     cout << "shuffledVec.size() : " << shuffledVec.size() << endl ;
//    test = pthread_mutex_lock(&map_mutex);
//
//    for (unsigned long i = 0; i < MultiThreadLevel; i++)
//    {
//        thread_list_reduce[map_threads[i]] = new OUT_ITEMS_VEC;
//    }
//    for (unsigned long i = 0; i < MultiThreadLevel; i++) {
//        pthread_mutex_init(&thread_mutex_reduce[map_threads[i]], NULL);
//    }
//    for (int i = 0; i < multiThreadLevel; ++i)
//    {
//        logger->logThreadCreated(ExecReduce);
       // cout << "reduce thread created :  " << i << endl;
//        test = pthread_create(&map_threads[i], NULL, reduceExec, &reduce_handler);
//        if (test == FAIL){
//            exceptionCaller("Failed to create thread");
//        }
//    }
//    test = pthread_mutex_unlock(&map_mutex);
//    if (test == FAIL){
//        exceptionCaller("Failed to unlock mutex");
//    }


//    OUT_ITEMS_VEC *out_items_vec = new OUT_ITEMS_VEC;

//    for (int i = 0; i < multiThreadLevel; ++i)
//    {
//        logger->logThreadTerminated(ExecReduce);
       // cout << "reduce thread treminated :  " << i << endl;
//        test = pthread_join(map_threads[i] , NULL);
//        if (test == FAIL){
//            exceptionCaller("Failed to join threads");
//        }
//    }

//    for (auto out_vec: thread_list_reduce)
//    {
//
//        (*out_items_vec).insert((*out_items_vec).end(), (*out_vec.second).begin(), (*out_vec.second).end()); // todo check1?
//    }
//
//    for (auto iter = thread_mutex_reduce.begin(); iter != thread_mutex_reduce.end(); ++iter)
//    {
//        test = pthread_mutex_destroy(&(iter->second));
//        if (test == FAIL){
//            exceptionCaller("Failed to destroy threads");
//        }
//    }
//
//    for (auto list = thread_list_reduce.begin(); list != thread_list_reduce.end(); ++list)
//    {
//        reduced_list reduced = *list->second;
//        if(autoDeleteV2K2)
//        {
//            for( auto toDelete=reduced.begin(); toDelete != reduced.end(); ++toDelete){
//                toDelete->first = nullptr;
//                toDelete->second = nullptr;
//            }
//        }
//    }
//    cout << "out_items_vec.size:  " << (*out_items_vec).size() << endl;
//
//    std::sort(out_items_vec->begin(), out_items_vec->end(), compk3());
    //============================================================================
    // log info ==================================================================
    //============================================================================
//    logger->endTimeReduce();
//    logger->logReduceAndOutputTime();
//    logger->logFinished();
//    frameworkDistraction();
//    return *out_items_vec;
//}

void Emit2 (k2Base* key, v2Base* val)
{
    if ( key == NULL || val == NULL) {
        exceptionCaller("Emit 2 was called with null");
        return;

    } else {
        pair<k2Base *, v2Base *> *p = new pair<k2Base *, v2Base *>(make_pair(key,val));
        thread_items_map items = *(thread_items_vec.at(map_thread_to_index[pthread_self()]));
        pthread_mutex_lock(&(items.mutex));
        cout << "thread " << map_thread_to_index[pthread_self()] << " emits 2 " << p->first << " : " << p->second << endl;
        items.pairs_vec->push_back(*p);
        if (sem_post(&sem) == FAIL) {
            exceptionCaller(" sem Post fail");
        }
        pthread_mutex_unlock(&(items.mutex));
    }
}

void Emit3 (k3Base* key, v3Base* val)
{
    if ( key == NULL || val == NULL) {
        exceptionCaller("Emit 3 was called with null");
        return;
    }
    int index = map_thread_to_index[pthread_self()];
    int_to_out_items_vec[index].push_back(std::make_pair(key, val));

}

#endif MAPREDUCECLIENT_C
