//
//  MapReduceFramework.cpp
//  OS3
//
//  Created by Eyal Silberman on 04/05/2017.
//  Copyright © 2017 Eyal Silberman. All rights reserved.
//

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
typedef pair<k2Base*, V2_VEC> shuffled_pair;

typedef map<k2Base*, V2_VEC, compk2> shuffled_map;        // REDUCE_ITEMS_LIST
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

//===============================
//GLOBALS AND MUTEX
unsigned int MultiThreadLevel;

sem_t sem;

pthread_t * map_threads;
pthread_t * reduce_threads;
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

pthread_cond_t exec_shuffle_notification;
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
    unsigned int getSize() {
        return this->_items.size();
    }
    void applyMap(const k1Base *const keyOne, const v1Base *const valueOne)
    {
      //  cout << "applying map " << pthread_self() << endl;
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
    void applyReduce(const k2Base *const key, const V2_VEC vec)
    {
        this->_mapReduceBase.Reduce(key, vec);
    }
    shuffled_pair getItem(unsigned int index){
        return this->items[index];
    }
};

void * frameworkInitialization(){

    map_threads = new pthread_t[MultiThreadLevel];
    reduce_threads = new pthread_t[MultiThreadLevel];
    logger = new MapReduceLogger();
    if(sem_init(&sem, 0 ,0) == FAIL){
        exceptionCaller(" Sem init Fail");
    }
    // mutex init
    pthread_mutex_init(&map_mutex, NULL);
    pthread_mutex_init(&index_mutex, NULL);
    pthread_mutex_init(&mapOver_mutex, NULL);
    MultiThreadLevel = 0;
    return nullptr;
}

void * frameworkDistraction(){
    delete logger;


    for (unsigned int i = 0; i < MultiThreadLevel; i++) {
        delete (thread_list_map[map_threads[i]]);
        delete (thread_list_reduce[reduce_threads[i]]);
     }


    // mutex destroy
    pthread_mutex_destroy(&map_mutex);
    pthread_mutex_destroy(&index_mutex);
    pthread_mutex_destroy(&mapOver_mutex);

    sem_destroy(&sem);
    thread_mutex_map.clear();
    thread_list_map.clear(); //threadsItemsListsTemp
    thread_list_reduce.clear();
    shuffled.clear();
    //delete [] map_threads;
    //delete [] reduce_threads;
    return nullptr;
}

void * mapExec(void * data){
    pthread_mutex_lock(&map_mutex);
  //  cout << "Thread " << thread_indicator[pthread_self()] << " in mapExec\n";
    pthread_mutex_unlock(&map_mutex);
    int test; //test will check for errors and exceptions
    logger->logThreadCreated(ExecMap);
    IN_ITEM item;

    mapDataHandler * handler = (mapDataHandler*) data;
    unsigned int curIndex = 0;
    unsigned int size = handler->getSize();

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
        for (unsigned int i = curIndex; (i < curIndex + BULK) && (i < size) ; ++i)
        {
            item = handler->getItem(i);
            handler->applyMap(item.first, item.second);
        }
    }
    return nullptr;
}

void * joinQueues() {

    for(unsigned int i = 0; i < MultiThreadLevel; ++i)
    {
        pthread_mutex_lock(&thread_mutex_map[map_threads[i]]);
        vector<pair<k2Base*, v2Base*>> vec = *(thread_list_map[map_threads[i]]);

        // cout << "vec size" << vec.size() << endl;
        for(auto pair : vec)
        {
            unsigned long tmp = shuffled.count(pair.first);
            if(tmp > 0)
            {
                shuffled[pair.first].push_back(pair.second);
            } else {
                V2_VEC *newVec = new V2_VEC;
                newVec->push_back(pair.second);
                shuffled[pair.first] = *newVec;
            }

        }
        pthread_mutex_unlock(&thread_mutex_map[map_threads[i]]);
    }


    return nullptr;
}

void * shuffle(void * data){
   // cout << "Thread " << thread_indicator[pthread_self()] << " in shuffle\n";
    if (data == nullptr){

    }
    logger->logThreadCreated(Shuffle);
    int samphoreValue = 0;
    sem_getvalue(&sem, &samphoreValue);

    while (!mapPartOver || samphoreValue > 0)
    {

        if (samphoreValue == 0){
            continue;
        }
        joinQueues();
        sem_getvalue(&sem, &samphoreValue);
        if (sem_wait(&sem) == FAIL){
            exceptionCaller("sem wait failure");
        }
    }
    joinQueues();
    return nullptr;
}

void * reduceExec(void * data){
    pthread_mutex_lock(&map_mutex);
   // cout << "Thread " << thread_indicator[pthread_self()] << " in reduce\n";
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
            shuffled_pair pair = handler->getItem(i);
            handler->applyReduce(pair.first, pair.second);
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
                                    int multiThreadLevel, bool autoDeleteV2K2)
{

    frameworkInitialization();
    MultiThreadLevel = multiThreadLevel;

    int test;
    //===========================================================================
    // log info
    //===========================================================================
    logger->logInitOfFramework(multiThreadLevel);
    logger->startTimeMap();
    //===========================================================================
    // map and shuffle

    mapDataHandler map_handler = mapDataHandler(itemsVec, mapReduce);

    test = pthread_mutex_lock(&map_mutex);
    if (test == FAIL){
        exceptionCaller("Failed to lock mutex");
    }

    //this loop creates a thread, a list for a thread and adds it to our threads-list list.
    for (unsigned long i = 0; i < MultiThreadLevel; i++)    {
        pthread_mutex_init(&thread_mutex_map[map_threads[i]],NULL);
    }

    for (unsigned long i = 0; i < MultiThreadLevel; i++)
    {
        logger->logThreadCreated(ExecMap);
    //    cout << "map thread created :  " << i << endl;
        test = pthread_create(&map_threads[i], NULL, mapExec, &map_handler);
        thread_indicator[map_threads[i]] = i;

        if (test == FAIL){
            exceptionCaller("Failed to create thread");
        }
    }
    test = pthread_mutex_unlock(&map_mutex);
    if (test == FAIL){
        exceptionCaller("Failed to unlock mutex");
    }

    for (unsigned long i = 0; i < MultiThreadLevel; i++)
    {
        pthread_mutex_lock(&thread_mutex_map[map_threads[i]]);
        thread_list_map[map_threads[i]] = new vector<pair<k2Base*, v2Base*>>;
        pthread_mutex_unlock(&thread_mutex_map[map_threads[i]]);
    }
    test = pthread_create(&shuffle_thread, NULL, shuffle, nullptr);
    if (test == FAIL){
        exceptionCaller("Failed to create shuffle thread");
    }

    for (int i = 0; i < multiThreadLevel; ++i)
    {
      //  cout << "thread_list_map[map_threads[i]] : " << thread_list_map[map_threads[i]]->size() << endl ;

      //  cout << "map thread treminated :  " << i << endl;
        logger->logThreadTerminated(ExecMap);
        test = pthread_join(map_threads[i], NULL);
    //    cout << "JOIN MAP: Thread " << i << endl;
        if (test == FAIL){
            exceptionCaller("Failed to join threads");
        }
    }
    if(sem_post( &sem) == FAIL){
        exceptionCaller("sem post fail");
    }
    //pthread_mutex_lock(&mapOver_mutex);
    mapPartOver = true;
    //pthread_mutex_unlock(&mapOver_mutex);

    logger->logThreadTerminated(Shuffle);
    test = pthread_join(shuffle_thread, NULL);
  //  cout << "JOIN shuffle Thread " << endl;
    if (test == FAIL){
        exceptionCaller("Failed to join threads");
    }

    joinQueues();
    for (auto iter = thread_mutex_map.begin(); iter != thread_mutex_map.end(); ++iter)
    {
        test = pthread_mutex_destroy(&(iter->second));
        if (test == FAIL){
            exceptionCaller("Failed to destroy threads");
        }
    }
    //===========================================================================
    // end of map part - log info
    //============================================================================
    logger->endTimeMap();
    logger->logMapAndShuffleTime();
    logger->startTimeReduce();


    //============================================================================
    //reduce and output

    test = pthread_mutex_lock(&map_mutex);
    if (test == FAIL){
        exceptionCaller("Failed to lock mutex");
    }
    //cout << "shuffled.size() : " << shuffled.size() << endl ;
    //=== shuffle_map -> shuffle_vec
    vector<shuffled_pair> shuffledVec;
    for(auto iter = shuffled.begin(); iter !=shuffled.end(); iter++){
        shuffledVec.push_back(make_pair(iter->first, iter->second));
    }

    reduceDataHandler reduce_handler = reduceDataHandler(shuffledVec, mapReduce);
     cout << "shuffledVec.size() : " << shuffledVec.size() << endl ;

    for (unsigned long i = 0; i < MultiThreadLevel; i++) {
        pthread_mutex_init(&thread_mutex_reduce[reduce_threads[i]], NULL);
    }
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        logger->logThreadCreated(ExecReduce);
       // cout << "reduce thread created :  " << i << endl;
        test = pthread_create(&reduce_threads[i], NULL, reduceExec, &reduce_handler);
        if (test == FAIL){
            exceptionCaller("Failed to create thread");
        }
    }
    test = pthread_mutex_unlock(&map_mutex);
    if (test == FAIL){
        exceptionCaller("Failed to unlock mutex");
    }
    for (unsigned long i = 0; i < MultiThreadLevel; i++)
    {
        pthread_mutex_lock(&thread_mutex_reduce[reduce_threads[i]]);
        thread_list_reduce[reduce_threads[i]] = new OUT_ITEMS_VEC;
        pthread_mutex_unlock(&thread_mutex_reduce[reduce_threads[i]]);
    }

    OUT_ITEMS_VEC *out_items_vec = new OUT_ITEMS_VEC;

    for (int i = 0; i < multiThreadLevel; ++i)
    {
        OUT_ITEMS_VEC curItems = (*thread_list_reduce[reduce_threads[i]]);
        cout << "CurItems.size:  " << curItems.size() << endl;
        (*out_items_vec).insert((*out_items_vec).end(), curItems.begin(), curItems.end()); // todo check1?
        logger->logThreadTerminated(ExecReduce);
       // cout << "reduce thread treminated :  " << i << endl;
        test = pthread_join(reduce_threads[i] , NULL);
        if (test == FAIL){
            exceptionCaller("Failed to join threads");
        }
    }

    for (auto iter = thread_mutex_reduce.begin(); iter != thread_mutex_reduce.end(); ++iter)
    {
        test = pthread_mutex_destroy(&(iter->second));
        if (test == FAIL){
            exceptionCaller("Failed to destroy threads");
        }
    }

    for (auto list = thread_list_reduce.begin(); list != thread_list_reduce.end(); ++list)
    {
        reduced_list reduced = *list->second;
        if(autoDeleteV2K2)
        {
            for( auto toDelete=reduced.begin(); toDelete != reduced.end(); ++toDelete){
                toDelete->first = nullptr;
                toDelete->second = nullptr;
            }
        }
    }
   // cout << "out_items_vec.size:  " << (*out_items_vec).size() << endl;

    std::sort(out_items_vec->begin(), out_items_vec->end(), compk3());
    //============================================================================
    // log info ==================================================================
    //============================================================================
    logger->endTimeReduce();
    logger->logReduceAndOutputTime();
    logger->logFinished();
    frameworkDistraction();
    return *out_items_vec;
}

void Emit2 (k2Base* key, v2Base* val)
{
    pair<k2Base*, v2Base*> pair = std::make_pair(key, val);
    pthread_mutex_lock(&thread_mutex_map[pthread_self()]);
    (*thread_list_map[pthread_self()]).push_back(pair);
    pthread_mutex_unlock(&thread_mutex_map[pthread_self()]);
    if (sem_post(&sem) == FAIL){
        exceptionCaller(" sem Post fail");
    }
}

void Emit3 (k3Base* key, v3Base* val)
{
    (*thread_list_reduce[(pthread_self())]).push_back(std::make_pair(key, val));
}
