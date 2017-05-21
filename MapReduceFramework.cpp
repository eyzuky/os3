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
#include "MapReduceLogger.hpp"
#include <map>
#include <vector>
#include <semaphore.h>
#include <iostream>
#include <algorithm>
//===========================
//DEFINES

#define BULK 10
#define FAIL -1

using namespace std;

//===============================
//TYPEDEFS

typedef pair<k2Base*,v2Base*> map_pair;             // INTERMEDIATE_ITEM;
typedef vector<map_pair> map_pair_list;             // INTERMEDIATE_ITEMS_LIST
typedef pair<k2Base*, V2_VEC> shuffled_pair; // REDUCE_ITEM
typedef vector<shuffled_pair> shuffled_list;        // REDUCE_ITEMS_LIST
typedef OUT_ITEM reduced_pair;        // OUT_ITEM
typedef vector<reduced_pair> reduced_list;          // OUT_ITEMS_QUEUE
typedef pair<pthread_t, map_pair_list> thread_and_list; // threadsOutput
typedef vector<thread_and_list> threads_and_their_list; //
//===============================
//GLOBALS AND MUTEX
sem_t sem;
pthread_mutex_t map_mutex;
pthread_mutex_t index_mutex;
pthread_mutex_t reduce_mutex;
pthread_mutex_t fakeMutex;
map <pthread_t, pthread_mutex_t> thread_mutex_map; //threadsItemsListsMutex
map <pthread_t, map_pair_list> thread_list_map; //threadsItemsListsTemp
map<pthread_t, reduced_list> thread_list_reduce;
shuffled_list shuffled;

pthread_cond_t exec_shuffle_notification;
MapReduceLogger *logger = new MapReduceLogger();
static void exceptionCaller(string exc){
    cerr << exc;
    exit(FAIL);
}

bool mapPartOver;
//=============dast class======
//notice that I almost remade this class, there were tons of conceptual errors (e.g trying to set a const not in the initialization list,
// or wrong names of variables and stuff. see git commits to see the differences
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
    int getCurrentIndex()
    {
        return this->_bulkIndex;
    }
    unsigned int getSize() {
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
    shuffled_list items;
    reduceDataHandler(shuffled_list &items_vec, const MapReduceBase& mapReduceBase): items(items_vec), _mapReduceBase(mapReduceBase){
        _bulkIndex = 0;
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
    int getCurrentIndex()
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
    mapPartOver = false;
    if(sem_init(&sem, 0 ,0) == FAIL){
        exceptionCaller(" Sem init Fail");
    }
    // mutex init
    pthread_mutex_init(&fakeMutex, NULL);
    pthread_mutex_init(&reduce_mutex, NULL);
    pthread_mutex_init(&map_mutex, NULL);
    pthread_mutex_init(&index_mutex, NULL);
    //todo initialize all the global variables

    return nullptr;
}

void * frameworkDistraction(){
    mapPartOver = false;

    // mutex destroy
    pthread_mutex_destroy(&fakeMutex);
    pthread_mutex_destroy(&reduce_mutex);
    pthread_mutex_destroy(&map_mutex);
    pthread_mutex_destroy(&index_mutex);
    //todo initialize all the global variables
    //globals clear
    sem_destroy(&sem);
    thread_mutex_map.clear();
    thread_list_map.clear(); //threadsItemsListsTemp
    thread_list_reduce.clear();
    shuffled.clear();

    return nullptr;
}

void * mapExec(void * data){
    int test; //test will check for errors and exceptions
    logger->logThreadCreated(ExecMap);
    int curIndex = 0;
    IN_ITEM item;
    mapDataHandler * handler = (mapDataHandler*) data;

    while (handler->getCurrentIndex() < handler->getSize()) {
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
        for (int i = curIndex; i < curIndex + BULK; ++i)
        {
            if(i >= handler->getSize())
            {
                break;
            }
            item = handler->getItem(i);
            handler->applyMap(item.first, item.second);
        }
        test = pthread_mutex_lock(&thread_mutex_map[pthread_self()]);
        if (test == FAIL){
            exceptionCaller("Failed to lock mutex");
        }
        // todo uniting all the queues...
        map_pair itemToAdd;
        map_pair_list& queueToCopy = thread_list_map[pthread_self()];
        // map_pair_list& destQueue = threadsItemsLists[pthread_self()]; // todo check threadsItemsLists right palce?

        while (!queueToCopy.empty())
        {
            itemToAdd = queueToCopy.front();
            //     queueToCopy.pop();
            //    destQueue.push(itemToAdd);
        }

        test = pthread_mutex_unlock(&thread_mutex_map[pthread_self()]);
        if (test == FAIL){
            exceptionCaller("Failed to unlock mutex");
        }
    }
    test = pthread_cond_signal(&exec_shuffle_notification);
    if (test == FAIL){
        exceptionCaller("Failed to send signal");
    }
    logger->logThreadTerminated(ExecMap);


    return nullptr;
}


void * joinQueues() {
    for (auto it = thread_list_map.begin(); it != thread_list_map.end(); ++it)
    {
        map_pair_list list = it->second;
        pthread_mutex_lock(&thread_mutex_map[it->first]);
        for(auto pair = list.begin(); pair != list.end(); ++pair)
        {
            if(std::find(shuffled.begin(), shuffled.end(), pair->first) == (shuffled.end()->first)) {
                V2_VEC newVec;
                newVec.insert(newVec.begin(), pair->second);
                shuffled_pair item = make_pair(pair->first, newVec);
                shuffled.insert(shuffled.end(), item);
            } else {
                shuffled_pair * iter = std::find(shuffled.begin(), shuffled.end(), pair->first);
                iter->second.insert(iter->second.begin(), pair->second);
            }
        }
        pthread_mutex_unlock(&thread_mutex_map[it->first]);
    }
    return nullptr;
}

void * shuffle(void * data){
    logger->logThreadCreated(Shuffle);


    pthread_mutex_lock(&map_mutex);

    pthread_mutex_unlock(&map_mutex);

    int retVal;

    while (!mapPartOver)
    {
        if (sem_wait(&sem) == FAIL){
            exceptionCaller("sem wait failure");
        }
        joinQueues();

        // todo?
        pthread_mutex_lock(&fakeMutex);


       // pthread_mutex_unlock(&fakeMutex);

    }
    joinQueues();

    //   pthread_mutex_lock(&logMutex);
    logger->logThreadTerminated(Shuffle);

    //   pthread_mutex_unlock(&logMutex);
//    //todo checkIfWriteSucceed(); - do we need this?

    return nullptr;
}


void * reduceExec(void * data){
    int test;

    logger->logThreadCreated(ExecReduce);
    int curIndex = 0;
    // shuffled_pair pair;
    reduceDataHandler *handler = (reduceDataHandler*)data;
    while ( handler->getCurrentIndex() < handler->getSize()) {
        test = pthread_mutex_lock(&index_mutex);
        if (test == FAIL){
            exceptionCaller("Failed to lock mutex");
        }
        curIndex = handler->getCurrentIndex();
        handler->proceedToNextBulk();
        for(int i = 0; i < BULK; i++){
            if(i >= handler->getSize())
            {
                break;
            }
            shuffled_pair pair = handler->getItem(i);
            handler->applyReduce(pair.first, pair.second);
        }
    }

    return nullptr;
}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2)
{
    frameworkInitialization();
    int test;
    //===========================================================================
    // log info
    //===========================================================================
    logger->logInitOfFramework(multiThreadLevel);
    logger->startTimeMap();
    //===========================================================================
    // map and shuffle

    mapDataHandler map_handler = mapDataHandler(itemsVec, mapReduce);

    pthread_t *map_threads = new pthread_t[multiThreadLevel];

    pthread_t shuffle_thread;

    test = pthread_mutex_lock(&map_mutex);
    if (test == FAIL){
        exceptionCaller("Failed to lock mutex");
    }
    //this loop creates a thread, a list for a thread and adds it to our threads-list list.
    for (int i = 0; i < multiThreadLevel; i++)
    {
        test = pthread_create(&map_threads[i], NULL, mapExec, &map_handler);
        if (test == FAIL){
            exceptionCaller("Failed to create thread");
        }
        map_pair_list list_for_thread;
        thread_list_map[map_threads[i]] = list_for_thread;
        thread_mutex_map[map_threads[i]] = PTHREAD_MUTEX_INITIALIZER;
    }
    test = pthread_mutex_unlock(&map_mutex);
    if (test == FAIL){
        exceptionCaller("Failed to unlock mutex");
    }
    test = pthread_create(&shuffle_thread, NULL, shuffle, nullptr);
    if (test == FAIL){
        exceptionCaller("Failed to create shuffle thread");
    }
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        test = pthread_join(map_threads[i], NULL);
        if (test == FAIL){
            exceptionCaller("Failed to join threads");
        }
    }
    if(sem_post( &sem) == FAIL){
        exceptionCaller("sem post fail");
    }
    mapPartOver = true;

    test = pthread_join(shuffle_thread, NULL);
    if (test == FAIL){
        exceptionCaller("Failed to join threads");
    }
    for (auto iter = thread_mutex_map.begin(); iter != thread_mutex_map.end(); ++iter)
    {
        test = pthread_mutex_destroy(&(iter->second));
        if (test == FAIL){
            exceptionCaller("Failed to destroy threads");
        }
    }
    delete [] map_threads;
    //===========================================================================
    // log info
    //============================================================================
    logger->endTimeMap();
    logger->logMapAndShuffleTime();
    logger->startTimeReduce();

    //============================================================================
    //reduce and output
    reduceDataHandler reduce_handler = reduceDataHandler(shuffled, mapReduce);
    pthread_t *reduce_threads = new pthread_t[multiThreadLevel];
    test = pthread_mutex_lock(&reduce_mutex);
    if (test == FAIL){
        exceptionCaller("Failed to lock mutex");
    }
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        test = pthread_create(&reduce_threads[i], NULL, reduceExec, &reduce_handler);
        if (test == FAIL){
            exceptionCaller("Failed to create thread");
        }
        reduced_list reduced;
        thread_list_reduce[reduce_threads[i]] = reduced;

    }
    test = pthread_mutex_unlock(&reduce_mutex);
    if (test == FAIL){
        exceptionCaller("Failed to unlock mutex");
    }
    OUT_ITEMS_VEC out_items_vec;
    for (auto list = thread_list_reduce.begin(); list != thread_list_reduce.end(); ++list)
    {
        reduced_list reduced = list->second;
        out_items_vec.insert(out_items_vec.end(), reduced.begin(), reduced.end());
    }
    sort(out_items_vec.begin(), out_items_vec.end());
    //============================================================================
    // log info
    //============================================================================
    logger->endTimeReduce();
    logger->logReduceAndOutputTime();
    logger->logFinished();
    return out_items_vec;
}
void Emit2 (k2Base* key, v2Base* val) {
    if (sem_post(&sem) == FAIL){
        exceptionCaller(" sem Post fail");
    }

    thread_list_map[pthread_self()].insert(thread_list_map[pthread_self()].end(), std::make_pair(key, val));

}

void Emit3 (k3Base* key, v3Base* val)
{
    thread_list_reduce[pthread_self()].push_back(std::make_pair(key, val));
}
