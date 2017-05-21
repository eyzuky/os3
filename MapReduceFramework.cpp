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
//===========================
//DEFINES

#define BULK 10


using namespace std;

//===============================
//TYPEDEFS

typedef pair<k2Base,v2Base> map_pair;             // INTERMEDIATE_ITEM;
typedef vector<map_pair> map_pair_list;             // INTERMEDIATE_ITEMS_LIST
typedef pair<k2Base, list<v2Base>> shuffled_pair; // REDUCE_ITEM
typedef vector<shuffled_pair> shuffled_list;        // REDUCE_ITEMS_LIST
typedef pair<k3Base, v3Base> reduced_pair;        // OUT_ITEM
typedef vector<reduced_pair> reduced_list;          // OUT_ITEMS_QUEUE
typedef pair<pthread_t, map_pair_list> thread_and_list; // threadsOutput
typedef vector<thread_and_list> threads_and_their_list; //
//===============================
//GLOBALS AND MUTEX
pthread_mutex_t map_mutex;
pthread_mutex_t index_mutex;
map <pthread_t, pthread_mutex_t> thread_mutex_map; //threadsItemsListsMutex
map <pthread_t, map_pair_list> thread_list_map;
map <pthread_t, map_pair_list> thread_list_reduce;

pthread_cond_t exec_shuffle_notification;
shuffled_list shuffled;
MapReduceLogger *logger = new MapReduceLogger();

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
    const shuffled_list _items;
    const MapReduceBase& _mapReduceBase;

public:
    reduceDataHandler(shuffled_list &items_vec, const MapReduceBase& mapReduceBase): _items(items_vec), _mapReduceBase(mapReduceBase){
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
        return this->_items.size();
    }
    int getCurrentIndex()
    {
        return this->_bulkIndex;
    }
    void applyReduce(const k2Base *const keyOne, const shuffled_list *const _items)
    {
    //    this->_mapReduceBase.Reduce(keyOne,valueOne);
    }
    shuffled_pair getItem(unsigned int index){
        return this->_items[index];
    }  //TODO - something here doesn't work -  i think its ok now :)
    //Works now because a list in c++ is a linked list and you cannot access it with index.. so it is a vector now
};


void * frameworkInitialization(){
    mapPartOver = false;
    //todo initialize all the global variables
    
    return nullptr;
}

void * mapExec(void * data){

    logger->logThreadCreated(ExecMap);
    int curIndex = 0;
    IN_ITEM item;
    mapDataHandler * handler = (mapDataHandler*) data;

    while (handler->getCurrentIndex() < handler->getSize()) {
        pthread_mutex_lock(&index_mutex);
        curIndex = handler->getCurrentIndex();
        handler->proceedToNextBulk();
        pthread_mutex_unlock(&index_mutex);
        for (int i = curIndex; i < curIndex + BULK; ++i)
        {
            if(i >= handler->getSize())
            {
                break;
            }
            item = handler->getItem(i);
            handler->applyMap(item.first, item.second);
        }
        pthread_mutex_lock(&thread_mutex_map[pthread_self()]);
        
        // todo uniting all the queues...
        map_pair itemToAdd;
        map_pair_list& queueToCopy = thread_list_map[pthread_self()];
        map_pair_list& destQueue = threadsItemsLists[pthread_self()];

        while (!queueToCopy.empty())
        {
            itemToAdd = queueToCopy.front();
            queueToCopy.pop();
            destQueue.push(itemToAdd);
        }

        pthread_mutex_unlock(&thread_mutex_map[pthread_self()]);
    }
    pthread_cond_signal(&exec_shuffle_notification);
    logger->logThreadTerminated(ExecMap);

    
    return nullptr;
}

void * shuffle(void * data){
    logger->logThreadCreated(Shuffle);

    pthread_mutex_lock(&mapMutex); // todo mapmutex or indexMutex
    pthread_mutex_unlock(&mapMutex);
//    checkIfWriteSucceed(); // todo do we need this?

    int retVal;

    while (!mapPartOver)
    {
        timeToWait = getTimeToWait();

        pthread_mutex_lock(&fakeMutex);
        retVal = pthread_cond_timedwait(&shufflerCV, &fakeMutex, &timeToWait);

        if (retVal != 0 && retVal != ETIMEDOUT)
        {
            exitIfFail(1, COND_TIMEDWAIT);
        }
        pthread_mutex_unlock(&fakeMutex);

        joinQueues();
    }
    joinQueues();

    pthread_mutex_lock(&logMutex);
    logger->logThreadTerminated(Shuffle);

    pthread_mutex_unlock(&logMutex);
//    //todo checkIfWriteSucceed(); - do we need this?

    return nullptr;
}


void * joinQueues(void * data){
    map_pair itemToAdd;
    for (auto it = threadsItemsLists.begin();
         it != threadsItemsLists.end();
         ++it){
        while (!it->second.empty()){
            pthread_mutex_lock(&threadsItemsListsMutex[it->first]);
            itemToAdd = it->second.front();
            it->second.pop();
            if (shuffled.count(itemToAdd.first) == 0)
            {
                shuffled[itemToAdd.first] = std::list<v2Base *>(1, itemToAdd.second);
            }
            else    // shufflerMap has that key.
            {
                shuffled[itemToAdd.first].push_back(itemToAdd.second);
            }
        }
        pthread_mutex_unlock(&threadsItemsListsMutex[it->first]);
    }
    return nullptr;
}


void * reduceExec(void * data){
    
    
    logger->logThreadCreated(ExecReduce);
    int curIndex = 0;
    shuffled_pair pair;
    reduceDataHandler *handler = (reduceDataHandler*)data;
    while ( handler->getCurrentIndex() < handler->getSize()) {
        pthread_mutex_lock(&index_mutex);
        curIndex = handler->getCurrentIndex();
        handler->proceedToNextBulk();
        for(int i = 0; i < BULK; i++){
            if(i >= handler->getSize())
            {
                break;
            }
            pair = handler->getItem(i);
            handler->applyReduce(pair.first, pair.second);
        }
    }
    
    return nullptr;
}



OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2)
{
    frameworkInitialization();

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
    
    pthread_mutex_lock(&map_mutex);
    //this loop creates a thread, a list for a thread and adds it to our threads-list list.
    for (int i = 0; i < multiThreadLevel; i++)
    {
        pthread_create(&map_threads[i], NULL, mapExec, &map_handler);
        map_pair_list list_for_thread;
        thread_list_map[map_threads[i]] = list_for_thread;
        thread_mutex_map[map_threads[i]] = PTHREAD_MUTEX_INITIALIZER;
    }
    pthread_mutex_unlock(&map_mutex);
    pthread_create(&shuffle_thread, NULL, shuffle, nullptr);
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_join(map_threads[i], NULL);
    }
    mapPartOver = true;

    pthread_join(shuffle_thread, NULL);
    for (auto iter = thread_mutex_map.begin(); iter != thread_mutex_map.end(); ++iter)
    {
        pthread_mutex_destroy(&(iter->second));
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
    
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_create(&reduce_threads[i], NULL, reduceExec, &reduce_handler);
    }

    
    OUT_ITEMS_VEC out_items_vec;
    //============================================================================
    // log info
    //============================================================================
    logger->endTimeReduce();
    logger->logReduceAndOutputTime();
    logger->logFinished();
    return out_items_vec;
}



void Emit2 (k2Base*, v2Base*)
{
    thread_list_map[pthread_self()].push(std::make_pair(key, val));
}

void Emit3 (k3Base*, v3Base*)
{
    thread_list_reduce[pthread_self()].push_back(std::make_pair(key, val));
}
