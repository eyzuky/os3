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
//===========================
//DEFINES

#define BULK 10


using namespace std;

//===============================
//TYPEDEFS

typedef pair<k2Base,v2Base> map_pair;
typedef list<map_pair> map_pair_list;
typedef pair<k2Base, list<v2Base>> shuffled_pair;
typedef list<shuffled_pair> shuffled_list;
typedef pair<k3Base, v3Base> reduced_pair;
typedef list<reduced_pair> reduced_list;
typedef pair<pthread_t, map_pair_list> thread_and_list;
typedef list<thread_and_list> threads_and_their_list;

//===============================
//GLOBALS AND MUTEX
pthread_mutex_t map_mutex;
pthread_mutex_t index_mutex;
threads_and_their_list thread_map_list;




//=============dast class======

class mapDataHandler {

    private:
    unsigned int _bulkIndex;
    const IN_ITEMS_VEC _items;
    const MapReduceBase& _mapReduceBase;

    public:
    mapDataHandler(IN_ITEMS_VEC &items_vec, const MapReduceBase& mapReduceBase): _items(items_vec), _mapReduceBase(mapReduceBase){
                _bulkIndex = 0;
   }

    unsigned int nextBulkIndex() const { return _bulkIndex;}
    unsigned int proceedToNextBulk()
    {
        this->_bulkIndex = BULK + _bulkIndex;
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
    void applyReduce(const k2Base *const keyOne, const shuffled_list *const _items)
    {
    //    this->_mapReduceBase.Reduce(keyOne,valueOne);
    }
//    IN_ITEM getItem(unsigned int index){
//        return this->_items[index];
//    }
};


//void * frameworkInitialization(){
//    //todo initialize all the global variables
//}
//
void * mapExec(void * data){

    return nullptr;
}
//
//
//void * shuffle(){
//
//}
//
//
//void * joinQueues(){
//
//}
//
//
//void * reduceExec(){
//
//}



OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2)
{
   // frameworkInitialization();
    MapReduceLogger *logger = new MapReduceLogger();
    logger->logInitOfFramework(multiThreadLevel);
    logger->startTimeMap();
    //=====================
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
        thread_and_list new_pair = make_pair(map_threads[i], list_for_thread);
        thread_map_list.push_back(new_pair);
    }
    
    
    //======================
    logger->endTimeMap();
    logger->logMapAndShuffleTime();
    logger->startTimeReduce();
    //======================
    //reduce and output
    OUT_ITEMS_VEC out_items_vec;

    
    
    
    //======================
    logger->endTimeReduce();
    logger->logReduceAndOutputTime();
    logger->logFinished();
    return out_items_vec;
}



void Emit2 (k2Base*, v2Base*)
{
    
}

void Emit3 (k3Base*, v3Base*)
{
    
}
