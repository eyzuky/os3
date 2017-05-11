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
using namespace std;

//===============================
//TYPEDEFS

typedef pair<k2Base,v2Base> map_pair;
typedef list<map_pair> map_pair_list;
typedef  pair<k2Base, list<v2Base>> shuffled_pair;
typedef list<shuffled_pair> shuffled_list;
typedef pair<k3Base, v3Base> reduced_pair;
typedef list<reduced_pair> reduced_list;


//===============================
//GLOBALS






//=============dast class======
#define BULK 10

class mapDataHandler {

    private:
    unsigned int _bulkIndex;
    const


    public:
    dataHandler(IN_ITEMSstst_VEC& items_vec, const MapReduceBase& mapReduceBase){
                _bulkIndex = 0;
                _items = items_vec;
                _mapReduceBase = mapReduceBase;
                // todo not sure about this
   }

    unsigned int nextBulkIndex() const { return _bulkIndex;}
    unsigned int proceedToNextBulk() { this->_bulkIndex = BULK + _bulkIndex;}
    unsigned int getSize() { this->_items.size();}

};


class reduceDataHandler {

private:
    unsigned int _bulkIndex;
    const IN_ITEMS_VEC _items;
    const MapReduceBase& mapReduceBase;


public:
    dataHandler(IN_ITEMS_VEC &items_vec, const MapReduceBase& mapReduceBase){
        _bulkIndex = 0;
        _items = items_vec;
        _mapReduceBase = mapReduceBase;
        // todo not sure about this
    }

    unsigned int nextBulkIndex() const { return _bulkIndex;}
    unsigned int proceedToNextBulk() { this->_bulkIndex = BULK + _bulkIndex;}

};

void * mapExec(){

}










OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2)
{
    
    
    
    
    pthread_t shuffle_thread;
    pthread_t *map_threads = new pthread_t[multiThreadLevel];
    
    
    
    OUT_ITEMS_VEC out_items_vec;
    return out_items_vec;
}



void Emit2 (k2Base*, v2Base*)
{
    
}
void Emit3 (k3Base*, v3Base*)
{
    
}
