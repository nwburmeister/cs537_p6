// FILE FOR IMPLEMENTING FRAMEWORK AS
// DEFINED IN MAPREDUCE.H

#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

struct arg_struct {
    char *key;
    Getter get_next;
    int partition_number;
};

struct key_value_mapper {
    int processed;
    char *key;
    char *val;
    struct key_value_mapper *next;
};

typedef struct partition_info {
    struct key_value_mapper *head;
    pthread_mutex_t lock;
} PARTITION_INFO;

Partitioner partitioner;
Reducer reducer;
Mapper map;

int *isNextKeyDifferent;
int NUM_FILES;
int NEXT_PARTITION;
int NUM_PARTITIONS;
int CURR_FILE;

char** FILES;
pthread_mutex_t fileLock;
struct partition_info *partitions;


unsigned long MR_DefaultHashPartition(char *key, int num_partitions) 
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

// TODO DO SOMETHING WITH THIS
unsigned long MR_SortedPartition(char *key, int num_partitions)
{

    char fourChars[5];
    strncpy(fourChars, key, 4);

    char *ptr;
    unsigned long voodoo;

    voodoo = strtoul(fourChars, &ptr, 36);
    //printf("%s %ld   \n ", fourChars, voodoo);
    return voodoo;
}

// returns a pointer to the iterator's next value
char* get_next(char *key, int partition_number)
{

    struct key_value_mapper *curr_partition =  partitions[partition_number].head;
    // need to check if next value is different
    if (isNextKeyDifferent[partition_number] == 1){
        // reset key to 0
        isNextKeyDifferent[partition_number] = 0;
        return NULL;
    }

    while (curr_partition != NULL){
        if (strcmp(curr_partition->key, key) == 0) {
            partitions[partition_number].head = curr_partition->next;
            if (curr_partition->next != NULL) {
                // if not 0, we have a new value
                if (strcmp(curr_partition->next->key, key) != 0){
                    // set flag to 1
                    isNextKeyDifferent[partition_number] = 1;
                    return curr_partition->next->val;
                }
                return curr_partition->val;

            }else {
                return curr_partition->val;
            }
        }
        curr_partition = curr_partition->next;
    }
    // returns NULL if for some reason the key is not found in the partition
    return NULL;
}


// FUNCTION FOR INSERTING A NEW KEY, VALUE 
// AT THE BEGINNING OF THE LINKED LIST
void MR_Emit(char *key, char *value)
{   
    if (strlen(key) == 0) {
        return;
    }

    int hashIndex = partitioner(key, NUM_PARTITIONS);
    struct key_value_mapper *curr_partition =  partitions[hashIndex].head;
    struct key_value_mapper *new = malloc(sizeof(struct key_value_mapper));
    new->key = malloc(sizeof(char)*(strlen(key) + 1));
    strcpy(new->key, key);
    new->val = value;
    new->processed = 0;

    struct key_value_mapper *iterator = partitions[hashIndex].head;
    
    if (iterator == NULL){
        partitions[hashIndex].head = new;
        new->next = NULL;
        return;
    }

    // ACQUIRE THE LOCK
    pthread_mutex_lock(&curr_partition[hashIndex].lock);
    struct key_value_mapper *prev = NULL;
    while(iterator != NULL) {    
        if(strcmp(iterator->key, key) > 0) {
            if (prev == NULL){
                new->next = iterator;
                partitions[hashIndex].head = new;
                return;
            } else {
                prev->next = new;
                new->next = iterator;
                return;
            }
             
        }    
        prev = iterator;
        iterator = iterator->next;
    }
    prev->next = new;
    new->next = NULL;
    // RELEASE THE LOCK
    pthread_mutex_unlock(&curr_partition[hashIndex].lock);
    
    //printf("%s Partition Index: %d\n", partitions[hashIndex].head->key, hashIndex);
    
} 


 void Reduce_Thread_Helper_Func()
 {

     // need to sort partition
     // keep track of next partition to sort

     while(1) {

         if (NEXT_PARTITION > num_partitions){
             break;
         }

         struct key_value_mapper *iterator =  partitions[NEXT_PARTITION].head;
         sort(NEXT_PARTITION);
         NEXT_PARTITION++;

         while(iterator != NULL)
         {
             reduce(iterator->key, get_next, i);
             iterator = iterator->next;
         }
     }

     // todo free data
 }


 void* Map_Threads_Helper_Func(){
     while (1)
     {
         char* curr_filename;
         pthread_mutex_lock(&fileLock);
         if(NUM_FILES <= CURR_FILE){
             pthread_mutex_unlock(&fileLock);
             return NULL;
         }
         curr_filename = file_names[CURR_FILE];
         CURR_FILE++;
         pthread_mutex_unlock(&fileLock);
         map(curr_filename);
     }
 }


void MR_Run(int argc, char *argv[], Mapper map,
            int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition,
            int num_partitions)
{    
    // INITIALIZATION ***********************************************


    partitioner = partition;
    NUM_PARTITIONS = num_partitions;
    NUM_FILES = argc;
    map = map;
    reduce = reduce;
    partitions = malloc((num_partitions+1) * sizeof(struct partition_info));
    isNextKeyDifferent = calloc(num_partitions, sizeof(int) );
    FILES = argv[1]


    // END INITIALIZATION *******************************************

    // THREADS FOR MAPPERS
     pthread_t mappers[num_mappers];
     for (int i = 0; i < num_mappers; i++)
     {
         if (i+1 < argc) {
             pthread_create(&mappers[i], NULL, Map_Threads_Helper_Func, NULL);
         }
     }

     // JOIN MAPPERS
     for (int i = 0; i < num_mappers; i++)
     {
         pthread_join(mappers[i], NULL);
     }

     // THREADS FOR REDUCERS
//     pthread_t reducers[num_reducers];
//
//     for (int i = 0; i < num_reducers; i++)
//     {
//         pthread_create(map(&mappers[i], NULL, reduce, (void *)&args);
//     }
//
//     for (int i = 0; i < num_reducers; i++)
//     {
//
//         pthread_join(map(&mappers[i] argv[i]));
//     }

}



//
//    for (int i = 0; i < NUM_PARTITIONS; i++){
//        struct key_value_mapper *iterator =  partitions[i].head;
//        while(iterator != NULL)
//        {
//            reduce(iterator->key, get_next, i);
//            iterator = partitions[i].head;
//        }
//    }