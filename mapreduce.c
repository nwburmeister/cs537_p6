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
    unsigned long hash;
    char *key;
    char *val;
    struct key_value_mapper *next;
};

typedef struct partition_info {
    struct key_value_mapper *head;
} PARTITION_INFO;

Mapper map;
Reducer reducer;

int NEXT_PARTITION;
int NUM_PARTITIONS;
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

// returns a pointer to the iterator's next value
char* get_next(char *key, int partition_number)
{

    struct key_value_mapper *curr_partition =  partitions[partition_number].head;
    
    if (curr_partition == NULL) {
        return NULL;
    }

    while(curr_partition != NULL) {
        // find the key and return the next value
        if(curr_partition->key == key && curr_partition->processed == 0) {
            curr_partition->processed = 1;
            if (curr_partition->next == NULL){
                return NULL;
            }
            return curr_partition->next->val;
        }
        curr_partition = curr_partition->next;
    }
    
    // returns NULL if for some reason the key is not found in the partition
    return NULL;
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


// FUNCTION FOR INSERTING A NEW KEY, VALUE 
// AT THE BEGINNING OF THE LINKED LIST
void MR_Emit(char *key, char *value)
{   
    if (strlen(key) == 0){
        return;
    }

    int hashIndex = MR_DefaultHashPartition(key, NUM_PARTITIONS); 
    struct key_value_mapper *curr_partition =  partitions[hashIndex].head;
    struct key_value_mapper *new = malloc(sizeof(struct key_value_mapper));
    new->key = malloc(sizeof(char)*(strlen(key) + 1));


    strcpy(new->key, key);
    new->hash = MR_SortedPartition(key, NUM_PARTITIONS);
    new->val = value;
    new->processed = 0;


    // if(curr_partition == NULL){
    //     strcpy(new->key, key);
    //     new->hash = MR_SortedPartition(key, NUM_PARTITIONS);
    //     new->val = value;
    //     new->processed = 0;
        
    //     //new->next = NULL;
    //     //partitions[hashIndex].head = new;
    // } else {
    //     strcpy(new->key, key);
    //     new->hash = MR_SortedPartition(key, NUM_PARTITIONS);
    //     new->val = value;
    //     new->processed = 0;
        
    //     //new->next = partitions[hashIndex].head;
    //     //partitions[hashIndex].head = new;
    // }

    // printf("%s\n", "hello");
    struct key_value_mapper *iterator = partitions[hashIndex].head;
    
    if (iterator == NULL){
        partitions[hashIndex].head = new;
        new->next = NULL;
        return;
    }
    
    struct key_value_mapper *prev = NULL;
    while(iterator != NULL) {    
        if(iterator->hash >= new->hash) {
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
    
    //printf("%s Partition Index: %d\n", partitions[hashIndex].head->key, hashIndex);
    
} 


// void Reduce_Thread_Helper_Func()
// {

//     // need to sort partition
//     // keep track of next partition to sort

//     while(1) {

//         if (NEXT_PARTITION > num_partitions){
//             break;
//         }

//         struct key_value_mapper *iterator =  partitions[NEXT_PARTITION].head;
//         sort(NEXT_PARTITION);
//         NEXT_PARTITION++;

//         while(iterator != NULL)
//         {
//             reduce(iterator->key, get_next, i);
//             iterator = iterator->next;
//         }
//     }

//     // todo free data
// }


// void* Map_Thread_Helper_Func(void* arg){
//     while (1) {
//         char* filename;
//         pthread_mutex_lock(&fileLock);
//         if(counter >= NUM_FILES){
//             pthread_mutex_unlock(&fileLock);
//             return NULL;
//         }
//         filename = file_names[counter++];
//         pthread_mutex_unlock(&fileLock);
//         map(filename);
//     }
// }


void MR_Run(int argc, char *argv[], Mapper map,
            int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition,
            int num_partitions)
{    
    // do some checks on input arguments

    NUM_PARTITIONS = num_partitions;

    map = map;
    reduce = reduce;
    partitions = malloc((num_partitions+1) * sizeof(struct partition_info));

    for(int i = 0; i < NUM_PARTITIONS; i++){
        //printf("%p\n", partitions[i].head);
    } 

    for (int i = 1; i < argc; i++)
    {
        map(argv[i]);
        // printf("%s\n", argv[i]);
    }   

    
    for (int i = 0; i < NUM_PARTITIONS; i++){
        struct key_value_mapper *iterator =  partitions[i].head;
        while(iterator != NULL)
        {
            reduce(iterator->key, get_next, i);   
            iterator = iterator->next;
        }
    }


    // call sorted partition
    
    // struct key_value_mapper *iterator = head;
    // while(iterator != NULL)
    // {
    //     reduce(iterator->key, get_next, MR_DefaultHashPartition(iterator->key, num_partitions));
    //     iterator = iterator->next;
    // }

    

    // pthread_t mappers[num_mappers];
    // for (int i = 0; i < num_mappers; i++)
    // {
    //     if (i+1 < argc) {
    //         pthread_create(&mappers[i], NULL, map, argv[i]));
    //     }
        
    // }

    // // thread join

    // for (int i = 0; i < num_mappers; i++)
    // {
    //     pthread_join(&mappers[i], NULL);
    // }

    // // now do reducers
    // pthread_t reducers[num_reducers];

    // for (int i = 0; i < num_reducers; i++)
    // {
    //     // todo get values
    //     struct arg_struct args;
    //     args.key = 
    //     args.get_next =
    //     args.partition_number = 
        
    //     pthread_create(map(&mappers[i], NULL, reduce, (void *)&args);
    // }

    // for (int i = 0; i < num_reducers; i++)
    // {

    //     pthread_join(map(&mappers[i] argv[i]));
    // }

}



