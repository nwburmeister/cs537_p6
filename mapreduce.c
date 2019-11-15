// FILE FOR IMPLEMENTING FRAMEWORK AS
// DEFINED IN MAPREDUCE.H

#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

Mapper map;
Reducer reducer;
struct partition_info *partitions;
int NEXT_PARTITION;
int NUM_PARTITIONS;
pthread_mutex_t fileLock;

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
} PARTITION_INFO;



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
    // QUESTION: is partition_number the same as the hash partition??
    
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
    long voodoo;

    voodoo = strtoul(fourChars, &ptr, 16);
    return voodoo;   
}

// sort each partition in alphabetical order
void sort(int partition_number) {


    // new linked list that will be sorted
    struct partition_info *new_sorted_list;
    // current partition we are trying to sort
    struct key_value_mapper *iterator = partitions[partition_number].head;
    

    while(iterator != NULL) {
        
        struct key_value_mapper *temp = iterator;
        
        if(new_sorted_list->head == NULL) {
            
            new_sorted_list->head = temp;
        
        } else {
            // get 32bit unsigned number to sort on
            unsigned long voodoo_to_insert = MR_SortedPartition(temp->key, NUM_PARTITIONS);
            // get head of partition
            struct key_value_mapper *new_list_iterator = new_sorted_list->head;
            // init prev node
            struct key_value_mapper *prev_node = NULL;
            while(new_list_iterator != NULL) {
                unsigned long curr_integer = MR_SortedPartition(new_list_iterator->key, NUM_PARTITIONS);
                if(voodoo_to_insert <= curr_integer) {
                    if(prev_node != NULL) {
                        prev_node->next = temp;
                        temp->next = new_list_iterator;
                        break;
                    } else {
                        temp->next = new_list_iterator;
                        new_sorted_list->head = temp;
                        break;
                    }
                }
                prev_node = new_list_iterator;
                new_list_iterator = new_list_iterator->next;
            }
            // if at end of list, insert node
            prev_node->next = temp;
            temp->next = NULL; 
        }
        iterator = iterator->next;
    }
}


// FUNCTION FOR INSERTING A NEW KEY, VALUE 
// AT THE BEGINNING OF THE LINKED LIST
void MR_Emit(char *key, char *value)
{   
    // loop through unsorted linked list
    
    int hashIndex = MR_DefaultHashPartition(key, NUM_PARTITIONS); 
    struct key_value_mapper *curr_partition =  partitions[hashIndex].head;
    struct key_value_mapper *new = malloc(sizeof(struct key_value_mapper));
    if(curr_partition == NULL){
        new->key = key;
        new->val = value;
        new->processed = 0;
        new->next = NULL;
        partitions[hashIndex].head = new;
    } else {
        new->key = key;
        new->val = value;
        new->processed = 0;
        new->next = partitions[hashIndex].head;
        partitions[hashIndex].head = new;
    }
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
    partitions = calloc(num_partitions, sizeof(PARTITION_INFO));
    
    for (int i = 1; i < argc; i++)
    {
        map(argv[i]);
        // printf("%s\n", argv[i]);
    }

    
    for (int i = 0; i < num_partitions; i++){
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



