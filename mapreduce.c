// FILE FOR IMPLEMENTING FRAMEWORK AS
// DEFINED IN MAPREDUCE.H

#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct arg_struct {
    char *key;
    Getter get_next;
    int partition_number;
};

struct key_value_mapper {
    char *key;
    char *val;
    struct key_value_mapper next;
};

struct key_value_mapper *head;
struct key_value_mapper *tail;


struct key_value_mapper kvm;
int kvm_size = 0;

// struct for each partition
struct partition_info {
    int partition_hash;
    struct key_value_mapper *sorted_head;
};
// heap allocated array that will hold the partitions
struct partition_info *sorted_partitions;

// global to keep track of the number of partitions
int total_partitions;

// returns a pointer to the iterator's next value
char* get_next(char *key, int partition_number)
{
    // QUESTION: is partition_number the same as the hash partition??

    int hash = MR_DefaultHashPartition(key, total_partitions);
    
    for(int i = 0; i < total_partitions; i++) {
        if(sorted_partitions[i].partition_hash == hash) {
            struct key_value_mapper *iterator = sorted_partitions[i].sorted_head;
            while(iterator != NULL) {
                // find the key and return the next value
                if(iterator.key == key) {
                    return iterator.next.val;
                }
            }
        }
    }
    
    // returns NULL if for some reason the key is not found in the partition
    return NULL;
}


unsigned long MR_DefaultHashPartition(char *key, int num_partitions) 
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions)
{
    // not sure if initializing a global here will work
    total_partitions = num_partitions;

    // initialize the array of pointers
    sorted_partitions = calloc(sizeof(partition_info) * num_partitions);

    // loop through unsorted linked list
    struct key_value_mapper *iterator = head;
    while(iterator != NULL) {
        // get this iterator's hash
        int hash = MR_DefaultHashPartition(iterator.key, num_partitions); 

        // flag to check if there is an existing partition with a matching hash
        int found = 0; 

        for(int i = 0; i < num_partitions; i++) {
            if(sorted_partitions[i].partition_hash == hash) {
                // add the iterator to the partition's linked list
                // iterator.next = sorted_partitions[i].sorted_head;
                // sorted_partitions[i].sorted_head = iterator;

                // make a copy of the current struct
                struct key_value_mapper copy; 
                copy.val = iterator.val;
                copy.key = iterator.key;

                copy.next = sorted_partitions[i].sorted_head;
                sorted_partitions[i].sorted_head = copy;

                found = 1;
            } 
        }

        // EDGE CASE: the key/value pair to be added is the first for its partition
        if(!found) {
            for(int i = 0; i < num_partitions; i++) {
                if(sorted_partitions[i] == 0) {
                    // make a copy of the current struct
                    struct key_value_mapper copy; 
                    copy.val = iterator.val;
                    copy.key = iterator.key;

                    struct partition_info new;
                    new.partition_hash = hash;
                    new.sorted_head = copy;
                    copy.next = NULL;
                }
            }
        }

        // might have to create new key_value_mapper structs each time we add it to a partition linked list
        // since we are changing its next value, which will probably mess things up
        iterator = iterator.next;
    }

    // not sure what to return??
    return NULL;
}

void MR_Emit(char *key, char *value)
{   
    struct key_value_mapper *iterator = head;
    struct key_value_mapper *prev = NULL;

    if(head == NULL) {
        struct key_value_mapper new;
        new.key = key;
        new.val = value;
        head = new;
        new.next = NULL;
    }

    while(iterator != NULL) {
        if(strcmp(iterator.key, key) > 0) {
            struct key_value_mapper new;
            new.key = key;
            new.val = value;
            prev.next = new;
            new.next = iterator; 
            break;
        }
        prev = iterator;
        iterator = iterator.next;
    }
} 


void MR_Run(int argc, char *argv[], Mapper map,
            int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition,
            int num_partitions)
{

    kvm_size = 512;
    kvm = calloc(sizeof(key_value_mapper) * kvm_size);
    
    // do some checks on input arguments

    pthread_t mappers[num_mappers];
    for (int i = 0; i < num_mappers; i++)
    {
        if (i+1 < argc) {
            pthread_create(&mappers[i], NULL, map, argv[i]));
        }
        
    }

    // thread join

    for (int i = 0; i < num_mappers; i++)
    {
        pthread_join(&mappers[i], NULL);
    }

    // now do reducers
    pthread_t reducers[num_reducers];

    for (int i = 0; i < num_reducers; i++)
    {
        // todo get values
        struct arg_struct args;
        args.key = 
        args.get_next =
        args.partition_number = 
        
        pthread_create(map(&mappers[i], NULL, reduce, (void *)&args);
    }

    for (int i = 0; i < num_reducers; i++)
    {

        pthread_join(map(&mappers[i] argv[i]));
    }

}



