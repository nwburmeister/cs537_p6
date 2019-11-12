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

// not sure what return type this should be
void get_next(char *key, int partition_number)
{

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



