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
struct key_value_mapper kvm;


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


// FUNCTION FOR INSERTING A NEW KEY, VALUE 
// AT THE BEGINNING OF THE LINKED LIST
void MR_Emit(char *key, char *value)
{   
    // TODO: ADD LOCKS
    struct key_value_mapper *iterator = head;
    struct key_value_mapper *prev = NULL;

    if(head == NULL) {
        struct key_value_mapper new;
        new.key = key;
        new.val = value;
        head = new;
        new.next = NULL;
    }else {
        struct key_value_mapper new;
        new.key = key;
        new.val = value;
        new.next = head;
        head = new;
    }

    // while(iterator != NULL) {
    //     if(strcmp(iterator.key, key) > 0) {
    //         struct key_value_mapper new;
    //         new.key = key;
    //         new.val = value;
    //         prev.next = new;
    //         new.next = iterator; 
    //         break;
    //     }
    //     prev = iterator;
    //     iterator = iterator.next;
    // }
} 


void MR_Run(int argc, char *argv[], Mapper map,
            int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition,
            int num_partitions)
{    
    // do some checks on input arguments

    for (int i = 1; i < argc; i++)
    {
        map(argv[i]);
    }


    // call sorted partition
    
    struct key_value_mapper iterator = head;
    while(iterator != NULL)
    {
        reduce(iterator.key, get_next, MR_DefaultHashPartition(iterator.key));
        iterator = iterator.next;
    }

    

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



