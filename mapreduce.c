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


// not sure what return type this should be
void get_next(char *key, int partition_number)
{

}


unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{

}

unsigned long MR_SortedPartition(char *key, int num_partitions)
{


}

void MR_Emit(char *key, char *value)
{

}


void MR_Run(int argc, char *argv[], Mapper map,
            int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition,
            int num_partitions)
{

    // do some checks on input arguments

    pthread_t mappers[num_mappers];
    
    


    for (int i = 1; i < num_mappers; i++)
    {
        pthread_create(&mappers[i], NULL, map, argv[i]));
    }

    // thread join

    for (int i = 1; i < num_mappers; i++)
    {
        pthread_join(&mappers[i], NULL);
    }

    // now do reducers
    pthread_t reducers[num_reducers];

    struct arg_struct args;
    args.arg1 = 

    for (int i = 1; i < num_mappers; i++)
    {
        pthread_create(map(&mappers[i], NULL, reduce, (void *)&args);
    }

    for (int i = 1; i < num_mappers; i++)
    {
        pthread_join(map(&mappers[i] argv[i]));
    }

}



