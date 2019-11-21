// FILE FOR IMPLEMENTING FRAMEWORK AS
// DEFINED IN MAPREDUCE.H

#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "unistd.h"
#include "mapreduce.h"

// struct to hold information about each key
struct key_value_mapper {
    int processed;
    char *key;
    char *val;
    struct key_value_mapper *next;
};

// struct to hold head to linked list and lock for each partition
typedef struct partition_info {
    struct key_value_mapper *head;
    pthread_mutex_t lock;
} PARTITION_INFO;

Partitioner partitioner;
Reducer reducer;
Mapper mapper;

int *isNextKeyDifferent;
int NUM_FILES;
int NEXT_PARTITION;
int NUM_PARTITIONS;
int CURR_FILE;

char **FILES;
pthread_mutex_t fileLock;
struct partition_info *partitions;

pthread_key_t glob_var_key;

// Default sorting that has been provided for us
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

// helper function to calculate log2(x)
int _log(int x) {
    int curr = x;
    int y = 0;

    while (curr != 1) {
        curr = curr / 2;
        y++;
    }

    return y;
}

// Calculates partition based on a given key and number of total partitions
unsigned long MR_SortedPartition(char *key, int num_partitions) {
    // if num_partitions is 1, the only possible partition is 0
    if (num_partitions == 1) {
        return 0;
    }

    // if the key is blank, return failure
    if (strlen(key) == 0) {
        return -1;
    }

    // find significant bits and shift accordingly
    int sigbits = _log(num_partitions);
    int shift = 32 - sigbits;
    unsigned long partition = (unsigned)atoi(key) >> shift;

    return partition;
}

// returns a pointer to the iterator's next value
char *get_next(char *key, int partition_number) {
    struct key_value_mapper *curr_partition = partitions[partition_number].head;
    // need to check if next value is different

    pthread_mutex_lock(&partitions[partition_number].lock);
    if (isNextKeyDifferent[partition_number] == 1) {
        // reset key to 0
        isNextKeyDifferent[partition_number] = 0;
        pthread_mutex_unlock(&partitions[partition_number].lock);
        return NULL;
    }

    if (curr_partition != NULL) {
        if (strcmp(curr_partition->key, key) == 0) {
            partitions[partition_number].head = curr_partition->next;
            if (curr_partition->next != NULL) {
                // if not 0, we have a new value
                if (strcmp(curr_partition->next->key, key) != 0) {
                    // set flag to 1
                    isNextKeyDifferent[partition_number] = 1;
                    pthread_mutex_unlock(&partitions[partition_number].lock);
                    return curr_partition->val;
                }
                pthread_mutex_unlock(&partitions[partition_number].lock);
                return curr_partition->val;
            } else {
                pthread_mutex_unlock(&partitions[partition_number].lock);
                return curr_partition->val;
            }
        }
    }
    // returns NULL if for some reason the key is not found in the partition

    pthread_mutex_unlock(&partitions[partition_number].lock);
    return NULL;
}

// FUNCTION FOR INSERTING A NEW KEY, VALUE
// AT THE BEGINNING OF THE LINKED LIST
void MR_Emit(char *key, char *value) {
    if (strlen(key) == 0) {
        return;
    }

    int hashIndex = partitioner(key, NUM_PARTITIONS);
    struct key_value_mapper *curr_partition = partitions[hashIndex].head;
    struct key_value_mapper *new = malloc(sizeof(struct key_value_mapper));
    new->key = malloc(sizeof(char) * (strlen(key) + 1));
    strcpy(new->key, key);
    new->val = value;
    new->processed = 0;

    struct key_value_mapper *iterator = partitions[hashIndex].head;

    if (iterator == NULL) {
        partitions[hashIndex].head = new;
        new->next = NULL;
        return;
    }

    // ACQUIRE THE LOCK
    pthread_mutex_lock(&partitions[hashIndex].lock);
    struct key_value_mapper *prev = NULL;
    while (iterator != NULL) {
        if (strcmp(iterator->key, key) > 0) {
            if (prev == NULL) {
                new->next = iterator;
                partitions[hashIndex].head = new;
                pthread_mutex_unlock(&partitions[hashIndex].lock);
                return;
            } else {
                prev->next = new;
                new->next = iterator;
                pthread_mutex_unlock(&partitions[hashIndex].lock);
                return;
            }
        }
        prev = iterator;
        iterator = iterator->next;
    }
    prev->next = new;
    new->next = NULL;
    // RELEASE THE LOCK
    pthread_mutex_unlock(&partitions[hashIndex].lock);
}

// assigns partitions to reducer threads
void *Reduce_Thread_Helper_Func() {
    // need to sort partition
    // keep track of next partition to sort

    while (1) {
        pthread_mutex_lock(&fileLock);
        if (NUM_PARTITIONS <= NEXT_PARTITION) {
            pthread_mutex_unlock(&fileLock);
            return NULL;
        }
        // GET THE NEXT AVAILABLE PARTITION TO PROCESS

        struct key_value_mapper *iterator = partitions[NEXT_PARTITION].head;
        int *p = malloc(sizeof(int));
        *p = NEXT_PARTITION;
        pthread_setspecific(glob_var_key, p);
        NEXT_PARTITION++;
        pthread_mutex_unlock(&fileLock);

        int *glob_spec_var = pthread_getspecific(glob_var_key);
        while (iterator != NULL) {
            reducer(iterator->key, get_next, *glob_spec_var);
            iterator = partitions[*glob_spec_var].head;
        }
    }
}

// assigns files to mapper threads
void *Map_Threads_Helper_Func() {
    while (1) {
        char *curr_filename;
        pthread_mutex_lock(&fileLock);

        if (NUM_FILES <= CURR_FILE) {
            pthread_mutex_unlock(&fileLock);
            return NULL;
        }
        curr_filename = FILES[CURR_FILE];
        CURR_FILE++;
        pthread_mutex_unlock(&fileLock);
        mapper(curr_filename);
    }
}

// creates threads and calls helper functions to run those threads
void MR_Run(int argc, char *argv[], Mapper map,
            int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition,
            int num_partitions) {
    // INITIALIZATION ***********************************************

    partitioner = partition;
    mapper = map;
    reduce = reduce;
    reducer = reduce;
    NUM_PARTITIONS = num_partitions;
    NUM_FILES = argc - 1;
    partitions = malloc((num_partitions + 1) * sizeof(struct partition_info));
    isNextKeyDifferent = calloc(num_partitions, sizeof(int));
    FILES = &argv[1];

    // END INITIALIZATION *******************************************

    // THREADS FOR MAPPERS
    pthread_t mappers[num_mappers];
    for (int i = 0; i < num_mappers; i++) {
        if (i < NUM_FILES) {
            pthread_create(&mappers[i], NULL, Map_Threads_Helper_Func, NULL);
        }
    }
    // JOIN MAPPERS
    for (int i = 0; i < num_mappers; i++) {
        if (i < NUM_FILES)
            pthread_join(mappers[i], NULL);
    }
    // THREADS FOR REDUCERS
    pthread_t reducers[num_reducers];
    pthread_key_create(&glob_var_key, NULL);
    for (int i = 0; i < num_reducers; i++) {
        if (i < NUM_PARTITIONS) {
            pthread_create(&reducers[i], NULL, Reduce_Thread_Helper_Func,
                           NULL);
        }
    }
    for (int i = 0; i < num_reducers; i++) {
        if (i < NUM_PARTITIONS) {
            pthread_join(reducers[i], NULL);
        }
    }

    // FREE MEMORY - might need to do more than this??
    free(partitions);
    free(isNextKeyDifferent);
}
