// FILE FOR IMPLEMENTING FRAMEWORK AS
// DEFINED IN MAPREDUCE.H


// not sure what return type this should be
void get_next()
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



}



