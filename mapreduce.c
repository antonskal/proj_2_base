#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mapreduce.h"
#include "common.h"
struct stat st;
// add your code here ...
#include <sys/stat.h>
void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result)
{
    // add you code here ...
    
    struct timeval start, end;

    if (NULL == spec || NULL == result)
    {
        EXIT_ERROR(ERROR, "NULL pointer!\n");
    }
    
    gettimeofday(&start, NULL);
    if (stat(spec->input_data_filepath,&st) == -1)
    {
	printf("ERROR getting file status");
	return;
    }
    long int spec_file_size = st.st_size;
    // add your code here ...
    printf("Size of file: %ld\n", spec_file_size);

    // calculate size in bytes of file
    // divide by n saving remainder
    long int split_size = spec_file_size / n;
    long int remainder = spec_file_size % n;
    char command[100];
    sprintf(command,"
    printf("Size of split files: %ld\n", split_size);
    printf("Remaining bytes: %ld\n", remainder);
    // call split on file using system
system("split --bytes=split_size 

    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
