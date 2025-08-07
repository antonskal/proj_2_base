#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mapreduce.h"
#include "common.h"
// add your code here ...
#include <string.h>
#include <sys/stat.h>
struct stat st;
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
    // add your code here ...
    FILE *pipe_fp;
    char command[100];
    char line_count[30];
    snprintf(command,sizeof(command),"wc -l %s | awk \'{print $1}\'",spec->input_data_filepath); 
    printf("Command to get line numbers: %s\n",command);
    pipe_fp = popen(command,"r");
    fgets(line_count,sizeof(line_count),pipe_fp);
    printf("Captured line_count: %s\n", line_count);
    long long int line_count_l = strtoll(line_count,NULL,10);
    long long int div = line_count_l / spec->split_num; //Line numbers per split
    //OPTIONAL strtok(line_count,"\n"); //removes new line
    //printf("Converted to long data type: %lld\n", line_count);
    
    char *split_command = "split --numeric-suffixes --additional-suffix=.txt --lines=";
    snprintf(command,sizeof(command),"%s%lld %s",split_command,div,spec->input_data_filepath);
    printf("Split command: %s\n", command);
    system(command);
    // Create N worker threads.
    int num_workers = spec->split_num;

   
// call split on file using system
//system("split --bytes=split_size 

    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
