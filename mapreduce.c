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
    long int spec_file_size = st.st_size;
    long int split_size = spec_file_size / spec->split_num;
    char digits_buffer[30]; //long long int will have 19 digits/bytes maximum + 1~2 so around 21 bytes
    snprintf(digits_buffer,sizeof(digits_buffer),"%ld",split_size); //converts the long int to a character byte buffer NULL TERMINATED
    //long int remainder = spec_file_size % spec->split_num;
    printf("Size of split files: %ld\n", split_size);
    printf("%s\n",digits_buffer);
    //printf("Remaining bytes: %ld\n", remainder);
    char *command = "split --numeric-suffixes --additional-suffix=.txt -b "; 
    size_t len1 = strlen(command);
    size_t len2 = strlen(digits_buffer);
    size_t len3 = strlen(spec->input_data_filepath);
    size_t len = len1 + len2 + len3 + 1 + 1; // + 1 for NULL terminated and one extra space
    char *system_command = malloc(len); 
    char *current_pos = system_command;
    memcpy(current_pos,command,strlen(command));
    current_pos += len1;
    memcpy(current_pos,digits_buffer,len2);
    current_pos += len2;
    *current_pos = ' ';
    current_pos += 1;
    memcpy(current_pos,spec->input_data_filepath,len3);
    current_pos += len3;
    *current_pos = '\0';
    printf("Concatenated string: %s\n",system_command);
    system(system_command);
    free(system_command);
    // Create N worker threads.
    int num_workers = spec->split_num;

   
// call split on file using system
//system("split --bytes=split_size 

    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
