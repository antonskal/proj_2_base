#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mapreduce.h"
#include "common.h"
// add your code here ...
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
struct stat st;
mode_t permissions = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
void split_file_by_lines(char *filepath,int n)
{

    FILE *pipe_fp;
    char command[100];
    char line_count[30];
    snprintf(command,sizeof(command),"wc -l %s | awk \'{print $1}\'",filepath); 
    printf("Command to get line numbers: %s\n",command);
    pipe_fp = popen(command,"r");
    fgets(line_count,sizeof(line_count),pipe_fp);
    printf("Captured line_count: %s\n", line_count);
    long long int line_count_l = strtoll(line_count,NULL,10);
    long long int div = line_count_l / n; //Line numbers per split
    //OPTIONAL strtok(line_count,"\n"); //removes new line
    //printf("Converted to long data type: %lld\n", line_count);
    
    char *split_command = "split --numeric-suffixes --additional-suffix=.txt --lines=";
    snprintf(command,sizeof(command),"%s%lld %s",split_command,div,filepath);
    printf("Split command: %s\n", command);
    system(command);
}


void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result)
{
    // add you code here ...
    //original_fd = open(spec->input_data_filepath,O_RDONLY);
    split_file_by_lines(spec->input_data_filepath,spec->split_num);
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
    // Create N worker threads.
    int num_workers = spec->split_num;
    for (int i = 0; i < num_workers; i++)
    {
	//pid_t pid = fork();
	char filename[30];
	snprintf(filename,sizeof(filename),"x%02d.txt",i);
	printf("Looking at file: %s\n",filename);
	DATA_SPLIT data;
	int fd = open(filename,O_RDONLY);
	data.fd = fd;
	fstat(fd,&st);
	data.size = st.st_size;
	printf("Size: %ld\n",st.st_size);
	char outfilename[30];
	snprintf(outfilename,sizeof(outfilename),"mr-%d.itm",i);
	int fd_out = open(outfilename,O_WRONLY | O_CREAT | O_TRUNC, permissions);	
	spec->map_func(&data,fd_out);
    }

   
// call split on file using system
//system("split --bytes=split_size 

    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
