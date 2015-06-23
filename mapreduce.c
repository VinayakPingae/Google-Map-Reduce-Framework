#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h> // fcntl.h has been included to perform universla I/O operations
#include "mapreduce.h"
#include "common.h"

void handler_sig(int signo)
{
    if (signo == SIGCONT) {
        printf("Received SIGCONT\n");
	}
    else if (signo == SIGSTOP) {
        printf("Received SIGSTOP\n");
	}
}


void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result)
{

	struct timeval start, end;

    if (NULL == spec || NULL == result)
    {
        EXIT_ERROR(ERROR, "NULL pointer!\n");
    }

    int input_file_desc;
	char *filePath = spec->input_data_filepath;
	int num_split = spec->split_num; // split_num = 50
	int i = 0, calculated_split_size = 0, numRead, temp_count = 0, parent = 0, child[num_split],  status = 0;
	int input_file_darray[num_split];
	int *fd_out = malloc(sizeof(int)*num_split);
	char ch;
	DATA_SPLIT * split;	
	/* Code snippet for making the indermediate file names*/
	char *ch_o = "mr";
	char writeOutputStr[15];
	char sp_underscore = '-';

	char sp_dot = '.';
	char *ch_ext = "itm";	
	//

	gettimeofday(&start, NULL);
		
	input_file_desc = open(filePath,O_RDONLY);
	long boundaryDecision =  lseek(input_file_desc,0L,SEEK_END);
	
		calculated_split_size = boundaryDecision / num_split;

	//printf("calculated_split_size %d\n",calculated_split_size*num_split);

  	lseek(input_file_desc, 0L, SEEK_SET);

	if(input_file_desc == -1) {
		printf("Error in file seek \n");
		exit(-1);	
	}
		
	int new_split_size=0;
	int curr_split=0;
	split = malloc(sizeof(DATA_SPLIT)*num_split);
	
		for(i = 0; i < num_split - 1; i++ ) {		
				input_file_darray[i] = open(filePath,O_RDONLY);
				lseek(input_file_darray[i],new_split_size,SEEK_SET);	
				lseek(input_file_desc,calculated_split_size,SEEK_CUR);	
				temp_count=0;	
					
					while((numRead = read(input_file_desc,&ch,1)) > 0) {
						if(ch != '\n') {
							temp_count++;				
						}
						else {
							temp_count++;
							break;
						}
					}

				curr_split=calculated_split_size + temp_count;
				new_split_size = new_split_size+calculated_split_size + temp_count;
		
				split[i].fd = input_file_darray[i];
				split[i].size = curr_split;
				
					if(spec->usr_data != NULL) {
						split[i].usr_data = spec->usr_data;
					} else {
						split[i].usr_data = NULL;
					}
	
				sprintf(writeOutputStr,"%s%c%d%c%s",ch_o,sp_underscore,i,sp_dot,ch_ext);	
				fd_out[i] = open(writeOutputStr,O_CREAT|O_RDWR,0666);	

    	}

	input_file_darray[i] = open(filePath,O_RDONLY);
	lseek(input_file_darray[i],new_split_size,SEEK_SET);

	temp_count = 0;	

	int final_new_split_size = boundaryDecision - new_split_size;

	split[i].fd = input_file_darray[i];
	split[i].size = final_new_split_size;
	// To specify the spec->usr_data for finder and counter 
		if(spec->usr_data != NULL) {
				split[i].usr_data = spec->usr_data;
		} else {
				split[i].usr_data = NULL;
		}

	sprintf(writeOutputStr,"%s%c%d%c%s",ch_o,sp_underscore,i,sp_dot,ch_ext);			

	fd_out[i] = open(writeOutputStr,O_CREAT|O_RDWR,0666);
	//printf("------------------- 2-------------------%d\n",new_split_size);
	
	
	
	child[0] = fork();
	if(child[0] == 0) {
		
		parent = getpid();
		spec->map_func(&split[0],fd_out[0]);
		lseek(fd_out[0],0L,SEEK_SET);
		signal(SIGSTOP,handler_sig);
		signal(SIGCONT,handler_sig);

	} else {
	
		parent = child[0];
		kill(parent,SIGSTOP);
	
	}
	
	if(getpid() != parent) {
	
		for(i=1;i<num_split;i++) {
	
			child[i] = fork();

			if(child[i] == 0) {

				spec->map_func(&split[i],fd_out[i]);
				lseek(fd_out[i],0L,SEEK_SET);
				_exit(EXIT_SUCCESS);

			}		
			
			
		}

		for(i=1;i<num_split;i++) {
			result->map_worker_pid[i] = child[i];
			waitpid(child[i],&status,0);
		}
		
	}

	if(getpid() == parent) {
	
		int finalOutputFD = open(result->filepath,O_CREAT|O_RDWR,0666);
		spec->reduce_func(fd_out,num_split,finalOutputFD);
		_exit(EXIT_SUCCESS);

	}
	
	result->map_worker_pid[0] = parent;
	result->reduce_worker_pid = parent;
	kill(parent,SIGCONT);
	waitpid(parent,&status,0);
	    
    // Calling of the counter function
    gettimeofday(&end, NULL);   
	
	// CLosing the file descriptors of the various files

	for(i=0;i<num_split;i++) {
		close(fd_out[i]);
		close(input_file_darray[i]);
	}
	free(fd_out);
	free(split);

    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);

}
