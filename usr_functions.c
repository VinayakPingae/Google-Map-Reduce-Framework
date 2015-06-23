#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include "common.h"
#include "usr_functions.h"
#define BOUNDARY 26

/* User-defined map function for the "Letter counter" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int letter_counter_map(DATA_SPLIT * split, int fd_out)
{
   char ch = 'A';
   char writeStr[15];
   char sp = ' ';	
   char endLine = '\n';	
   int inputFileDesc,outputFileDesc,numRead;
   inputFileDesc = split->fd;
   outputFileDesc = fd_out;			
   int c = 0, count[BOUNDARY],i,k;

 	  	for(k = 0; k < BOUNDARY ; k++) {
		 count[k] = 0;
   		}

   long boundaryDecision =  split->size;
   char* string=malloc(sizeof(char)*boundaryDecision); // For the string buffer used in the program
   
   numRead = read(inputFileDesc,string,boundaryDecision);
	
	   if(numRead > 0) {
		   for(i = 0 ;i < boundaryDecision ; i++) {
				if ( (string[i] >= 'a' && string[i] <= 'z') ) {
		   		      count[string[i]-'a']++;
				} else if ((string[i] >= 'A' && string[i] <= 'Z')) {
					 count[string[i]-'A']++;
				}
			  c++;	
			 }
	   } else {
		   return -1;
	   }
  
   //printf("\nsplit->size :: %d  -------  split->fd :: %d --- fd_out :: %d\n",split->size,split->fd,fd_out);
  
   		for(i = 0; i < BOUNDARY; i++) {
			sprintf(writeStr,"%c%c%d%c",ch+i,sp,count[i],endLine);
			//printf("\n-------%s \n\n",writeStr);			
			write(outputFileDesc,writeStr,strlen(writeStr));
		}
	
	//close(inputFileDesc);
	//close(outputFileDesc);
	free(string);
    return 0;
}

/* User-defined reduce function for the "Letter counter" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int letter_counter_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
    char ch = 'A';
    char writeStr[15];
    char sp = ' ';	
    char endLine = '\n';	
	char * stringRead = NULL, *token;
	long boundaryDecision = 0L;
	int totalCount[BOUNDARY] = {0}, alphaCount =0;
	int i = 0, numRead = 0;	
	for(i = 0; i < fd_in_num;i++) {
		boundaryDecision =  lseek(p_fd_in[i],0L,SEEK_END);
		lseek(p_fd_in[i],0L,SEEK_SET);
	    stringRead=malloc(sizeof(char)*boundaryDecision); // For the string buffer used in the program    
		while((numRead = read(p_fd_in[i],stringRead,boundaryDecision))> 0) {
			alphaCount = 0;
			while((token = strtok(stringRead, " ")) != NULL) {
				int found = strtol(token, NULL, 0);
				if(found != 0) {
					totalCount[alphaCount] += found;
					alphaCount++;
				}
				stringRead = NULL;
			}
		}	
	}

	for( i =0; i < BOUNDARY; i++) {
		sprintf(writeStr,"%c%c%d%c",ch+i,sp,totalCount[i],endLine);
		write(fd_out,writeStr,strlen(writeStr));
	}
	free(stringRead);
    return 0;
}

/* User-defined map function for the "Word finder" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int word_finder_map(DATA_SPLIT * split, int fd_out)
{
   char *ch = split->usr_data;
   int inputFileDesc,outputFileDesc,numRead;
   inputFileDesc = split->fd;
   outputFileDesc = fd_out;			
   int c = 0,i = 0;
   long boundaryDecision =  split->size;
   char* string=malloc(sizeof(char)*boundaryDecision);
   char *temp_buffer; // For the string buffer used in the program
   int ptr_start, ptr_end;
   numRead = read(inputFileDesc,string,boundaryDecision);
   ptr_start = 0;
   ptr_end=0;
   if(numRead > 0) {
	   while(i < boundaryDecision) {
			for(c=ptr_start ; string[c] !='\n'&&c< boundaryDecision; c++ ){
					ptr_end++;
			}
				ptr_end++;
				temp_buffer = (char *)malloc(sizeof(char) * (ptr_end-ptr_start));
				strncpy(temp_buffer, &string[ptr_start], ptr_end-ptr_start);

			if(strstr(temp_buffer, ch) != NULL) {
				write(outputFileDesc,temp_buffer,strlen(temp_buffer));
			}
	  
			ptr_start = ptr_end;
			i=ptr_end;
		}

   }	
	//close(inputFileDesc);
	//close(outputFileDesc);
    free(string);
	free(temp_buffer);
    return 0;
}

/* User-defined reduce function for the "Word finder" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int word_finder_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
    char * stringRead = NULL;
	long boundaryDecision = 0L;
	int i = 0, numRead = 0;	

	for(i = 0; i < fd_in_num;i++) {
		boundaryDecision =  lseek(p_fd_in[i],0L,SEEK_END);
		lseek(p_fd_in[i],0L,SEEK_SET);
	    stringRead=malloc(sizeof(char)*boundaryDecision); // For the string buffer used in the program    
			while((numRead = read(p_fd_in[i],stringRead,boundaryDecision)) > 0) {
					write(fd_out,stringRead,boundaryDecision);
			}	
	}
	free(stringRead);
    return 0;
}


