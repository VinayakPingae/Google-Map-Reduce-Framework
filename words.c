#include <stdio.h>
#include <string.h>
#include<fcntl.h>
#include<stdlib.h>
#define BOUNDARY 26

int main(int argc, char *argv[])
{
   char *ch = "alice", *p;
   char writeStr[15];
   char sp = ' ';	
   char endLine = '\n';	
   int inputFileDesc,outputFileDesc,numRead;
   inputFileDesc = open(argv[1],O_RDONLY);
   outputFileDesc = open(argv[2],O_WRONLY);			
   int c = 0, count = 0,i = 0;
   long boundaryDecision =  lseek(inputFileDesc,0L,SEEK_END);
   lseek(inputFileDesc,0L,SEEK_SET);
   //boundaryDecision=12000;
   char* string=malloc(sizeof(char)*boundaryDecision);
   char *temp_buffer; // For the string buffer used in the program
   int ptr_start, ptr_end;
   numRead = read(inputFileDesc,string,boundaryDecision);
   ptr_start = 0;
   ptr_end=0;
   while(i <boundaryDecision) {
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
    	
	close(inputFileDesc);
	close(outputFileDesc);
   return 0;
}
