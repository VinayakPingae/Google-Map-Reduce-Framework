#include <stdio.h>
#include <string.h>
#include<fcntl.h>
#include<stdlib.h>
#define BOUNDARY 26

int main(int argc, char *argv[])
{
   char ch = 'A';
   char writeStr[15];
   char sp = ' ';	
   char endLine = '\n';	
   int inputFileDesc,outputFileDesc,numRead;
   inputFileDesc = open(argv[1],O_RDONLY);
   outputFileDesc = open(argv[2],O_WRONLY);			
   int c = 0, count[BOUNDARY] = {0},i;
   long boundaryDecision =  lseek(inputFileDesc,0L,SEEK_END);
   lseek(inputFileDesc,0L,SEEK_SET);
   //boundaryDecision=2000;
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
	}
   for(i = 0; i < BOUNDARY; i++) {
			sprintf(writeStr,"%c%c%d%c",ch+i,sp,count[i],endLine);
			printf("%s",writeStr);
			write(outputFileDesc,writeStr,strlen(writeStr));
	}
	
	close(inputFileDesc);
	close(outputFileDesc);
   return 0;
}
