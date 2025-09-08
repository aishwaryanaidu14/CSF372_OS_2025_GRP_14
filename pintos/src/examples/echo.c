#include <stdio.h>
#include <syscall.h>

int main (int argc, char **argv)
{
  int i;

  for (i = 1; i < argc; i++){
    char* curr_arg=argv[i];
    char curr_char=curr_arg[0];
    int ind=0;
    while(curr_char!='\0'){
      if(!(curr_char=='\''||curr_char=='"'||curr_char=='\\'))
        printf("%c", curr_char);
      ind++;
      curr_char=curr_arg[ind];
    }
    printf(" ");
  }
  if(argc>1) printf ("\n");
  return EXIT_SUCCESS;
}