#include<stdio.h>
#include<../lib/user/syscall.c>

int main(int argc, char *argv[]) {
    for(int i=1;i<argc;i++){
        const char *filename = argv[i];
        int aa=create(filename, 32);
        printf("%s: created\n", filename);
    }
    
    return 0;
}
