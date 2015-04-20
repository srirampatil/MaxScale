#ifndef _ARRAY_HG
#define _ARRAY_HG

#include <stdlib.h>
#include <spinlock.h>

#define ARRAY_MIN_SIZE 4
#define ARRAY_DEFAULT_INCREMENT 1.5

typedef struct array_t{
    void** data;
    unsigned int n_elems;
    unsigned int size;
    SPINLOCK lock;
}ARRAY;

ARRAY* array_init();
void array_free(ARRAY*);
void* array_fetch(ARRAY*,unsigned int);
int array_push(ARRAY*, void*);
void* array_pop(ARRAY* array);
int array_insert(ARRAY*,unsigned int,void*);
int array_delete(ARRAY*,unsigned int);
void array_sort(ARRAY*,int(*)(const void*,const void*));
void array_clear(ARRAY*);
unsigned int array_size(ARRAY* array);
#endif
