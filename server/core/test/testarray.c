/*
 * This file is distributed as part of MaxScale.  It is free
 * software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation,
 * version 2.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Copyright MariaDB Corporation Ab 2014
 */

/**
 *
 * @verbatim
 * Revision History
 *
 * Date          Who              Description
 * 21/04/2015    Markus Makela    Initial implementation
 *
 * @endverbatim
 */
#include <stdbool.h>
#include <stdio.h>
#include <array.h>
#include <pthread.h>
#include <spinlock.h>
#include <time.h>
#define TEST_ITERATIONS 1000
#define TEST_INSERTS 1000
#define THREAD_COUNT 25

static bool start = false;

void* addfun(void* param)
{
    ARRAY* array = (ARRAY*)param;
    struct timespec ts;
    int i,index;
    int* val;

    ts.tv_sec = 0;
    ts.tv_nsec = 1000000;

    while(!start)
    {
	nanosleep(&ts,NULL);
    }

    for(i = 0;i<TEST_INSERTS;i++)
    {
	val = malloc(sizeof(int));
	*val = i;
    array_push(array,val);
	
    }
    return NULL;
}

void* insfun(void* param)
{
    ARRAY* array = (ARRAY*)param;
    struct timespec ts;
    int i;
    int* val;

    ts.tv_sec = 0;
    ts.tv_nsec = 1000000;

    while(!start)
    {
	nanosleep(&ts,NULL);
    }

    for(i = 0;i<TEST_INSERTS;i++)
    {
	val = malloc(sizeof(int));
	*val = i;
    array_insert(array,1,val);
	
    }
    return NULL;
}

int cmpfun(const void* a, const void* b)
{
    void* val1 = *(void**)a;
    void* val2 = *(void**)b;
    return *(int*)val1 - *(int*)val2;
}

int main(int argc, char** argv)
{
    pthread_t thr[THREAD_COUNT];
    int i;
    int values[TEST_ITERATIONS];
    ARRAY* array = array_init();
    int index = TEST_ITERATIONS / 2;
    int expval = -1;
    
    if(array == NULL)
    {
        fprintf(stderr,"Error: Failed to initialize array.\n");           
        return 1;
    }

    for(i = 0;i<TEST_ITERATIONS;i++)
    {
        if(i == index)
        {
            printf("Test value at index %d: %d\n",index,i);
            expval = i;
        }
        values[i] = i;
        if(array_push(array,(void*)&values[i]) == 0)
        {
            fprintf(stderr,"Error: Failed to add value %d to the array.\n",values[i]);           
            return 1;
        }
    }


    void* ptr = array_fetch(array,index);
    if(ptr == NULL)
    {
        fprintf(stderr,"Error: Failed to fetch value from the array, NULL was returned.\n");
        return 1;
    }

    int value = *((int*)ptr);

    if(value != expval)
    {
        fprintf(stderr,"Error: Value at index %d was %d, not %d.\n",index,value,expval);
        return 1;
    }

    for(i = TEST_ITERATIONS - 1; i > -1;i--)
    {
        ptr = array_fetch(array,i);
        if(ptr != &values[i])
        {
            fprintf(stderr,"Error: Returned address was %p, not %p\n",
                    ptr,&values[i]);
            return 1;
        }
    }
    
    array_clear(array);
    int asize;
    if((asize = array_size(array)) != 0)
    {
	fprintf(stderr,
	 "Error: Array size is %d after clearing\n",
	 asize);
	return 1;
    }

printf("Creating %d threads each pushing %d elements into the array\n",
       THREAD_COUNT,
       TEST_INSERTS);

    for(i = 0;i<THREAD_COUNT;i++)
    {
	pthread_create(&thr[i],NULL,addfun,array);
    }

    start = true;
        for(i = 0;i<THREAD_COUNT;i++)
    {
	pthread_join(thr[i],NULL);
    }

    if((asize = array_size(array)) != TEST_INSERTS * THREAD_COUNT)
    {
	fprintf(stderr,
	 "Error: Array size is %d after pushing %d elements\n",
	 asize,
	 TEST_INSERTS * THREAD_COUNT);
	return 1;
    }

printf("Creating %d threads each inserting %d elements at the beginning of the array\n",
       THREAD_COUNT,
       TEST_INSERTS);

    for(i = 0;i<THREAD_COUNT;i++)
    {
	pthread_create(&thr[i],NULL,insfun,array);
    }

    start = true;
        for(i = 0;i<THREAD_COUNT;i++)
    {
	pthread_join(thr[i],NULL);
    }

    if((asize = array_size(array)) != TEST_INSERTS * THREAD_COUNT * 2)
    {
	fprintf(stderr,
	 "Error: Array size is %d after inserting %d elements to index 1\n",
	 asize,
	 TEST_INSERTS * THREAD_COUNT);
	return 1;
    }

    printf("Sorting array\n");
    array_sort(array,cmpfun);
    asize = array_size(array);
    int prev = -1;

    for(i = 0;i<asize;i++)
    {
        int* iptr = (int*)array_fetch(array,i);
        if(iptr == NULL)
        {
            fprintf(stderr,"Error: index %d returned NULL after sorting\n",i);   
            return 1;
        }            
        if(*iptr < prev)
        {
         fprintf(stderr,"Error:values %d was before %d after sorting\n",prev,*iptr);   
         return 1;
        }
    }
    
    array_free(array);
    
    return 0;
}
