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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <hashtable.h>

#define testassert(a,b) if(!(a)){fprintf(stderr,b"\n");return false;}

static int hfun(void* key);
static int cmpfun (void *, void *);

static int hfun(
        void* key)
{
    int *i = (int *)key;
    int j = (*i * 23) + 41;
    return j;
}

static int cmpfun(
        void* v1,
        void* v2)
{
        int i1;
        int i2;

        i1 = *(int *)v1;
        i2 = *(int *)v2;

        return (i1 < i2 ? -1 : (i1 > i2 ? 1 : 0));
}

static bool do_hashtest(int elem, int size)
{
        HASHTABLE* h;
	HASHITERATOR* iter;
	int i;
	int val[elem];
	int res1[elem];
	int res2[elem];
	int* ival;
	struct timespec ts1;
	struct timespec ts2;

	memset(val,0,elem*sizeof(int));
	memset(res1,0,elem*sizeof(int));
	memset(res2,0,elem*sizeof(int));

        printf("Allocating hashtable of size %d and inserting %d elements.\n",size,elem);

        h = hashtable_alloc(size,hfun,cmpfun);
	testassert(h != NULL,"Error: hashtable was NULL");

	
	for(i = 0;i<elem;i++)
	{
	    val[i] = i;
	    testassert(hashtable_add(h,&val[i],&val[i]) != 0,
		     "Error: failed to add value to hashtable");
	}
	clock_gettime(CLOCK_REALTIME,&ts1);
	iter = hashtable_iterator(h);
	clock_gettime(CLOCK_REALTIME,&ts2);
	ts1.tv_sec = ts2.tv_sec - ts1.tv_sec;
	ts1.tv_nsec = ts2.tv_nsec - ts1.tv_nsec;
	printf("Iterator allocation took %lu seconds and %lu nanoseconds\n",(unsigned long)ts1.tv_sec,(unsigned long)ts1.tv_nsec);
	testassert(iter != NULL,"Error: first iterator was NULL");

	i = 0;
	clock_gettime(CLOCK_REALTIME,&ts1);

	while((ival = hashtable_next(iter)))
	{
	    res1[i++] = *ival;
	}

	clock_gettime(CLOCK_REALTIME,&ts2);
	ts1.tv_sec = ts2.tv_sec - ts1.tv_sec;
	ts1.tv_nsec = ts2.tv_nsec - ts1.tv_nsec;
	printf("First iteration took %lu seconds and %lu nanoseconds\n",(unsigned long)ts1.tv_sec,(unsigned long)ts1.tv_nsec);
	hashtable_iterator_free(iter);

	iter = hashtable_iterator(h);
	testassert(iter != NULL,"Error: second iterator was NULL");

	i = 0;
	clock_gettime(CLOCK_REALTIME,&ts1);

	while((ival = hashtable_next(iter)))
	{
	    res2[i++] = *ival;
	}
	clock_gettime(CLOCK_REALTIME,&ts2);
	ts1.tv_sec = ts2.tv_sec - ts1.tv_sec;
	ts1.tv_nsec = ts2.tv_nsec - ts1.tv_nsec;
	printf("Second iteration took %lu seconds and %lu nanoseconds\n",(unsigned long)ts1.tv_sec,(unsigned long)ts1.tv_nsec);
	hashtable_iterator_free(iter);

	for(i = 0;i<elem;i++)
	{
	    testassert((res1[i] == res2[i]),"Error: iterators returned different values");
	}
	hashtable_free(h);
        return true;
}

/** 
 * @node Simple test which creates hashtable and frees it. Size and number of entries
 * sre specified by user and passed as arguments.
 *
 *
 * @return 0 if succeed, 1 if failed.
 *
 * 
 * @details (write detailed description here)
 *
 */
int main(void)
{
        int rc = 1;

        if (!do_hashtest(0, 1))         goto return_rc;
        if (!do_hashtest(10, 1))        goto return_rc;
        if (!do_hashtest(1000, 10))     goto return_rc;
        if (!do_hashtest(10, 0))        goto return_rc;
        if (!do_hashtest(10, -5))       goto return_rc;
        if (!do_hashtest(1500, 17))     goto return_rc;
        if (!do_hashtest(1, 1))         goto return_rc;
        if (!do_hashtest(10000, 133))   goto return_rc;
        if (!do_hashtest(1000, 1000))   goto return_rc;
        if (!do_hashtest(10000, 10000)) goto return_rc;
        
        rc = 0;
return_rc:
        return rc;
}
