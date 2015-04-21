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
#include <array.h>

#define TEST_ITERATIONS 1000

int main(int argc, char** argv)
{
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
            printf("Test value at index %d: %d",index,i);
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
        if(ptr != &array[i])
        {
            fprintf(stderr,"Error: Returned address was %p, not %p\n",
                    ptr,&array[i]);
            return 1;
        }
    }
    
    array_free(array);
    
    return 0;
}
