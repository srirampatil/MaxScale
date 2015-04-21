#ifndef _ARRAY_HG
#define _ARRAY_HG
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
