#include <array.h>

/** @file array.c Dynamic tightly packed array
 *
 * This file contains functions that operate on dynamic arrays. These arrays have
 * their data in a contiguous area of memory. This allows constant time access
 * to all indices and amortized constant time appending of values at the end
 * of the array. The array expands its allocated memory when necessary.
 */

int array_expand(ARRAY* array,unsigned int newsize);

/**
 * Allocate a new Array and initialize its variables.
 * @return Pointer to new array or NULL if memory allocation failed.
 */
ARRAY* array_init()
{
    ARRAY* array;
    if((array = malloc(sizeof(ARRAY))) != NULL)
    {
        array->n_elems = 0;
        array->size = ARRAY_MIN_SIZE;
        spinlock_init(&array->lock);
        if((array->data = malloc(sizeof(void*)*array->size)) == NULL)
        {
            free(array);
            return NULL;
        }
    }
    spinlock_release(&array->lock);
    return array;
}

/**
 * Free a previously allocated array.
 * @param array Array to free.
 */
void array_free(ARRAY* array)
{
    spinlock_acquire(&array->lock);
    array->n_elems = 0;
    spinlock_release(&array->lock);
    free(array->data);
    free(array);

}

/**
 * Return a value from the array at a given index. The indices start from 0 and
 * go up to N-1 where N is the number of elements in the array. Returns the value
 * or NULL if the index is out of bounds.
 * @param array Array to search
 * @param index Index of the given value
 * @return Pointer to value at the given index or NULL if the index is out of bounds
 */
void* array_fetch(ARRAY* array,unsigned int index)
{
    void* rval;
    spinlock_acquire(&array->lock);
    rval = array->n_elems > index ? array->data[index] : NULL;
    spinlock_release(&array->lock);
    return rval;
}

/**
 * Append a value to the array. The value is placed at the largest index in the
 * array.
 * @param array Array to append
 * @param data Data to add to the array
 * @return Number of added values
 */
int array_push(ARRAY* array, void* data)
{
    spinlock_acquire(&array->lock);
    if(array->n_elems + 1 >= array->size)
    {
        if(array_expand(array,ARRAY_DEFAULT_INCREMENT*array->size))
        {
            return 0;
            spinlock_release(&array->lock);
        }
    }
    array->data[array->n_elems++] = data;
    spinlock_release(&array->lock);
    return 1;
}

/**
 * Pop the last value from the array, removing it from the array.
 * @param array
 * @return The last value in the array or NULL if the array is empty
 */
void* array_pop(ARRAY* array)
{
    void* value;
    spinlock_acquire(&array->lock);
    if(array->n_elems == 0)
    {
        spinlock_release(&array->lock);
        return NULL;
    }
    value = array->data[array->n_elems - 1];
    array->n_elems--;
    spinlock_release(&array->lock);
    return value;
}

/**
 * Insert a value at the given index. The value of the index must be lower than
 * or equal to the number of elements in the array.
 * @param array Array to use
 * @param index Index where to insert the value
 * @param data Data to insert
 * @return Number of values added
 */
int array_insert(ARRAY* array, unsigned int index,void* data)
{
    int i;

    if(index > array->n_elems)
        return 0;
    else if(index == array->n_elems)
        return array_push(array,data);

    spinlock_acquire(&array->lock);

    if(array->n_elems + 1 >= array->size)
    {
        if(array_expand(array,ARRAY_DEFAULT_INCREMENT*array->size))
        {
            spinlock_release(&array->lock);
            return 0;
        }
    }
    
    for(i = array->n_elems;i>index;i--)
    {
        array->data[i] = array->data[i-1];
    }
    array->data[index] = data;
    array->n_elems++;
    spinlock_release(&array->lock);

    return 1;
}

/**
 * Delete a value at the given index.
 * @param array Array to use
 * @param index Index of the value to delete
 * @return  Number of deleted values
 */
int array_delete(ARRAY* array, unsigned int index)
{
    int i;

    spinlock_acquire(&array->lock);

    if(index >= array->n_elems)
    {
        spinlock_release(&array->lock);
        return 0;
    }
    for(i = index;i<array->n_elems - 1;i++)
    {
        array->data[i] = array->data[i+1];
    }

    return 1;
}

/**
 * Increase the capacity of the array. The array spinlock must already be hold.
 * This function should never be called directly.
 * @param array Array to expand
 * @param newsize The new size of the array
 * @return 0 on success, 1 if memory allocation fails.
 */
int array_expand(ARRAY* array,unsigned int newsize)
{
    void** newdata;
    
    if((newdata = realloc(array->data,sizeof(void*)*newsize)) == NULL)
    {
        return 1;
    }

    array->data = newdata;
    array->size = newsize;

    return 0;
}

/**
 * Sort an array with a user-provided function. The function is called with the
 * addresses of two elements in the array.
 * @param array Array to sort
 * @param cmpfn Function used for sorting
 * @see qsort
 */
void array_sort(ARRAY* array,int(*cmpfn)(const void*,const void*))
{
    spinlock_acquire(&array->lock);
    qsort(array->data,array->n_elems,sizeof(void*),cmpfn);
    spinlock_release(&array->lock);
}

/**
 * Remove all elements in the array, reducing the size to 0. The allocated memory
 * is kept in store for future use.
 * @param array Array to clear
 */
void array_clear(ARRAY* array)
{
    spinlock_acquire(&array->lock);
    array->n_elems = 0;
    spinlock_release(&array->lock);
}
/**
 * Return the size of the array.
 * @param array Array to inspect
 * @return Size of the array
 */
unsigned int array_size(ARRAY* array)
{
    unsigned int rval;
    spinlock_acquire(&array->lock);
    rval = array->n_elems;
    spinlock_release(&array->lock);
    return rval;
}
