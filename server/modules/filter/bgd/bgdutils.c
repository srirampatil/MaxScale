#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>

#include "bgdutils.h"

/*
 * Opens a file given folder and file names
 *
 * @param folder_path   parent forlder of the file
 * @param file_name     name of the file
 * @param mode          mode in which the file should be opened
 */
int open_file(char *folder_path, char *file_name, char *mode, FILE *fp)
{
    int error = 0;
    char *file_path = NULL;

    // +1 for separator "/"
    int size = ((folder_path != NULL) ? (strlen(folder_path) + 1) : 0);

    // +1 for null termination
    size += strlen(file_name) + 1;

    file_path = (char *)calloc(1, size);

    if (folder_path != NULL)
        sprintf(file_path, "%s/%s", folder_path, file_name);
    else
        sprintf(file_path, "%s", file_name);

    fp = fopen(file_path, mode);
    if (fp == NULL)
        error = errno;

    free(file_path);
    return error;
}


const char *err_msg_from_code(ErrCode err)
{
    char *msg = NULL;
    switch(err)
    {
    case ErrNone:
        msg = "";
        break;

    case ErrJSONParse:
        msg = "Error while parsing JSON";
        break;

    case ErrJSONObject:
        msg = "Could not convert to JSON object";
        break;

    case ErrNoExist:
        msg = "ErrNoExist";
        break;

    case ErrFileWrite:
        msg = "Error while writing to the file";
        break;

    case ErrFileRead:
        msg = "Error while reading from the file";
        break;
    }

    return msg;
}

#if 0
char *concat_for_path(int count, ...)
{
    int i;
    char *result, *final_path;
    va_list argp;
    int len = strlen(elem);

    va_start(argp, count);
    for (i = 0; i < count; ++i)
        len += strlen(va_arg(argp, char *)) + 1;
    va_end(argp);

    result = (char *)calloc(len, sizeof(char));
    va_start(argp, count);
    for (i = 0; i < count; ++i) {
        strcat(result, va_arg(argp, char *));
        strcat(result, "/");
    }
    va_end(argp);

    result[strlen(result) - 1] = '\0';

    final_path = realpath(result, NULL);
    free(result);
    return final_path;
}
#endif
