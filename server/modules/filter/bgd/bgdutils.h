#ifndef _BGDUTILS_H__
#define _BGDUTILS_H__

#define DOT_STR "."

#define PARAM_OPTIONS "options"
#define PARAM_TABLES "tables"

#define TABLES_DELIM ","

#define DATA_FILE_EXTN ".data"
#define SCHEMA_FILE_EXTN ".schema"
#define DATA_EXTN_LENGTH 5
#define SCHEMA_EXTN_LENGTH 7

typedef enum
{
    OpWriteSchema = 1,
    OpWriteRow,

    OpReadSchema,
    OpReadRow
} OpType;

typedef enum
{
    DataFormatJSON = 1,
    DataFormatXML
} DataFormat;

typedef enum
{
    ErrNone = 0,

    ErrJSONParse,
    ErrJSONObject,

    ErrNoExist,
    ErrFileWrite,
    ErrFileRead
} ErrCode;

FILE *open_file(char *folder_path, char *file_name, char *mode);

const char *err_msg_from_code(ErrCode err);

#if 0
char *concat_for_path(const char *elem, ...);
#endif

#endif  // _BGDUTILS_H__
