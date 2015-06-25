#ifndef _TABLE_INFO_H__
#define _TABLE_INFO_H__

#include <my_config.h>
#include <mysql.h>
#include <json-c/json_object.h>

#define TABLE_SCHEMA_DBNAME "dbname"
#define TABLE_SCHEMA_TBLNAME "tblname"
#define TABLE_SCHEMA_NCOLUMNS "ncolumns"
#define TABLE_SCHEMA_COLUMNS "columns"

#define COLUMN_DEF_TYPE "type"
#define COLUMN_DEF_COLNAME "colname"
#define COLUMN_DEF_DEFVAL "defval"

typedef struct column_def ColumnDef;

typedef struct
{
    char *dbname;           // database name
    char *tblname;          // table name

    int ncolumns;           // number of columns

    ColumnDef *head;        // head of list of columns
    ColumnDef *tail;        // tail of list of columns
} TableSchema;

struct column_def
{
    enum enum_field_types type;     // columns data type
    char *colname;                  // column name
    void *defval;                   // default value

    ColumnDef *next;                // next column
};


struct json_object *table_schema_to_json(TableSchema *);
TableSchema *table_schema_from_json(struct json_object *);
const char *table_schema_to_json_string(TableSchema *);
TableSchema *table_schema_from_json_string(char *);

struct json_object *column_def_to_json(ColumnDef *);
ColumnDef *column_def_from_json(struct json_object *);
const char *column_def_to_json_string(ColumnDef *);
ColumnDef *column_def_from_json_string(char *);

#endif  //_TABLE_INFO_H__
