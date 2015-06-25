#include "table_schema.h"

#include <stdlib.h>
#include <string.h>
#include <json-c/json_util.h>
#include <json-c/json_object_iterator.h>
#include <json-c/json_tokener.h>

/*
 * Function to convert TableSchema object into a JSON object
 *
 * @param schema - TableSchema object
 * @return struct json_object representing TableSchema
 */
struct json_object *table_schema_to_json(TableSchema *schema)
{
    if(schema == NULL)
        return NULL;

    struct json_object *schemajson = json_object_new_object();

    json_object_object_add(schemajson, TABLE_SCHEMA_DBNAME, 
            json_object_new_string(schema->dbname));
    json_object_object_add(schemajson, TABLE_SCHEMA_TBLNAME, 
            json_object_new_string(schema->tblname));
    json_object_object_add(schemajson, TABLE_SCHEMA_NCOLUMNS,
            json_object_new_int(schema->ncolumns));

    struct json_object *columns = json_object_new_array();

    ColumnDef *col = schema->head;
    while (col != NULL)
    {
        struct json_object *columnjson = column_def_to_json(col);
        if (columnjson != NULL)
            json_object_array_add(columns, columnjson);
    }

    json_object_object_add(schemajson, TABLE_SCHEMA_COLUMNS, columns);

    return schemajson;
}

static void free_table_schema(TableSchema **tblschema)
{
    TableSchema *schema = *tblschema;

    if (!schema)
        return;

    if (schema->dbname)
        free(schema->dbname);

    if (schema->tblname)
        free(schema->tblname);

    free(schema);
    schema = NULL;
}

/*
 * Generates TableSchema from given JSON object
 *
 * @param schemajson - JSON representation of a TableSchema
 * @return TableSchema object
 */
TableSchema *table_schema_from_json(struct json_object *schemajson)
{
    if (schemajson == NULL)
        return NULL;

    TableSchema *schema = (TableSchema *)calloc(1, sizeof(TableSchema));
    const char *key;
    struct json_object *value;

    struct json_object_iterator begin_iter = json_object_iter_begin(schemajson);
    struct json_object_iterator end_iter = json_object_iter_end(schemajson);

    while (!json_object_iter_equal(&begin_iter, &end_iter))
    {
        key = json_object_iter_peek_name(&begin_iter);
        value = json_object_iter_peek_value(&begin_iter);

        if (!strcmp(key, TABLE_SCHEMA_DBNAME))
            schema->dbname = strdup(json_object_get_string(value));
        else if (!strcmp(key, TABLE_SCHEMA_TBLNAME))
            schema->tblname = strdup(json_object_get_string(value));
        else if (!strcmp(key, TABLE_SCHEMA_NCOLUMNS))
            schema->ncolumns = json_object_get_int(value);
        else if (!strcmp(key, TABLE_SCHEMA_COLUMNS))
        {
            struct json_object *col_value;
            struct json_object_iterator abegin_iter = json_object_iter_begin(value);
            struct json_object_iterator aend_iter = json_object_iter_end(value);

            while (!json_object_iter_equal(&abegin_iter, &aend_iter))
            {
                col_value = json_object_iter_peek_value(&abegin_iter);

                ColumnDef *col = column_def_from_json(col_value);
                if (schema->head == NULL)
                    schema->head = schema->tail = col;
                else
                {
                    schema->tail->next = col;
                    schema->tail = col;
                }

                json_object_iter_next(&abegin_iter);
            }
        }

        json_object_iter_next(&begin_iter);
    }
    
    return schema;
}


const char *table_schema_to_json_string(TableSchema *schema)
{
    struct json_object *schemajson = table_schema_to_json(schema);
    if (schemajson)
        return json_object_to_json_string(schemajson);

    return NULL;
}

TableSchema *table_schema_from_json_string(char *jsonstr)
{
    TableSchema *schema = NULL;
    struct json_tokener *tok = json_tokener_new();
    struct json_object *schemajson = json_tokener_parse((const char *)jsonstr);
    enum json_tokener_error jerr = json_tokener_get_error(tok);
    if (json_tokener_success != jerr)
        goto tjstr_end;

    schema = table_schema_from_json(schemajson);

tjstr_end:
    json_tokener_free(tok);
    return schema;
}

struct json_object *column_def_to_json(ColumnDef *col)
{
    struct json_object *columnjson = json_object_new_object();
        
    json_object_object_add(columnjson, COLUMN_DEF_COLNAME, 
                json_object_new_string(col->colname));
    json_object_object_add(columnjson, COLUMN_DEF_TYPE, 
                json_object_new_int(col->type));
        
    if (col->defval != NULL)
        json_object_object_add(columnjson, COLUMN_DEF_DEFVAL, 
                    json_object_new_string((char *)col->defval));

    return columnjson;
}


ColumnDef *column_def_from_json(struct json_object *columnjson)
{
    if (columnjson == NULL);
        return NULL;

    struct json_object *value = NULL;
    ColumnDef *col = (ColumnDef *)calloc(1, sizeof(ColumnDef));
    const char *key;

    struct json_object_iterator begin_iter = json_object_iter_begin(columnjson);
    struct json_object_iterator end_iter = json_object_iter_end(columnjson);

    while (!json_object_iter_equal(&begin_iter, &end_iter))
    {
        key = json_object_iter_peek_name(&begin_iter);
        value = json_object_iter_peek_value(&begin_iter);

        if (!strcmp(key, COLUMN_DEF_COLNAME))
            col->colname = strdup(json_object_get_string(value));
        else if (!strcmp(key, COLUMN_DEF_TYPE))
            col->type = (enum enum_field_types) json_object_get_int(value);
        else if (!strcmp(key, COLUMN_DEF_DEFVAL))
            col->defval = strdup(json_object_get_string(value));

        json_object_iter_next(&begin_iter);
    }

    return col;
}

const char *column_def_to_json_string(ColumnDef *col)
{
    struct json_object *columnjson = column_def_to_json(col);
    if (columnjson)
        return json_object_to_json_string(columnjson);

    return NULL;
}

ColumnDef *column_def_from_json_string(char *jsonstr)
{
    ColumnDef *col = NULL;
    struct json_tokener *tok = json_tokener_new();
    struct json_object *columnjson = json_tokener_parse((const char *)jsonstr);
    enum json_tokener_error jerr = json_tokener_get_error(tok);
    if(json_tokener_success != jerr)
        goto cjstr_end;

    col = column_def_from_json(columnjson);

cjstr_end:
    json_tokener_free(tok);
    return col;
}
