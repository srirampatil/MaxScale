#include <table_schema.h>

#include <json-c/json_object.h>
#include <json-c/json_util.h>

#include "bgdrw.h"
#include "bgdutils.h"

static ErrCode write_schema(char *file, TableSchema *schema);
static ErrCode read_schema(char *file, TableSchema **schema);

ErrCode write_object(char *file, void *obj, OpType optype, DataFormat format)
{
    ErrCode err = ErrNone;

    switch (optype)
    {
    case OpWriteSchema:
        err = write_schema(file, (TableSchema *)obj);
        break;
    }

    return err;
}


void *read_object(char *file, OpType optype, DataFormat format)
{
    void *retobj = NULL;
    ErrCode err = ErrNone;

    switch (optype)
    {
    case OpReadSchema:
        {
            TableSchema *schema = (TableSchema *)calloc(1, sizeof(TableSchema));
            err = read_schema(file, &schema);
            if (err != ErrNone)
            {
                free_table_schema(&schema);
                return NULL;
            }

            retobj = schema;
        }
        break;
    }

    return retobj;
}

/////////////////// private functions //////////////////////

static ErrCode write_schema(char *file, TableSchema *schema)
{
    struct json_object *json = table_schema_to_json(schema);
    if (json == NULL)
        return ErrJSONObject;

    if (json_object_to_file(file, json) < 0)
        return ErrFileWrite;

    return ErrNone;
}

static ErrCode read_schema(char *file, TableSchema **schema)
{
    /* struct json_object *obj = json_object_from_file(file);
    if (obj == NULL)
        return ErrFileRead;

    *schema = table_schema_from_json(obj); */
    return ErrNone;
}
