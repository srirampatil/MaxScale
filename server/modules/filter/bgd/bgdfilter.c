/*
 *
 *       Filename:  bgdfilter.c
 *
 *    Description:  Design a filter that will capture incoming inserts, updates and 
 *                  deletes, for specified tables (as regex) in a separate log file
 *                  that is consumable as JSON or CSV form. So  that  external  ETL 
 *                  processes can process it for data uploading  into  DWH  or  big 
 *                  data platform. Optionally a plugin that takes this log  into  a 
 *                  Kafka  broker  that  can  put  this  data on Hadoop node can be 
 *                  developed as next step.
 *
 *        Version:  1.0
 *        Created:  Monday 25 May 2015 10:16:06  IST
 *       Revision:  none
 *
 *         Author:  Sriram Patil
 *   Organization:  
 *
 */

#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <filter.h>
#include <modinfo.h>
#include <modutil.h>
#include <skygw_utils.h>
#include <log_manager.h>
#include <time.h>
#include <sys/time.h>
#include <regex.h>
#include <string.h>
#include <atomic.h>
#include <query_classifier.h>
#include <spinlock.h>
#include <mysql_client_server_protocol.h>
#include <sys/stat.h>
#include <hashtable.h>

#include "bgdutils.h"
#include "bgdrw.h"

/** Defined in log_manager.cc */
extern int            lm_enabled_logfiles_bitmask;
extern size_t         log_ses_count[];
extern __thread log_info_t tls_log_info;

MODULE_INFO info = {
	MODULE_API_FILTER,
	MODULE_GA,
	FILTER_VERSION,
	"A data logger filter, which logs incoming inserts, updates and deletes for \
            specified table in log files in JSON or XML format"
};

static char *version_str = "V1.1.1";

/*
 * The filter entry points
 */
static	FILTER	*createInstance(char **options, FILTER_PARAMETER **);
static	void	*newSession(FILTER *instance, SESSION *session);
static	void 	closeSession(FILTER *instance, void *session);
static	void 	freeSession(FILTER *instance, void *session);
static	void	setDownstream(FILTER *instance, void *fsession, DOWNSTREAM *downstream);
static  void	setUpstream(FILTER *instance, void *fsession, UPSTREAM *downstream);
static	int	    routeQuery(FILTER *instance, void *fsession, GWBUF *queue);
static  int     clientReply(FILTER *instance, void *fsession, GWBUF *queue);
static	void	diagnostic(FILTER *instance, void *fsession, DCB *dcb);

static FILTER_OBJECT MyObject = {
    createInstance,
    newSession,
    closeSession,
    freeSession,
    setDownstream,
    setUpstream,
    routeQuery,
    clientReply,
    diagnostic,
};


/*
 * TableInfo structure
 */
typedef struct table_info TableInfo;

struct table_info
{
    bool is_valid;          // identifies if the table_info is filled with 
                            // valid information

    char *schema_file_path;    
    char *data_file;        // <db name>.<table name>.data
    FILE *fp;

    TableSchema *schema;
};


/*
 *       struct:  BGD_INSTANCE
 *  Description:  bgdfilter instance.
 */
typedef struct bgd_instance BGD_INSTANCE;

struct bgd_instance {
    char *format;   /* Storage format JSON or XML (Default: JSON */
    char *path;     /* Path to a folder where to store all data files */
   
    HASHTABLE *htable;   /* Hash table mapping from file name to TableInfo */

    BGD_INSTANCE *next;
};

static SPINLOCK instances_lock;
static BGD_INSTANCE *instances_list;

/*
 *       struct:  BGD_SESSION
 *  Description:  bgdfilter session
 */
typedef struct bgd_session BGD_SESSION;

struct bgd_session {
    DOWNSTREAM down;
    UPSTREAM up;

    GWBUF *query_buf;

    char *default_db;       // Current database name 
    char *active_db;           // Used when trying to change database (USE DB)

    char *curr_data_file_path;
    char *curr_schema_file_path;

    bool active;            // Not used currently
};


static void read_existing_schema(BGD_INSTANCE *bgd_instance, char *dbname,
                char *tname, TableInfo *tinfo);
static bool process_tables_param(char *, BGD_INSTANCE *);

static void free_bgd_instance(BGD_INSTANCE *);
static void free_bgd_session(BGD_SESSION *);
void free_table_info(void *);       // use while initializing HASHTABLE

static void build_data_file_path(char *dbname, char *tblname, char **file_name);
static int fp_hashfn(void *key);
static int fp_cmpfn(void *key1, void *key2);

/**
 * Implementation of the mandatory version entry point
 *
 * @return version string of the module
 */
char *version()
{
	return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 */
void ModuleInit()
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    spinlock_init(&instances_lock);
    instances_list = NULL;
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
FILTER_OBJECT *GetModuleObject()
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));
	return &MyObject;
}

/* 
 * Create directory with all the error checking
 *
 * @param path  path of directory to be created.
 * @return zero if successful else errno value set by system calls.
 */
static int 
create_dir(char *path)
{
    struct stat st;

    // Checking if the directory already exists
    if(stat(path, &st) == 0)
    {
        // Checking if the exising file is a directory
        if(S_ISDIR(st.st_mode))
        {
            LOGIF(LD, (skygw_log_write_flush(
				    LOGFILE_DEBUG,
				    "bgdfilter: '%s' directory already exists.\n",
				    path)));

            return 0;
        }
        else
        {
            LOGIF(LE, (skygw_log_write_flush(
				    LOGFILE_ERROR,
				    "bgdfilter: '%s' exists, but not a directory.\n",
				    path)));

            return EEXIST;
        }

    }
    else if(mkdir(path, 0644) != 0)
    {
        int err = errno;
        if(err != EEXIST)
            return err;
    }

    return 0;
}

/*
 * Log the data coming from an insert query.
 *
 * @param bgd_instance  Current filter instance
 * @param bgd_session   Current filter session
 * @param queue         buffer containing the insert query
 */
static void
log_insert_data(BGD_INSTANCE *bgd_instance, BGD_SESSION *bgd_session, GWBUF *queue)
{
    char *file_name = bgd_session->curr_data_file_path;
    TableInfo *tinfo = NULL;

    LOGIF(LD, (skygw_log_write_flush(
                    LOGFILE_DEBUG, 
                    "bgdfilter: %s.\n", __func__)));

    tinfo = hashtable_fetch(bgd_instance->htable, file_name);
    if (tinfo == NULL || tinfo->fp == NULL)
    {
        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, 
                        "bgdfilter: Table info or file pointer not found in \
                        the hash table.")));
        goto log_insert_data_end;
    }

    if (fprintf(tinfo->fp, "%s\n", modutil_get_SQL(queue)) < 0)
    {
        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, 
                        "bgdfilter: Cannot write to '%s/%s'",
                        bgd_instance->path, file_name)));
        goto log_insert_data_end;
    }

    fflush(tinfo->fp);

log_insert_data_end:
    return;
}

/* 
 *         Name:  createInstance
 *  Description:  
 */
static FILTER *createInstance(char **options, FILTER_PARAMETER **params)
{
    BGD_INSTANCE * bgd_instance;
    HASHTABLE *ht;
    int dir_ret, i;
    bool has_tables_param = false;

    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    if ((bgd_instance = (BGD_INSTANCE *)calloc(1, sizeof(BGD_INSTANCE))) == NULL)
        return NULL;

    // Hash table initialization
    if ((ht = hashtable_alloc(100, fp_hashfn, fp_cmpfn)) == NULL)
    {
        LOGIF(LE, (skygw_log_write_flush(
				LOGFILE_ERROR,
				"bgdfilter: Could not allocate hashtable")));

        free_bgd_instance(bgd_instance);
        return NULL;
    }

    hashtable_memory_fns(ht, (HASHMEMORYFN)strdup, NULL, (HASHMEMORYFN)free, (HASHMEMORYFN)free_table_info);

    bgd_instance->htable = ht;
    bgd_instance->format = NULL;
    bgd_instance->path = NULL;

    if (!params)
    {
        LOGIF(LE, (skygw_log_write_flush(
				LOGFILE_ERROR,
				"bgdfilter: Insufficient parameters.\n")));
        free_bgd_instance(bgd_instance);
        return NULL;
    } 

    if (options)
        bgd_instance->path = strdup(options[0]);
    else
        bgd_instance->path = strdup("/tmp/bgd");

    if ((dir_ret = create_dir(bgd_instance->path)) != 0)
    {
        LOGIF(LE, (skygw_log_write_flush(
				LOGFILE_ERROR,
				"bgdfilter: '%s' %s.\n",
                bgd_instance->path,
				strerror(dir_ret))));

        free_bgd_instance(bgd_instance);
        return NULL;
    }

    for (i = 0; params[i]; i++)
    {
        if (!strcmp(PARAM_TABLES, params[i]->name))
        {
            if (!process_tables_param(params[i]->value, bgd_instance))
            {
                free_bgd_instance(bgd_instance);
                return NULL;
            }

            has_tables_param = true;
        }
        else if (!filter_standard_parameter(params[i]->name))
        {
		    LOGIF(LE, (skygw_log_write_flush(
                   LOGFILE_ERROR,
                   "bgdfilter: Unexpected parameter '%s'.\n",
                   params[i]->name)));
        }
    }

    if(!has_tables_param)
    {
        LOGIF(LE, (skygw_log_write_flush(
				LOGFILE_ERROR,
				"bgdfilter: 'tables' parameter required.\n")));
        free_bgd_instance(bgd_instance);
        return NULL;
    } 

    spinlock_acquire(&instances_lock);
    bgd_instance->next = instances_list;
    instances_list = bgd_instance;
    spinlock_release(&instances_lock);

    return (FILTER *)bgd_instance;
}

/* 
 *         Name:  newSession
 *  Description:  
 */
static void *newSession(FILTER *instance, SESSION *session)
{
    BGD_SESSION *bgd_session;
    MYSQL_session *ses_data = (MYSQL_session *)session->data;

    if((bgd_session = (BGD_SESSION *)calloc(1, sizeof(BGD_SESSION))) == NULL)
        return NULL;

    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    bgd_session->active_db = NULL;
    bgd_session->query_buf = NULL;
    bgd_session->default_db = NULL;
    bgd_session->active = false;
    bgd_session->curr_data_file_path = NULL;

    if(ses_data->db != NULL)
        bgd_session->default_db = strdup(ses_data->db);

    return bgd_session;
}

/* 
 *         Name:  closeSession
 *  Description:  
 */
static void closeSession(FILTER *instance, void *session)
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));
 
    return;
}

/* 
 *         Name:  freeSession
 *  Description:  
 */
static void freeSession(FILTER *instance, void *session)
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    free_bgd_session((BGD_SESSION *)session);    
}

/* 
 *         Name:  setDownstream
 *  Description:  
 */
static void	setDownstream(FILTER *instance, void *fsession, 
        DOWNSTREAM *downstream)
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    BGD_SESSION *bgd_session = (BGD_SESSION *) fsession;
    bgd_session->down = *downstream;
}


static void	
setUpstream(FILTER *instance, void *fsession, UPSTREAM *upstream)
{
    BGD_SESSION *bgd_session = (BGD_SESSION *)fsession;
    bgd_session->up = *upstream;
}

/* 
 *         Name:  routeQuery
 *  Description:  
 */
static int 
routeQuery(FILTER *instance, void *fsession, GWBUF *queue)
{
    BGD_INSTANCE *bgd_instance = (BGD_INSTANCE *)instance;
    BGD_SESSION *bgd_session = (BGD_SESSION *)fsession;
    int number_of_tables = 0, i;
    char **table_names = NULL;
    bool success = true;
    char *table_file = NULL;
    char *tbl = NULL, *db = NULL;

    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n", __func__))); 

    // Check if user is changing database
    if (*((char *)(queue->start + 4)) == MYSQL_COM_INIT_DB)
    {
        if (bgd_session->active_db)
            free(bgd_session->active_db);

        bgd_session->active_db = strdup((char *) (queue->start + 5));
        bgd_session->active = true;
        goto route_query_end;
    }
    
    if (modutil_is_SQL(queue))
    {
        bgd_session->query_buf = gwbuf_clone(queue);

        if (!query_is_parsed(queue))
            success = parse_query(queue);

        if (!success)
        {
            skygw_log_write(LOGFILE_ERROR,"Error: Parsing query failed.");
            goto route_query_end;
        }
    
        table_names = skygw_get_table_names(queue, &number_of_tables, true);
        if(number_of_tables == 0)
            goto route_query_end;

        // Handling the case where db.table is specified in the query
        if (strstr(table_names[0], DOT_STR))
        {
            char *savedstr = NULL;
            char *tok = strtok_r(table_names[0], DOT_STR, &savedstr);
            while (tok)
            {
                if (!db)
                    db = tok;
                else if (!tbl)
                    tbl = tok;
                    
                tok = strtok_r(NULL, DOT_STR, &savedstr);
            }

            if (bgd_session->active_db)
                free(bgd_session->active_db);

            bgd_session->active_db = strdup(db);
        }
        else
        {
            db = bgd_session->default_db;
            tbl = table_names[0];

            if (bgd_session->active_db)
                free(bgd_session->active_db);

            bgd_session->active_db = strdup(db);
        }

        build_data_file_path(db, tbl, &table_file);
        TableInfo *tinfo = hashtable_fetch(bgd_instance->htable, table_file);
        if (tinfo == NULL)
        {
            free(table_file);
            goto route_query_end;
        }

        read_existing_schema(bgd_instance, db, tbl, tinfo);

        if(bgd_session->curr_data_file_path != NULL)
            free(bgd_session->curr_data_file_path);

        bgd_session->curr_data_file_path = table_file;
        bgd_session->active = true;
    }
   
route_query_end:
    return bgd_session->down.routeQuery(bgd_session->down.instance, 
                    bgd_session->down.session, queue);
}


static int 
clientReply(FILTER *instance, void *fsession, GWBUF *reply)
{
    BGD_INSTANCE *bgd_instance = (BGD_INSTANCE *)instance;
    BGD_SESSION *bgd_session = (BGD_SESSION *)fsession;
    bool success = true;
    GWBUF *queue = bgd_session->query_buf;
    TableSchema *schema = NULL;
    ColumnDef *cdef = NULL;
    bool is_sql;
    TableInfo *tinfo = NULL;

    unsigned char *ptr = (unsigned char *)reply->start;

    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG, "bgdfilter: %s - %s.\n", __func__,
                ((bgd_session->active)?"session active":"session inactive"))));

    if (!bgd_session->active)
      goto client_reply_end; 

    if (PTR_IS_ERR(ptr) || bgd_session->query_buf == NULL || !PTR_IS_OK(ptr))
    {
        if (bgd_session->active_db != NULL)
        {
            free(bgd_session->active_db);
            bgd_session->active_db = NULL;
        }
        goto client_reply_end;
    }

    // Changin the database
    if (*((char *)(queue->start + 4)) == MYSQL_COM_INIT_DB)
    {
        if (bgd_session->default_db != NULL)
            free(bgd_session->default_db);

        bgd_session->default_db = bgd_session->active_db;
        bgd_session->active_db = NULL;
        goto client_reply_end;
    }

    is_sql = modutil_is_SQL(queue);
    if (is_sql)
    {
        if (!query_is_parsed(queue))
            success = parse_query(queue);

        if (!success)
        {
            skygw_log_write(LOGFILE_ERROR,"Error: Parsing query failed.");
            goto client_reply_end;
        }

        int op = query_classifier_get_operation(queue);
        switch (op)
        {
        case QUERY_OP_INSERT:              
            log_insert_data(bgd_instance, bgd_session, queue);
            break;

        case QUERY_OP_CREATE_TABLE: {
            schema = skygw_get_schema_from_create(bgd_session->query_buf);
            schema->dbname = strdup(bgd_session->active_db);

            tinfo = (TableInfo *) hashtable_fetch(bgd_instance->htable,
                            bgd_session->curr_data_file_path);
            tinfo->schema = schema;

            write_object(tinfo->schema_file_path, tinfo->schema, OpWriteSchema,
                    DataFormatJSON); 

        }
            break;

        default:
            LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: not detected %d %s.\n", op,
				modutil_get_query(queue))));
            break;
        }
    }

client_reply_end:
    
    bgd_session->active = false;
    return bgd_session->up.clientReply(bgd_session->up.instance,
                bgd_session->up.session, reply);
}

/* 
 *         Name:  diagnostic
 *  Description:  
 */
static void	diagnostic(FILTER *instance, void *fsession, DCB *dcb)
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));
    return;
}

/////////////// Filter params processing ////////////////

/*
 * Processes the tables parameter and stores respective TableInfo objects in
 * HASHTABLE. Assumes that tables string is trimmed.
 *
 * @param tables    comma separated * <db>.<table> strings expected, no spaces 
 *                  in between
 * @param bgd_instance
 * @return false if error
 */
static bool process_tables_param(char *tables, BGD_INSTANCE *bgd_instance)
{
    if(tables == NULL || !strcmp(tables, ""))
        return false;

    struct stat st;
    int error;
    char *savedptr = NULL;
    char *tname = strtok_r(tables, TABLES_DELIM, &savedptr);
    while (tname != NULL)
    {
        TableInfo *tinfo = (TableInfo *)calloc(1, sizeof(TableInfo));
        if (tinfo == NULL)
        {
            LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "bgdfilter: \
                            Cannot allocate memory to TableInfo in \
                            function %s at line %d.", __func__,
                            __LINE__)));
            return false;
        }

        tinfo->is_valid = true;

        // +1 for null termination
        tinfo->data_file = (char *)calloc(strlen(tname) + DATA_EXTN_LENGTH
                                       + 1, sizeof(char));
        strcpy(tinfo->data_file, tname);
        strcat(tinfo->data_file, DATA_FILE_EXTN);

        error = open_file(bgd_instance->path, tinfo->data_file, "a", tinfo->fp);
        if (error)
        {
            LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, 
                    "bgdfilter: Cannot open '%s/%s': %s",
                    bgd_instance->path, tinfo->data_file, strerror(error))));
            return false;
        }

        hashtable_add(bgd_instance->htable, tinfo->data_file, tinfo);

        tname = strtok_r(NULL, TABLES_DELIM, &savedptr);
    }

    return true;
}

/////////////// Freeing functions ///////////////////

/*
 *  Frees the TableInfo object
 */
void free_table_info(void *param)
{
    if(param == NULL)
        return;

    TableInfo *info = (TableInfo *)param;
    if (info->data_file != NULL)
        free(info->data_file);

    if (info->fp != NULL)
        fclose(info->fp);

    free(info);
}


////////////////// static functions ///////////////////

/*
 * Frees BGD_INSTANCE object
 */
static void free_bgd_instance(BGD_INSTANCE *instance)
{
    if(instance == NULL)
        return;

    if (instance->path != NULL)
        free(instance->path);

    if (instance->htable != NULL)
        hashtable_free(instance->htable);

    free(instance);
}


static void free_bgd_session(BGD_SESSION *session)
{
    if(session == NULL)
        return;

    if (session->query_buf != NULL)
        free(session->query_buf);

    if (session->default_db != NULL)
        free(session->default_db);

    if (session->active_db != NULL)
        free(session->active_db);

    if (session->curr_data_file_path != NULL)
        free(session->curr_data_file_path);

    free(session);
}


/*
 * Hash function for htable in BGD_INSTANCE
 *
 * @param key   null-terminated string to be hashed.
 * @return hash value
 */
static int fp_hashfn(void *key)
{
    if(key == NULL)
        return 0;

    int hash = 0,c = 0;
    char* ptr = key;
    while((c = *ptr++))
        hash = c + (hash << 6) + (hash << 16) - hash;

    return hash;
}

/*
 * Comparison function for htable in BGD_INSTANCE
 *
 * @param key1  first null-terminated string to be compared
 * @param key2  second null-terminated string to be compared
 * @return zero if equal, non-zero otherwise
 */
static int fp_cmpfn(void *key1, void *key2)
{
    return strcmp((char *)key1, (char *)key2);
}


static void build_data_file_path(char *dbname, char *tblname, char **file_name)
{
    // 1 for . and 1 for null termination
    int size = strlen(tblname) + DATA_EXTN_LENGTH + 1 + 1;
    if (dbname)
        size += strlen(dbname);

    *file_name = (char *)calloc(1, size);
    if (*file_name != NULL)
    {
        if (dbname)
            sprintf(*file_name, "%s.%s%s", dbname, tblname, DATA_FILE_EXTN);
        else
            sprintf(*file_name, "%s%s", tblname, DATA_FILE_EXTN);
    }
}

static void build_schema_file_path(char *dbname, char *tblname, char **file_name)
{
    // 1 for . and 1 for null termination
    int size = strlen(tblname) + SCHEMA_EXTN_LENGTH + 1 + 1;
    if (dbname)
        size += strlen(dbname);

    *file_name = (char *)calloc(1, size);
    if (*file_name != NULL)
    {
        if (dbname)
            sprintf(*file_name, "%s.%s%s", dbname, tblname, DATA_FILE_EXTN);
        else
            sprintf(*file_name, "%s%s", tblname, DATA_FILE_EXTN);
    }
}

static void read_existing_schema(BGD_INSTANCE *bgd_instance, char *dbname,
                char *tname, TableInfo *tinfo)
{
    int error;
    struct stat st;

    LOGIF(LD, (skygw_log_write_flush(LOGFILE_DEBUG, "bgdfilter: %s", __func__)));    

    if (!tinfo->schema_file_path) {
        // Store schema file name
        char *file_path = (char *)calloc(strlen(bgd_instance->path) + 1
                                + strlen(tname) + SCHEMA_EXTN_LENGTH 
                                + 1, sizeof(char));
        sprintf(file_path, "%s/%s.%s.%s", bgd_instance->path, dbname, tname, SCHEMA_FILE_EXTN);

        tinfo->schema_file_path = file_path;
    }

    if (!tinfo->schema) {
        bzero(&st, sizeof(struct stat));
        if (!stat(tinfo->schema_file_path, &st)) {
            error = read_object(tinfo->schema_file_path, OpReadSchema,
                            DataFormatJSON, tinfo->schema);
            if (error) {
                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, 
                    "%s", err_msg_from_code(error))));
            }
        }
    }
}