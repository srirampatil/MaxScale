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

/** Defined in log_manager.cc */
extern int            lm_enabled_logfiles_bitmask;
extern size_t         log_ses_count[];
extern __thread log_info_t tls_log_info;

MODULE_INFO 	info = {
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
 *       struct:  BGD_INSTANCE
 *  Description:  bgdfilter instance.
 */

typedef struct bgd_instance BGD_INSTANCE;

struct bgd_instance {
    char *format;   /* Storage format JSON or XML (Default: JSON */
    char *path;     /* Path to a folder where to store all data files */
    char *match;    /* Mandatory regex to match against table names */
    
    regex_t re;     /*  Compiled regex text */

    HASHTABLE *fp_htable;   /* Hash table mapping from file name to file pointer */

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
    char *current_db;
    int active;
};

/*
 * Hash function for fp_htable in BGD_INSTANCE
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
 * Comparison function for fp_htable in BGD_INSTANCE
 *
 * @param key1  first null-terminated string to be compared
 * @param key2  second null-terminated string to be compared
 * @return zero if equal, non-zero otherwise
 */
static int fp_cmpfn(void *key1, void *key2)
{
    return strcmp((char *)key1, (char *)key2);
}

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
 * Opens a file given folder and file names
 *
 * @param folder_path   parent forlder of the file
 * @param file_name     name of the file
 * @param mode          mode in which the file should be opened
 */
static FILE *open_file(char *folder_path, char *file_name, char *mode)
{
    char *file_path = (char *)calloc(1, strlen(folder_path) + strlen(file_name)
            + 1 /* for separator / */
            + 1 /* for null termination */
            );

    sprintf(file_path, "%s/%s", folder_path, file_name);
    FILE *fp = fopen(file_path, mode);

    free(file_path);
    return fp;
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
    int number_of_table_names;
    char *file_name = NULL;
    char **table_names;

    LOGIF(LD, (skygw_log_write_flush(
                    LOGFILE_DEBUG, 
                    "bgdfilter: %s.\n", __func__)));

    table_names = skygw_get_table_names(queue, &number_of_table_names, TRUE);
    if(number_of_table_names == 0)
        return;

    file_name = (char *)calloc(1, strlen(bgd_session->current_db)
                    + strlen(table_names[0]) + 4 /* for "data" */
                    + 2 /* for two . */
                    + 1 /* for null termination */
                    );

    sprintf(file_name, "%s.%s.data", bgd_session->current_db, 
            table_names[0]);

    FILE *fp = hashtable_fetch(bgd_instance->fp_htable, file_name);
    if(fp == NULL)
    {
        fp = open_file(bgd_instance->path, file_name, "a");
        if(fp == NULL)
        {
            int err = errno;
            LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, 
                            "bgdfilter: Cannot open '%s/%s': %s",
                            bgd_instance->path, file_name, strerror(err))));

            goto log_insert_data_end;
        }

        hashtable_add(bgd_instance->fp_htable, file_name, fp);
    }

    if(fprintf(fp, "%s\n", modutil_get_SQL(queue)) < 0)
    {
        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, 
                        "bgdfilter: Cannot write to '%s/%s'",
                        bgd_instance->path, file_name)));

        goto log_insert_data_end;
    }

    fflush(fp);

log_insert_data_end:
    if(file_name != NULL)
        free(file_name);
}

/* 
 *         Name:  createInstance
 *  Description:  
 */
static FILTER *createInstance(char **options, FILTER_PARAMETER **params)
{
    BGD_INSTANCE * bgd_instance;
    HASHTABLE *ht;
    int dir_ret;

    if((bgd_instance = (BGD_INSTANCE *)calloc(1, sizeof(BGD_INSTANCE))) == NULL)
        return NULL;

    // Hash table initialization
    if((ht = hashtable_alloc(100, fp_hashfn, fp_cmpfn)) == NULL)
    {
        LOGIF(LE, (skygw_log_write_flush(
				LOGFILE_ERROR,
				"bgdfilter: Could not allocate hashtable")));
        free(bgd_instance);
        return NULL;
    }

    hashtable_memory_fns(ht, (HASHMEMORYFN)strdup, NULL, (HASHMEMORYFN)free, NULL);

    bgd_instance->fp_htable = ht;
    bgd_instance->format = NULL;
    bgd_instance->path = NULL;
    bgd_instance->match = NULL;

    if(options)
        bgd_instance->path = strdup(options[0]);
    else
        bgd_instance->path = strdup("/tmp/bgd");

    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    if((dir_ret = create_dir(bgd_instance->path)) != 0)
    {
        LOGIF(LE, (skygw_log_write_flush(
				LOGFILE_ERROR,
				"bgdfilter: '%s' %s.\n",
                bgd_instance->path,
				strerror(dir_ret))));

        free(bgd_instance->path);
        free(bgd_instance);

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

    bgd_session->query_buf = NULL;
    bgd_session->current_db = strdup(ses_data->db);

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

    BGD_SESSION *bgd_session = (BGD_SESSION *)session;
    if(bgd_session->current_db != NULL)
        free(bgd_session->current_db);

    free(bgd_session);
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

    bgd_session->query_buf = gwbuf_clone(queue);
    
    return bgd_session->down.routeQuery(bgd_session->down.instance, 
                    bgd_session->down.session, queue);
}


static int 
clientReply(FILTER *instance, void *fsession, GWBUF *reply)
{
    BGD_INSTANCE *bgd_instance = (BGD_INSTANCE *)instance;
    BGD_SESSION *bgd_session = (BGD_SESSION *)fsession;
    bool success = false;
    GWBUF *queue = bgd_session->query_buf;

    unsigned char *ptr = (unsigned char *)reply->start;

    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__))); 

    if(PTR_IS_ERR(ptr) || bgd_session->query_buf == NULL || !PTR_IS_OK(ptr))
        goto client_reply_end;

    if(modutil_is_SQL(queue))
    {
        if(!query_is_parsed(queue))
            success = parse_query(queue);

        if(!success)
        {
            skygw_log_write(LOGFILE_ERROR,"Error: Parsing query failed.");
            goto client_reply_end;
        }

        switch(query_classifier_get_operation(queue))
        {
        case QUERY_OP_INSERT:              
            log_insert_data(bgd_instance, bgd_session, queue);
            break;
        }
    }

client_reply_end:
    
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
