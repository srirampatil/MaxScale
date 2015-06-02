/*
 * =====================================================================================
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
 * =====================================================================================
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
static	int	routeQuery(FILTER *instance, void *fsession, GWBUF *queue);
static	void	diagnostic(FILTER *instance, void *fsession, DCB *dcb);


static FILTER_OBJECT MyObject = {
    createInstance,
    newSession,
    closeSession,
    freeSession,
    setDownstream,
    NULL,		// No Upstream requirement
    routeQuery,
    NULL,		// No client reply
    diagnostic,
};


/*
 * =====================================================================================
 *       struct:  BGD_INSTANCE
 *  Description:  bgdfilter instance.
 * =====================================================================================
 */
typedef struct {
    char *format;   /* Storage format JSON or XML (Default: JSON */
    char *path;     /* Path to a folder where to store all data files */
    char *match;    /* Mandatory regex to match against table names */
    
    regex_t re;     /*  Compiled regex text */

} BGD_INSTANCE;


/*
 * =====================================================================================
 *       struct:  BGD_SESSION
 *  Description:  bgdfilter session
 * =====================================================================================
 */
typedef struct {
    DOWNSTREAM down;
    char *current_db;
    int active;
} BGD_SESSION;

/**
 * Implementation of the mandatory version entry point
 *
 * @return version string of the module
 */
char *
version()
{
	return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 */
void
ModuleInit()
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
FILTER_OBJECT *
GetModuleObject()
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));
	return &MyObject;
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  create_dir
 *  Description:  Create directory at path. Return zero if successful else error number.
 * =====================================================================================
 */
static int create_dir(char *path)
{
    struct stat st;

    if(stat(path, &st) == 0)
    {
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
            LOGIF(LD, (skygw_log_write_flush(
				    LOGFILE_DEBUG,
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
 * ===  FUNCTION  ======================================================================
 *         Name:  createInstance
 *  Description:  
 * =====================================================================================
 */
static FILTER *
createInstance(char **options, FILTER_PARAMETER **params)
{
    BGD_INSTANCE * bgd_instance;
    int dir_ret;

    if((bgd_instance = (BGD_INSTANCE *)calloc(1, sizeof(BGD_INSTANCE))) == NULL)
        return NULL;

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
				LOGFILE_DEBUG,
				"bgdfilter: '%s' %s.\n",
                bgd_instance->path,
				strerror(dir_ret))));

        free(bgd_instance->path);
        free(bgd_instance);

        return NULL;
    }

    return (FILTER *)bgd_instance;
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  newSession
 *  Description:  
 * =====================================================================================
 */
static void *
newSession(FILTER *instance, SESSION *session)
{
    BGD_SESSION *bgd_session;
    MYSQL_session *ses_data = (MYSQL_session *)session->data;

    if((bgd_session = (BGD_SESSION *)calloc(1, sizeof(BGD_SESSION))) == NULL)
        return NULL;

    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    
    bgd_session->current_db = strdup(ses_data->db);

    return bgd_session;
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  closeSession
 *  Description:  
 * =====================================================================================
 */
static void 
closeSession(FILTER *instance, void *session)
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    return;
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  freeSession
 *  Description:  
 * =====================================================================================
 */
static void 
freeSession(FILTER *instance, void *session)
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
 * ===  FUNCTION  ======================================================================
 *         Name:  setDownstream
 *  Description:  
 * =====================================================================================
 */
static void	
setDownstream(FILTER *instance, void *fsession, DOWNSTREAM *downstream)
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    BGD_SESSION *bgd_session = (BGD_SESSION *) fsession;
    bgd_session->down = *downstream;
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  routeQuery
 *  Description:  
 * =====================================================================================
 */
static int	
routeQuery(FILTER *instance, void *fsession, GWBUF *queue)
{
    BGD_INSTANCE *bgd_instance = (BGD_INSTANCE *)instance;
    BGD_SESSION *bgd_session = (BGD_SESSION *)fsession;

    int number_of_table_names, number_of_db_names;
    char **table_names, **db_names;
    bool success = false;
    
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    if(modutil_is_SQL(queue))
    {
        if(!query_is_parsed(queue))
            success = parse_query(queue);

        if(!success)
        {
            skygw_log_write(LOGFILE_ERROR,"Error: Parsing query failed.");
            goto end;
        }

        switch(query_classifier_get_operation(queue))
        {
            case QUERY_OP_INSERT:              
                LOGIF(LD, (skygw_log_write_flush(
				            LOGFILE_DEBUG,
    				        "bgdfilter: insert query")));

                /* table_names = skygw_get_table_names(queue, &number_of_table_names, TRUE);
                db_names = skygw_get_database_names(queue, &number_of_db_names);

                if(number_of_db_names > 0)
                    fprintf(bgd_instance->fp, "%s\n", db_names[0]);
                else
                    fprintf(bgd_instance->fp, "%s\n", table_names[0]);

                fflush(bgd_instance->fp);
                   */
                break;
        }
    } 

end:
    return bgd_session->down.routeQuery(bgd_session->down.instance, 
                    bgd_session->down.session, queue);
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  diagnostic
 *  Description:  
 * =====================================================================================
 */
static void	
diagnostic(FILTER *instance, void *fsession, DCB *dcb)
{
    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));
    return;
}
