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

    FILE *fp;
    char *filebase;

} BGD_INSTANCE;


/*
 * =====================================================================================
 *       struct:  BGD_SESSION
 *  Description:  bgdfilter session
 * =====================================================================================
 */
typedef struct {
    DOWNSTREAM down;
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
 * ===  FUNCTION  ===============================LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				skygw_get_canonical(queue))));=======================================
 *         Name:  createInstance
 *  Description:  
 * =====================================================================================
 */
static FILTER *
createInstance(char **options, FILTER_PARAMETER **params)
{
    BGD_INSTANCE * bgd_instance;

    if((bgd_instance = (BGD_INSTANCE *)calloc(1, sizeof(BGD_INSTANCE))) == NULL)
        return NULL;

    bgd_instance->format = NULL;
    bgd_instance->path = NULL;
    bgd_instance->match = NULL;
    bgd_instance->filebase = NULL;
    bgd_instance->fp = NULL;

    if(options)
        bgd_instance->filebase = strdup(options[0]);
    else
        bgd_instance->filebase = strdup("/tmp/bgd");

    bgd_instance->fp = fopen(bgd_instance->filebase, "w+");

    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

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

    if((bgd_session = (BGD_SESSION *)calloc(1, sizeof(BGD_SESSION))) == NULL)
        return NULL;

    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

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

    BGD_INSTANCE *bgd_instance = (BGD_INSTANCE *)instance;
    if(bgd_instance->fp != NULL)
        fclose(bgd_instance->fp);

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

    free(session);
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
    
    parse_query(queue);

    LOGIF(LD, (skygw_log_write_flush(
				LOGFILE_DEBUG,
				"bgdfilter: %s.\n",
				__func__)));

    if(modutil_is_SQL(queue))
    {
        if(query_classifier_get_operation(queue) == QUERY_OP_INSERT)
        {
            LOGIF(LD, (skygw_log_write_flush(
				        LOGFILE_DEBUG,
				        "bgdfilter: insert query")));

            fprintf(bgd_instance->fp, "%s\n", modutil_get_SQL(queue));
            fflush(bgd_instance->fp);
        }
        else
        {
            LOGIF(LD, (skygw_log_write_flush(
				        LOGFILE_DEBUG,
				        "bgdfilter: non-insert query")));
        }
    } 

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
