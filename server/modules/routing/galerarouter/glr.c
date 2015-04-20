/*
 * This file is distributed as part of the MariaDB Corporation MaxScale.  It is free
 * software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation,
 * version 2.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Copyright MariaDB Corporation Ab 2015
 */

#include <router.h>
#include <galerarouter.h>
#include <query_classifier.h>
#include <common/routeresolution.h>
#include <modutil.h>

/** This includes the log manager thread local variables */
LOG_MANAGER_TLS

MODULE_INFO 	info = {
	MODULE_API_ROUTER,
	MODULE_GA,
	ROUTER_VERSION,
	"Conflict avoiding Galera router"
};

/** Defined in log_manager.cc */
extern int            lm_enabled_logfiles_bitmask;
extern size_t         log_ses_count[];
extern __thread log_info_t tls_log_info;

/**
 * @file glr.c	The Galera router entry points
 *
 * This file contains the entry points defined in the router interface for the Galera router.
 *
 * @verbatim
 * Revision History
 *
 * Date			Who				Description
 * 07/04/2015	Markus Makela	Initial implementation
 * 
 * @endverbatim
 */

static char *version_str = "V1.0.0";

static ROUTER* createInstance(SERVICE *service, char **options);
static void* newSession(ROUTER *instance, SESSION *session);
static void closeSession(ROUTER *instance, void *session);
static void freeSession(ROUTER *instance, void *session);
static int routeQuery(ROUTER *instance, void *session, GWBUF *queue);
static void diagnostic(ROUTER *instance, DCB *dcb);

static void clientReply(ROUTER* instance,
                        void* router_session,
                        GWBUF* queue,
                        DCB* backend_dcb);

static  void handleError(ROUTER* instance,
                         void* router_session,
                         GWBUF* errmsgbuf,
                         DCB* backend_dcb,
                         error_action_t action,
                         bool* succp);
static uint8_t getCapabilities (ROUTER* inst,
                                void* router_session);
int handle_query(GALERA_INSTANCE* inst,GALERA_SESSION* ses,GWBUF *query);
int retry_commit(GALERA_SESSION* ses,DCB* dcb);
int refresh_nodes(GALERA_SESSION* session, SERVER_REF* servers);
static ROUTER_OBJECT MyObject = {
    createInstance,
    newSession,
    closeSession,
    freeSession,
    routeQuery,
    diagnostic,
    clientReply,
    handleError,
    getCapabilities
};


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
 * The module initialization routine, called when the module
 * is first loaded.
 */
void
ModuleInit()
{
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
ROUTER_OBJECT* GetModuleObject()
{
        return &MyObject;
}

/**
 * Create a new router instance.
 * @param service Service owner of the instance
 * @param options Router options
 * @return Pointer to router instance or NULL if an error occurred.
 */
static ROUTER* createInstance(SERVICE *service, char **options)
{
    GALERA_INSTANCE* inst;
    char* param;
    char* val;
    char* saveptr;

    if((inst = malloc(sizeof(GALERA_INSTANCE))))
    {
	inst->service = service;
	inst->safe_reads = false;
	spinlock_init(&inst->lock);

	if(options)
	{
	    int i;
	    for(i = 0;options[i];i++)
	    {
		param = strtok_r(options[i]," =",&saveptr);
		val = strtok_r(NULL," =",&saveptr);
		if(strcmp(param,"safe_reads") == 0 && val)
		{
		    if(config_truth_value(val))
			inst->safe_reads = true;
		    else
			inst->safe_reads = false;
		}
		else if(strcmp(param,"max_retries") == 0 && val)
		{
		    inst->max_retries = atoi(val);
		}
		else
		{
		    skygw_log_write(LE,"Error: Unexpected parameter %s%s%s.",
			     param,val?"=":"",val?val:"");
		    free(inst);
		    return NULL;
		}
	    }
	}
    }

    return (ROUTER*)inst;
}

/**
 * Start a new session for the Galera router. This function allocates memory for
 * the router session and connects to all nodes.
 * @param instance Router instance
 * @param session The client session
 * @return Pointer to the new session or NULL if an error occurred.
 */
static void* newSession(ROUTER *instance, SESSION *session)
{
    GALERA_INSTANCE* inst = (GALERA_INSTANCE*)instance;
    GALERA_SESSION* ses = NULL;
    SERVER_REF* sref;
    DCB* dcb;

    if((ses = (GALERA_SESSION*)malloc(sizeof(GALERA_SESSION))) != NULL)
    {
	ses->autocommit = true;
	ses->trx_open = false;
	ses->active_node = NULL;
	ses->queue = NULL;
	ses->closed = false;
	ses->session = session;
	ses->conflict.n_retries = 0;
	ses->conflict.max_retries = inst->max_retries;
	spinlock_init(&ses->lock);
	
	if((ses->nodes = array_init()) == NULL)
	{
	    free(ses);
	    skygw_log_write_flush(LOGFILE_ERROR,"Error: Array initialization failed.");
	    return NULL;
	}

	if(refresh_nodes(ses,inst->service->dbref) == 0)
	{
	    array_free(ses->nodes);
	    free(ses);
	    skygw_log_write(LOGFILE_ERROR,"Session creation failed: Failed to connect to any nodes.");
	    return NULL;
	}

	ses->sescmd = sescmdlist_allocate();
	if(ses->sescmd == NULL)
	{
	    array_free(ses->nodes);
	    free(ses);
	    skygw_log_write(LOGFILE_ERROR,"Session creation failed: Failed to allocate session command list.");
	    return NULL;
	}


	LOGIF(LT,(skygw_log_write(LT,"Started Galerarouter session.")));
    }
    return ses;
}

static void closeSession(ROUTER *instance, void *session)
{
    GALERA_SESSION* ses = (GALERA_SESSION*)session;
    int i,sz;

    if(spinlock_acquire_with_test(&ses->lock,&ses->closed,false))
    {
	sz = array_size(ses->nodes);
	for(i = 0;i<sz;i++)
	{
	    DCB* dcb = array_fetch(ses->nodes,i);
	    dcb_close(dcb);
	}

	ses->closed = true;
	spinlock_release(&ses->lock);
    }
}

static void freeSession(ROUTER *instance, void *session)
{
    GALERA_SESSION* ses = (GALERA_SESSION*)session;
    array_free(ses->nodes);
    free(ses);
}

/**
 * Route a query to a backend server or to all backend servers if it is a query
 * that modifies the session state.
 * @param instance Router instance
 * @param session Router session
 * @param query Query to route
 * @return 1 on success, 0 on failure.
 */
static int routeQuery(ROUTER *instance, void *session, GWBUF *query)
{
    int rval = 0;
    GALERA_SESSION* ses = (GALERA_SESSION*)session;
    GALERA_INSTANCE* inst = (GALERA_INSTANCE*)instance;

    if(spinlock_acquire_with_test(&ses->lock,&ses->closed,false))
    {
	ses->conflict.n_retries = 0;
	rval = handle_query(inst,ses,query);
	spinlock_release(&ses->lock);
    }

    return rval;
}

static void diagnostic(ROUTER *instance, DCB *dcb)
{
    // Print information about current number of connections and writes sent to each node
    // Print routing mode, whether reads are load balanced or not
    dcb_printf(dcb,"Galera router\n");
}

static void clientReply(ROUTER* instance,
                        void* router_session,
                        GWBUF* queue,
                        DCB* backend_dcb)
{
    GALERA_SESSION* ses = (GALERA_SESSION*)router_session;
    GALERA_INSTANCE* inst = (GALERA_INSTANCE*)instance;
    SCMDCURSOR* cursor;
    
    /** If a session command was sent, process reply */
    if(spinlock_acquire_with_test(&ses->lock,&ses->closed,false))
    {
	if(GWBUF_IS_TYPE_SESCMD_RESPONSE(queue))
	{
	    cursor = dcb_get_sescmdcursor(backend_dcb);
	    if(sescmdlist_process_replies(cursor,&queue))
	    {
		sescmdlist_execute(cursor);
	    }
	}

	/** If a queued query was stored, route it */
	if(ses->queue)
	{
	    GWBUF* tmp = ses->queue;
	    ses->queue = NULL;
	    if(handle_query(inst,ses,tmp) == 0)
	    {
		LOGIF(LT,(skygw_log_write(LT,"Failed to route queued query.")));
	    }
	}

	/** Return reply to client if the client is waiting for one */
	if(queue)
	{
	    SESSION_ROUTE_REPLY(ses->session,queue);
	}

	spinlock_release(&ses->lock);
    }
}

static void handleError(ROUTER* instance,
                        void* router_session,
                        GWBUF* errmsgbuf,
                        DCB* backend_dcb,
                        error_action_t action,
                        bool* succp)
{
    GALERA_INSTANCE* inst = (GALERA_INSTANCE*)instance;
    GALERA_SESSION* ses = (GALERA_SESSION*)router_session;
    int rval = 0;
    unsigned int i,sz;
    if(action == ERRACT_RESET)
    {
	backend_dcb->dcb_errhandle_called = false;
	return;
    }

    if(spinlock_acquire_with_test(&ses->lock,&ses->closed,false))
    {
	if(!backend_dcb->dcb_errhandle_called)
	{

	    backend_dcb->dcb_errhandle_called = true;

	    switch(action)
	    {
	    case ERRACT_NEW_CONNECTION:

		/** node down, close DCB and reassign nodes*/
		sz = array_size(ses->nodes);

		for(i = 0;i<sz;i++)
		{
		    if(array_fetch(ses->nodes,i) == backend_dcb)
		    {
			array_delete(ses->nodes,i);
			dcb_close(backend_dcb);
			refresh_nodes(ses,inst->service->dbref);
			break;
		    }
		}

		break;

	    case ERRACT_REPLY_CLIENT:

		if(*((unsigned short*)(GWBUF_DATA(errmsgbuf) + 5)) == MYSQL_ER_LOCK_DEADLOCK &&
		 ses->conflict.n_retries < ses->conflict.max_retries)
		{
		    if((rval = retry_commit(ses,backend_dcb)))
		    {
			LOGIF(LT,(skygw_log_write(LT,"Commit retry number %d",ses->conflict.n_retries)));
			backend_dcb->dcb_errhandle_called = false;
		    }
		}

		if(rval == 0)
		{
		    backend_dcb->func.write(backend_dcb,errmsgbuf);
		}
		break;

	    default:
		break;
	    }
	}
	spinlock_release(&ses->lock);
    }
}

static uint8_t getCapabilities (
                                ROUTER* inst,
                                void* router_session)
{
    return RCAP_TYPE_STMT_INPUT;
}
