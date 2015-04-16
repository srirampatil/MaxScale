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

#include "common/routeresolution.h"

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

static ROUTER* createInstance(SERVICE *service, char **options)
{
    GALERA_INSTANCE* inst;

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
		if(strcmp(options[i],"safe_reads") == 0)
		{
		    inst->safe_reads = true;
		}
	    }
	}
    }

    return inst;
}

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
	spinlock_init(&ses->lock);
	
	if((ses->nodes = slist_init()) == NULL)
	{
	    free(ses);
	    skygw_log_write_flush(LOGFILE_ERROR,"Error: Slist initialization failed.");
	    return NULL;
	}

	sref = inst->service->dbref;

	while(sref)
	{
	    if((dcb = dcb_connect(sref->server,
			     session,
			     sref->server->protocol)))
	    {
		slcursor_add_data(ses->nodes,(void*)dcb);
	    }

	    sref = sref->next;
	}

	if(slist_size(ses->nodes) == 0)
	{
	    slist_done(ses->nodes);
	    free(ses);
	    skygw_log_write(LOGFILE_ERROR,"Session creation failed: All attempts to connect to servers failed.");
	    return NULL;
	}

	ses->sescmd = sescmdlist_allocate();
	if(ses->sescmd == NULL)
	{
	    slist_done(ses->nodes);
	    free(ses);
	    skygw_log_write(LOGFILE_ERROR,"Session creation failed: Failed to allocate session command list.");
	}

    }
    return ses;
}

static void closeSession(ROUTER *instance, void *session)
{
    GALERA_SESSION* ses = (GALERA_SESSION*)session;
    if(spinlock_acquire_with_test(&ses->lock,&ses->closed,false))
    {
	slcursor_move_to_begin(ses->nodes);

	do
	{
	    DCB* dcb = slcursor_get_data(ses->nodes);
	    dcb_close(dcb);
	    slcursor_remove_data(ses->nodes);
	}while(slcursor_step_ahead(ses->nodes));

	ses->closed = true;
	spinlock_release(&ses->lock);
    }
}

static void freeSession(ROUTER *instance, void *session)
{
    GALERA_SESSION* ses = (GALERA_SESSION*)session;
    slist_done(ses->nodes);
    free(ses);
}

static int routeQuery(ROUTER *instance, void *session, GWBUF *query)
{
    int i,hash,rval = 0;
    GALERA_SESSION* ses = (GALERA_SESSION*)session;
    GALERA_INSTANCE* inst = (GALERA_INSTANCE*)instance;
    skygw_query_type_t qtype = QUERY_TYPE_UNKNOWN;
    DCB* dcb;
    mysql_server_cmd_t command;
    route_target_t target;
    if(spinlock_acquire_with_test(&ses->lock,&ses->closed,false))
    {
	command = MYSQL_GET_COMMAND(query->start);
	qtype = resolve_query_type(query,command);
	target = get_route_target(qtype,ses->trx_open,false,NULL);

	/** Treat reads as writes, guarantees consistent reads*/

	if(inst->safe_reads)
	    qtype |= QUERY_TYPE_WRITE;

	if(QUERY_IS_TYPE(qtype,QUERY_TYPE_BEGIN_TRX))
	{
	    /** BEGIN statement of a new transaction,
	     * store it and wait for the next query. */

	    if(ses->queue)
		gwbuf_free(ses->queue);

	    ses->trx_open = true;
	    ses->queue = query;
	    rval = 1;
	}
	else if(skygw_is_session_command(query))
	{

	    /** Session command handling */

	    sescmdlist_add_command(ses->sescmd,query);
	    route_sescmd(ses->nodes,query);
	    gwbuf_free(query);

	    if(MYSQL_IS_COM_QUIT(query->start))
		rval = 0;
	}
	else if(ses->trx_open)
	{
	    /** Open transaction in progress */

	    if(ses->active_node == NULL)
	    {
		/** No chosen node yet, hash the query and send it to the server. */

		hash = hash_query_by_table(query,slist_size(ses->nodes));
		dcb = get_dcb_from_hash(ses->nodes,hash);
		ses->active_node = dcb;
		rval = dcb->func.write(dcb,ses->queue);
		ses->queue = query;
	    }
	    else
	    {
		/** We have an active node, route query there. */

		if(SERVER_IS_JOINED(ses->active_node->server))
		{
		    dcb = ses->active_node;
		    rval = dcb->func.write(dcb,query);
		}

		if(QUERY_IS_TYPE(qtype, QUERY_TYPE_COMMIT) ||
		 QUERY_IS_TYPE(qtype, QUERY_TYPE_ROLLBACK))
		{
		    ses->trx_open = false;
		    ses->active_node = NULL;
		}
	    }
	}
	else if (target == TARGET_MASTER || target == TARGET_UNDEFINED)
	{
	    hash = hash_query_by_table(query,slist_size(ses->nodes));
	    dcb = get_dcb_from_hash(ses->nodes,hash);
	    rval = dcb->func.write(dcb,ses->queue);
	}
   
    // If the query is a read and router load balances reads, send to lowest connection count node and return 1

	spinlock_release(&ses->lock);
    }

    return rval;
}

static void diagnostic(ROUTER *instance, DCB *dcb)
{
    // Print information about current number of connections and writes sent to each node
    // Print routing mode, whether reads are load balanced or not
}

static void clientReply(
                        ROUTER* instance,
                        void* router_session,
                        GWBUF* queue,
                        DCB* backend_dcb)
{
    // If a session command was sent, process reply

    // If a queued query was stored, send it to the node

    // Return reply to client if the client is waiting for one
}

static void handleError(ROUTER* instance,
                        void* router_session,
                        GWBUF* errmsgbuf,
                        DCB* backend_dcb,
                        error_action_t action,
                        bool* succp)
{
    // Close connection to failed backend
}

static uint8_t getCapabilities (
                                ROUTER* inst,
                                void* router_session)
{
    return RCAP_TYPE_STMT_INPUT;
}
