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
	
	if((ses->nodes = slist_init()))
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

	ses->closed = false;
    }
    return ses;
}

static void closeSession(ROUTER *instance, void *session)
{
    GALERA_SESSION* ses = session;

    slcursor_move_to_begin(ses->nodes);

    // Add checks for closed router session

    while(slcursor_step_ahead(ses->nodes))
    {
	DCB* dcb = slcursor_get_data(ses->nodes);
	dcb_close(dcb);
    }

    ses->closed = true;
}

static void freeSession(ROUTER *instance, void *session)
{
    GALERA_SESSION* ses = session;
    slist_done(ses->nodes);
    free(ses);
}

static int routeQuery(ROUTER *instance, void *session, GWBUF *queue)
{
    // Check if the session is closed, if so, return 0

    // Resolve query type, is it a read or a write

    // If query is BEGIN, store it for later, return 1

    // If the query is a read and router load balances reads, send to lowest connection count node and return 1

    // If the query is a write or the router doesn't load balance reads, get the name of the database and the table
    // and feed them into the hashing algorithm

    // Based on the output of the hashing function and the number of synced nodes, send the query to a node, return 1

    // If the query is a session command, send to all nodes and return 1
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
    // Return requirement for statement based routing
}
