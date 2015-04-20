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

/**
 * @file glr_routing.c Galera router routing functions
 *
 * This file contains functions used for routing queries to backend servers.
 *
 * @verbatim
 * Revision History
 *
 * Date			Who				Description
 * 07/04/2015	Markus Makela	Initial implementation
 *
 * @endverbatim
 */

#include <galerarouter.h>
#include <stdlib.h>
#include <skygw_utils.h>
#include <query_classifier.h>
#include <strings.h>
#include <modutil.h>
#include <common/routeresolution.h>

/** This includes the log manager thread local variables */
LOG_MANAGER_TLS

/**
 * Route a query to each node in the array. This function assumes that the data
 * held in the array contains pointers to DCBs each with an open connection to
 * a server. The buffer is cloned for each node so the caller should free the
 * original buffer.
 * @param cursor List of DCBs to use
 * @param buffer Buffer to route
 * @return Number of sent queries
 */
int route_sescmd(ARRAY* array,GWBUF* buffer)
{
    DCB *dcb;
    GWBUF* clone;
    int rval = 0;
    unsigned int i,sz;

    sz = array_size(array);

    for(i = 0;i<sz;i++)
    {
	dcb = array_fetch(array,i);
	SCMDCURSOR* cursor = dcb_get_sescmdcursor(dcb);

	if(SERVER_IS_JOINED(dcb->server))
	{
	    if(!sescmdlist_execute(cursor))
	    {
		skygw_log_write(LOGFILE_ERROR,"Error: Failed to write session command to '%s'.",
			 dcb->server->name);
	    }
	    else
	    {
		rval++;
	    }
	}
    }

    return rval;
}

/**
 * Handle an incoming query.
 * Resolve the type of the query and route it to the appropriate node. Queries
 * that modify data are routed to a node based on the output of the hashing
 * function. Read-only queries can be either load balanced across all nodes or
 * they can be routed to a node based on the output of the hashing function. This
 * is controlled by the 'safe_reads' router option.
 * @param inst Router instance
 * @param ses Router session
 * @param query GWBUF with the query to handle
 * @return 1 on success, 0 on failure. All failures will trigger the closing
 * of the session.
 */
int handle_query(GALERA_INSTANCE* inst,GALERA_SESSION* ses,GWBUF *query)
{
    int hash,rval = 0;
    skygw_query_type_t qtype = QUERY_TYPE_UNKNOWN;
    DCB* dcb;
    mysql_server_cmd_t command;
    route_target_t target;
    command = MYSQL_GET_COMMAND(((unsigned char*)query->start));
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

	if(MYSQL_IS_COM_QUIT(((unsigned char*)query->start)))
	    rval = 0;
    }
    else if(ses->trx_open)
    {
	/** Open transaction in progress */

	if(ses->active_node == NULL)
	{
	    /** No chosen node yet, hash the query and send it to the server. */

	    hash = hash_query_by_table(query,array_size(ses->nodes));
	    dcb = array_fetch(ses->nodes,hash);
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
	hash = hash_query_by_table(query,array_size(ses->nodes));
	dcb = array_fetch(ses->nodes,hash);
	rval = dcb->func.write(dcb,ses->queue);
    }

    // If the query is a read and router load balances reads, send to lowest connection count node and return 1
    return rval;
}

#if BUILD_TOOLS

#include <modutil.h>
#include <getopt.h>

struct option longopt[] =
{
    {"help",no_argument,0,'?'}
};

static char* server_options[] = {
    "MariaDB Corporation MaxScale",
    "--no-defaults",
    "--datadir=.",
    "--language=.",
    "--skip-innodb",
    "--default-storage-engine=myisam",
    NULL
};

const int num_elements = (sizeof(server_options) / sizeof(char *)) - 1;

static char* server_groups[] = {
    "embedded",
    "server",
    "server",
    NULL
};

int main(int argc,char** argv)
{
    int printhelp = 0;
    char c;
    int longind = 0;

    while((c = getopt_long(argc,argv,"h?",longopt,&longind)) != -1)
    {
	switch(c)
	{
	case 'h':
	case '?':
	    printhelp = 1;
	    break;
	}
    }

    if(argc < 3 || printhelp)
    {
	printf("Usage: %s QUERY NODES\n\n"
		"Parses the QUERY and prints to which node it would be routed to\n"
		"by the Galera router. "
		"The NODES parameter is the number of galera nodes\n\n",argv[0]);
	return 1;
    }

    if(mysql_library_init(num_elements, server_options, server_groups))
    {
	printf("Error: Cannot initialize embedded mysqld library.\n");
	return 1;
    }

    GWBUF* buffer = modutil_create_query(argv[1]);
    int hval = hash_query_by_table(buffer,atoi(argv[2]));
    printf("Query routed to node %d\n",hval);
    gwbuf_free(buffer);
    return 0;
}
#endif
