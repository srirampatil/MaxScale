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
 * Route a query to each node in the slist. This function assumes that the data
 * held in the slist contains pointers to DCBs each with an open connection to
 * a server. The buffer is cloned for each node so the caller should free the
 * original buffer.
 * @param cursor List of DCBs to use
 * @param buffer Buffer to route
 * @return Number of sent queries
 */
int route_sescmd(slist_cursor_t* cursor,GWBUF* buffer)
{
    DCB *dcb;
    GWBUF* clone;
    int rval = 0;

    slcursor_move_to_begin(cursor);

    do{
	dcb = slcursor_get_data(cursor);
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
    }while(slcursor_step_ahead(cursor));

    return rval;
}

/**
 * Return the DCB associated with the node number.
 * @param cursor slist with DCBs as data
 * @param node Number of the node
 * @return Pointer to the DCB at position of node
 */
DCB* get_dcb_from_hash(slist_cursor_t* cursor, int node)
{
    int i;
    
    slcursor_move_to_begin(cursor);

    for(i = 0;i < node;i++)
	slcursor_step_ahead(cursor);

    return (DCB*)slcursor_get_data(cursor);
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
