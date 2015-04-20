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
 * @file glr.c	The Galera router entry points
 *
 * This file contains error hadling functions for the Galera router.
 *
 * @verbatim
 * Revision History
 *
 * Date			Who				Description
 * 09/04/2015	Markus Makela	Initial implementation
 * 
 * @endverbatim
 */

#include <galerarouter.h>

/**
 *
 * @param session
 * @param servers
 * @param oldconn
 * @return
 */
int refresh_nodes(GALERA_SESSION* session, SERVER_REF* servers)
{
    ARRAY* array;
    SERVER_REF* sref;
    DCB* dcb;
    unsigned int i,j;
    int oknodes = 0;

    sref = servers;
    array = session->nodes;
    j = array_size(array);

    while(sref)
    {
	if(SERVER_IS_JOINED(sref->server))
	{
	    bool no_conn = true;


	    for(i = 0;i<j;i++)
	    {
		dcb = array_fetch(array,i);
		if(dcb->server == sref->server)
		{
		    no_conn = false;
		    oknodes++;
		    break;
		}
	    }

	    if(no_conn)
	    {
		dcb = dcb_connect(sref->server,
				 session->session,
				 sref->server->protocol);
		if(dcb)
		{
		    array_push(array,(void*)dcb);
		    oknodes++;
		}
	    }
	}
	
	sref = sref->next;
    }

    if(oknodes == 0)
    {
	skygw_log_write(LOGFILE_ERROR,"Error: All attempts to connect to servers failed, closing Galerarouter session.");
    }

    return oknodes;
}
