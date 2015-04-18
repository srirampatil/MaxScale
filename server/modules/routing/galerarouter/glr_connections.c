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

slist_cursor_t* refresh_nodes(GALERA_SESSION* session, SERVER_REF* servers, slist_cursor_t* oldconn)
{
    slist_cursor_t* newconn;
    SERVER_REF* sref;
    DCB* dcb;

    if((newconn = slist_init()) == NULL)
	{
	    skygw_log_write_flush(LOGFILE_ERROR,"Error: Slist initialization failed.");
	    return NULL;
	}

	sref = servers;

	while(sref)
	{
	    if(SERVER_IS_JOINED(sref->server)
		&& (dcb = dcb_connect(
			sref->server,
			session,
			sref->server->protocol)))
	    {
		slcursor_add_data(session->nodes,(void*)dcb);
	    }
	    sref = sref->next;
	}

	if(slist_size(newconn) == 0)
	{
	    slist_done(newconn);
	    skygw_log_write(LOGFILE_ERROR,"Session creation failed: All attempts to connect to servers failed.");
	    return NULL;
	}

    return 0;
}
