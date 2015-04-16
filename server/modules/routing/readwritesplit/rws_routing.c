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
 * Copyright MariaDB Corporation Ab 2013-2014
 */

#include <readwritesplitsession.h>
#include <readwritesplit2.h>
#include <query_classifier.h>
#include <modutil.h>
#include <sescmd.h>
#include <common/routeresolution.h>
/** Defined in log_manager.cc */
extern int            lm_enabled_logfiles_bitmask;
extern size_t         log_ses_count[];
extern __thread       log_info_t tls_log_info;

int hashkeyfun(void* key);
int hashcmpfun (void *, void *);
void* hstrdup(void* fval);
void* hfree(void* fval);
bool get_dcb(
        DCB**              p_dcb,
        ROUTER_CLIENT_SES* rses,
        backend_type_t     btype,
        char*              name,
        int                max_rlag);

bool route_session_write(
        ROUTER_CLIENT_SES* router_client_ses,
        GWBUF*             querybuf,
        ROUTER_INSTANCE*   inst,
        unsigned char      packet_type,
        skygw_query_type_t qtype);

void rses_end_locked_router_action(
        ROUTER_CLIENT_SES* rses);
backend_ref_t* get_bref_from_dcb(ROUTER_CLIENT_SES* rses, DCB* dcb);
backend_ref_t* get_root_master_bref(ROUTER_CLIENT_SES* rses);
int  rses_get_max_replication_lag(ROUTER_CLIENT_SES* rses);
int  rses_get_max_slavecount(ROUTER_CLIENT_SES* rses, int router_nservers);
void bref_clear_state(backend_ref_t* bref, bref_state_t state);
void bref_set_state(backend_ref_t*   bref, bref_state_t state);
void print_error_packet(
        ROUTER_CLIENT_SES* rses, 
        GWBUF*             buf, 
        DCB*               dcb);

/**
 * Examine the query type, transaction state and routing hints. Find out the
 * target for query routing.
 * 
 *  @param qtype      Type of query 
 *  @param trx_active Is transacation active or not
 *  @param hint       Pointer to list of hints attached to the query buffer
 * 
 *  @return bitfield including the routing target, or the target server name 
 *          if the query would otherwise be routed to slave.
 */

/**
 * Routing function. Find out query type, backend type, and target DCB(s). 
 * Then route query to found target(s).
 * @param inst		router instance
 * @param rses		router session
 * @param querybuf	GWBUF including the query
 * 
 * @return true if routing succeed or if it failed due to unsupported query.
 * false if backend failure was encountered.
 */
bool route_single_stmt(
	ROUTER_INSTANCE*   inst,
	ROUTER_CLIENT_SES* rses,
	GWBUF*             querybuf)
{
	skygw_query_type_t qtype          = QUERY_TYPE_UNKNOWN;
	mysql_server_cmd_t packet_type;
	uint8_t*           packet;
	MYSQL_session*	   data;
	int                ret            = 0;
	DCB*               master_dcb     = NULL;
	DCB*               target_dcb     = NULL;
	route_target_t     route_target;
	bool           	   succp          = false;
	int                rlag_max       = MAX_RLAG_UNDEFINED;
	backend_type_t     btype; /*< target backend type */
	char* dbname;
	
	
	ss_dassert(!GWBUF_IS_TYPE_UNDEFINED(querybuf));
	packet = GWBUF_DATA(querybuf);
	packet_type = packet[4];

	/** 
	 * Read stored master DCB pointer. If master is not set, routing must 
	 * be aborted 
	 */
	if ((master_dcb = rses->rses_master_ref->bref_dcb) == NULL)
	{
		char* query_str = modutil_get_query(querybuf);
		CHK_DCB(master_dcb);
		LOGIF(LE, (skygw_log_write_flush(
			LOGFILE_ERROR,
			"Error: Can't route %s:%s:\"%s\" to "
			"backend server. Session doesn't have a Master "
			"node",
			STRPACKETTYPE(packet_type),
			STRQTYPE(qtype),
			(query_str == NULL ? "(empty)" : query_str))));
		free(query_str);
		succp = false;
		goto retblock;
	}

	data = (MYSQL_session*)master_dcb->session->data;
	if(data)
	    dbname = (char*)data->db;
	else
	    dbname = NULL;

	/** If buffer is not contiguous, make it such */
	if (querybuf->next != NULL)
	{
		querybuf = gwbuf_make_contiguous(querybuf);
	}

    qtype = resolve_query_type(querybuf,packet_type);

	/**
	 * Check if the query has anything to do with temporary tables.
	 */

	qtype = tmptable_parse(rses->rses_tmptable,dbname,querybuf,qtype);
	if(QUERY_IS_TYPE(qtype,QUERY_TYPE_READ_TMP_TABLE))
	    qtype |= QUERY_TYPE_WRITE;
	/**
	 * If autocommit is disabled or transaction is explicitly started
	 * transaction becomes active and master gets all statements until
	 * transaction is committed and autocommit is enabled again.
	 */
	if (rses->rses_autocommit_enabled &&
		QUERY_IS_TYPE(qtype, QUERY_TYPE_DISABLE_AUTOCOMMIT))
	{
		rses->rses_autocommit_enabled = false;
		
		if (!rses->rses_transaction_active)
		{
			rses->rses_transaction_active = true;
		}
	}
	else if (!rses->rses_transaction_active &&
		QUERY_IS_TYPE(qtype, QUERY_TYPE_BEGIN_TRX))
	{
		rses->rses_transaction_active = true;
	}
	/** 
	 * Explicit COMMIT and ROLLBACK, implicit COMMIT.
	 */
	if (rses->rses_autocommit_enabled &&
		rses->rses_transaction_active &&
		(QUERY_IS_TYPE(qtype,QUERY_TYPE_COMMIT) ||
		QUERY_IS_TYPE(qtype,QUERY_TYPE_ROLLBACK)))
	{
		rses->rses_transaction_active = false;
	} 
	else if (!rses->rses_autocommit_enabled &&
		QUERY_IS_TYPE(qtype, QUERY_TYPE_ENABLE_AUTOCOMMIT))
	{
		rses->rses_autocommit_enabled = true;
		rses->rses_transaction_active = false;
	}        
	
	if (LOG_IS_ENABLED(LOGFILE_TRACE))
	{
		uint8_t*      packet = GWBUF_DATA(querybuf);
		unsigned char ptype = packet[4];
		size_t        len = MIN(GWBUF_LENGTH(querybuf), 
					MYSQL_GET_PACKET_LEN((unsigned char *)querybuf->start)-1);
		char*         data = (char*)&packet[5];
		char*         contentstr = strndup(data, len);
		char*         qtypestr = skygw_get_qtype_str(qtype);
		
		skygw_log_write(
			LOGFILE_TRACE,
				"> Autocommit: %s, trx is %s, cmd: %s, type: %s, "
				"stmt: %s%s %s",
				(rses->rses_autocommit_enabled ? "[enabled]" : "[disabled]"),
				(rses->rses_transaction_active ? "[open]" : "[not open]"),
				STRPACKETTYPE(ptype),
				(qtypestr==NULL ? "N/A" : qtypestr),
				contentstr,
				(querybuf->hint == NULL ? "" : ", Hint:"),
				(querybuf->hint == NULL ? "" : STRHINTTYPE(querybuf->hint->type)));
		
		free(contentstr);
		free(qtypestr);
	}
	/** 
	 * Find out where to route the query. Result may not be clear; it is 
	 * possible to have a hint for routing to a named server which can
	 * be either slave or master. 
	 * If query would otherwise be routed to slave then the hint determines 
	 * actual target server if it exists.
	 * 
	 * route_target is a bitfield and may include :
	 * TARGET_ALL
	 * - route to all connected backend servers
	 * TARGET_SLAVE[|TARGET_NAMED_SERVER|TARGET_RLAG_MAX]
	 * - route primarily according to hints, then to slave and if those
	 *   failed, eventually to master
	 * TARGET_MASTER[|TARGET_NAMED_SERVER|TARGET_RLAG_MAX]
	 * - route primarily according to the hints and if they failed, 
	 *   eventually to master
	 */
	route_target = get_route_target(qtype, 
					rses->rses_transaction_active,
					rses->rses_config.rw_use_sql_variables_in == TYPE_MASTER,
					querybuf->hint);
	
	if(skygw_is_session_command(querybuf))
	{
	    succp = route_session_write(
					rses, 
					gwbuf_clone(querybuf), 
					inst, 
					packet_type, 
					qtype);
		
		if (succp)
		{
			atomic_add(&inst->stats.n_all, 1);
		}
		goto retblock;
	}
	
	
	/** Lock router session */
	if (!spinlock_acquire_with_test(&rses->rses_lock, &rses->rses_closed, FALSE))
	{
		if (packet_type != MYSQL_COM_QUIT)
		{
			char* query_str = modutil_get_query(querybuf);
			
			LOGIF(LE, (skygw_log_write_flush(
				LOGFILE_ERROR,
				"Error: Can't route %s:%s:\"%s\" to "
				"backend server. Router is closed.",
				STRPACKETTYPE(packet_type),
				STRQTYPE(qtype),
				(query_str == NULL ? "(empty)" : query_str))));
			free(query_str);
		}
		succp = false;
		goto retblock;
	}
	/**
	 * There is a hint which either names the target backend or
	 * hint which sets maximum allowed replication lag for the 
	 * backend.
	 */
	if (TARGET_IS_NAMED_SERVER(route_target) ||
		TARGET_IS_RLAG_MAX(route_target))
	{
		HINT* hint;
		char* named_server = NULL;
		
		hint = querybuf->hint;
		
		while (hint != NULL)
		{
			if (hint->type == HINT_ROUTE_TO_NAMED_SERVER)
			{
				/**
				 * Set the name of searched 
				 * backend server.
				 */
				named_server = hint->data;
				LOGIF(LT, (skygw_log_write(
					LOGFILE_TRACE,
					"Hint: route to server "
					"'%s'",
					named_server)));       
			}
			else if (hint->type == HINT_PARAMETER &&
				(strncasecmp((char *)hint->data,
				"max_slave_replication_lag",
				strlen("max_slave_replication_lag")) == 0))
			{
				int val = (int) strtol((char *)hint->value, 
							(char **)NULL, 10);
				
				if (val != 0 || errno == 0)
				{
					/**
					 * Set max. acceptable
					 * replication lag 
					 * value for backend srv
					 */
					rlag_max = val;
					LOGIF(LT, (skygw_log_write(
						LOGFILE_TRACE,
						"Hint: "
						"max_slave_replication_lag=%d",
						rlag_max)));
				}
			}
			hint = hint->next;
		} /*< while */
		
		if (rlag_max == MAX_RLAG_UNDEFINED) /*< no rlag max hint, use config */
		{
			rlag_max = rses_get_max_replication_lag(rses);
		}
		btype = route_target & TARGET_SLAVE ? BE_SLAVE : BE_MASTER; /*< target may be master or slave */
		/**
		 * Search backend server by name or replication lag. 
		 * If it fails, then try to find valid slave or master.
		 */ 
		succp = get_dcb(&target_dcb, rses, btype, named_server,rlag_max);
		
		if (!succp)
		{
			if (TARGET_IS_NAMED_SERVER(route_target))
			{
				LOGIF(LT, (skygw_log_write(
					LOGFILE_TRACE,
					"Was supposed to route to named server "
					"%s but couldn't find the server in a "
					"suitable state.",
					named_server)));
			}
			else if (TARGET_IS_RLAG_MAX(route_target))
			{
				LOGIF(LT, (skygw_log_write(
					LOGFILE_TRACE,
					"Was supposed to route to server with "
					"replication lag at most %d but couldn't "
					"find such a slave.",
					rlag_max)));
			}
		}
	}
	else if (TARGET_IS_SLAVE(route_target))
	{
		btype = BE_SLAVE;
		
		if (rlag_max == MAX_RLAG_UNDEFINED) /*< no rlag max hint, use config */
		{
			rlag_max = rses_get_max_replication_lag(rses);
		}
		/**
		 * Search suitable backend server, get DCB in target_dcb
		 */ 
		succp = get_dcb(&target_dcb, rses, BE_SLAVE, NULL,rlag_max);

		if (succp)
		{
		    backend_ref_t* br = get_root_master_bref(rses);
			ss_dassert(get_root_master_bref(rses) == 
				rses->rses_master_ref);
			atomic_add(&inst->stats.n_slave, 1);
		}
		else
		{
			LOGIF(LT, (skygw_log_write(LOGFILE_TRACE,
						   "Was supposed to route to slave"
						   "but finding suitable one "
						   "failed.")));
		}
	}
	else if (TARGET_IS_MASTER(route_target))
	{
		DCB* curr_master_dcb = NULL;
		
		succp = get_dcb(&curr_master_dcb, 
				rses, 
				BE_MASTER, 
				NULL,
				MAX_RLAG_UNDEFINED);
		
		if (succp && master_dcb == curr_master_dcb)
		{
			atomic_add(&inst->stats.n_master, 1);
			target_dcb = master_dcb;
		}
		else
		{
			if (succp && master_dcb != curr_master_dcb)
			{
				LOGIF(LT, (skygw_log_write(LOGFILE_TRACE,
							   "Was supposed to "
							   "route to master "
							   "but master has "
							   "changed.")));
			}
			else
			{
				LOGIF(LT, (skygw_log_write(LOGFILE_TRACE,
							   "Was supposed to "
							   "route to master "
							   "but couldn't find "
							   "master in a "
							   "suitable state.")));
			}
			/**
			 * Master has changed. Return with error indicator.
			 */
			rses_end_locked_router_action(rses);
			succp = false;
			goto retblock;
		}
	}
	
	if (succp) /*< Have DCB of the target backend */
	{
		backend_ref_t*   bref;
		
		
		bref = get_bref_from_dcb(rses, target_dcb);
		
		ss_dassert(target_dcb != NULL);
		
		LOGIF(LT, (skygw_log_write(
			LOGFILE_TRACE,
			"Route query to %s \t%s:%d <",
			(SERVER_IS_MASTER(bref->bref_backend->backend_server) ? 
			"master" : "slave"),
			bref->bref_backend->backend_server->name,
			bref->bref_backend->backend_server->port)));
		/** 
		 * Store current stmt if execution of previous session command 
		 * haven't completed yet.
		 * 
		 * !!! Note that according to MySQL protocol
		 * there can only be one such non-sescmd stmt at the time.
		 * It is possible that bref->bref_pending_cmd includes a pending
		 * command if rwsplit is parent or child for another router, 
		 * which runs all the same commands.
		 * 
		 * If the assertion below traps, pending queries are treated 
		 * somehow wrong, or client is sending more queries before 
		 * previous is received.
		 */
		if (BREF_IS_WAITING_RESULT(bref))
		{
			ss_dassert(bref->bref_pending_cmd == NULL);
			bref->bref_pending_cmd = gwbuf_clone(querybuf);
			
			rses_end_locked_router_action(rses);
			goto retblock;
		}
		
		if ((ret = target_dcb->func.write(target_dcb, gwbuf_clone(querybuf))) == 1)
		{
			backend_ref_t* bref;
			
			atomic_add(&inst->stats.n_queries, 1);
			/**
			 * Add one query response waiter to backend reference
			 */
			bref = get_bref_from_dcb(rses, target_dcb);
			bref_set_state(bref, BREF_QUERY_ACTIVE);
			bref_set_state(bref, BREF_WAITING_RESULT);
		}
		else
		{
			LOGIF(LE, (skygw_log_write_flush(
				LOGFILE_ERROR,
				"Error : Routing query failed.")));
			succp = false;
		}
	}
	rses_end_locked_router_action(rses);
	
retblock:

	return succp;	
}
/**
 * Client Reply routine
 *
 * The routine will reply to client for session change with master server data
 *
 * @param	instance	The router instance
 * @param	router_session	The router session 
 * @param	backend_dcb	The backend DCB
 * @param	queue		The GWBUF with reply data
 */
void handle_clientReply (
        ROUTER* instance,
        void*   router_session,
        GWBUF*  writebuf,
        DCB*    backend_dcb)
{
        DCB*               client_dcb;
        ROUTER_CLIENT_SES* router_cli_ses;
        backend_ref_t*     bref;
	GWBUF* buffer = writebuf;
        SCMDCURSOR* cursor;
	router_cli_ses = (ROUTER_CLIENT_SES *)router_session;
        CHK_CLIENT_RSES(router_cli_ses);

        /**
         * Lock router client session for secure read of router session members.
         * Note that this could be done without lock by using version #
         */
        if (!spinlock_acquire_with_test(&router_cli_ses->rses_lock, &router_cli_ses->rses_closed, FALSE))
        {
                print_error_packet(router_cli_ses, buffer, backend_dcb);
                goto lock_failed;
	}
        /** Holding lock ensures that router session remains open */
        ss_dassert(backend_dcb->session != NULL);
	client_dcb = backend_dcb->session->client;

        /** Unlock */
        rses_end_locked_router_action(router_cli_ses);        
	
	if (client_dcb == NULL)
	{
                while ((buffer = gwbuf_consume(
                        buffer,
                        GWBUF_LENGTH(buffer))) != NULL);
		/** Log that client was closed before reply */
                goto lock_failed;
	}
	/** Lock router session */
        if (!spinlock_acquire_with_test(&router_cli_ses->rses_lock, &router_cli_ses->rses_closed, FALSE))
        {
                /** Log to debug that router was closed */
                goto lock_failed;
        }
        bref = get_bref_from_dcb(router_cli_ses, backend_dcb);

	/** This makes the issue becoming visible in poll.c */
	if (bref == NULL)
	{
		/** Unlock router session */
		rses_end_locked_router_action(router_cli_ses);
		goto lock_failed;
	}
	
        CHK_BACKEND_REF(bref);
        
	if (GWBUF_IS_TYPE_SESCMD_RESPONSE(buffer))
	{
                
	    /** 
	     * Discard all those responses that have already been sent to
	     * the client. 
	     */
	    GWBUF* ncmd;
	    SCMDCURSOR* cursor;

	    cursor = dcb_get_sescmdcursor(backend_dcb);
	    bool success = sescmdlist_process_replies(cursor, &buffer);

	    if(!success)
	    {
		bref_clear_state(bref,BREF_IN_USE);
		bref_set_state(bref,BREF_CLOSED);
	    }
	    else
	    {
		sescmdlist_execute(cursor);
	    }

	    /** 
	     * If response will be sent to client, decrease waiter count.
	     * This applies to session commands only. Counter decrement
	     * for other type of queries is done outside this block.
	     */
	    if (buffer != NULL && client_dcb != NULL)
	    {
		/** Set response status as replied */
		bref_clear_state(bref, BREF_WAITING_RESULT);
	    }
	}
	/**
         * Clear BREF_QUERY_ACTIVE flag and decrease waiter counter.
         * This applies for queries  other than session commands.
         */

	else if (BREF_IS_QUERY_ACTIVE(bref))
	{
                bref_clear_state(bref, BREF_QUERY_ACTIVE);
                /** Set response status as replied */
                bref_clear_state(bref, BREF_WAITING_RESULT);
        }

        if (buffer != NULL && client_dcb != NULL)
        {
                /** Write reply to client DCB */
		SESSION_ROUTE_REPLY(backend_dcb->session, buffer);
        }
        /** Unlock router session */
        rses_end_locked_router_action(router_cli_ses);
        
        /** Lock router session */
        if (!spinlock_acquire_with_test(&router_cli_ses->rses_lock, &router_cli_ses->rses_closed, FALSE))
        {
                /** Log to debug that router was closed */
                goto lock_failed;
        }
        
       if (bref->bref_pending_cmd != NULL) /*< non-sescmd is waiting to be routed */
	{
		int ret;
		
		CHK_GWBUF(bref->bref_pending_cmd);
		
		if ((ret = bref->bref_dcb->func.write(
				bref->bref_dcb, 
				gwbuf_clone(bref->bref_pending_cmd))) == 1)
		{
			ROUTER_INSTANCE* inst = (ROUTER_INSTANCE *)instance;
			atomic_add(&inst->stats.n_queries, 1);
			/**
			 * Add one query response waiter to backend reference
			 */
			bref_set_state(bref, BREF_QUERY_ACTIVE);
			bref_set_state(bref, BREF_WAITING_RESULT);
		}
		else
		{
			LOGIF(LE, (skygw_log_write_flush(
				LOGFILE_ERROR,
				"Error : Routing query \"%s\" failed.",
				bref->bref_pending_cmd)));
		}
		gwbuf_free(bref->bref_pending_cmd);
		bref->bref_pending_cmd = NULL;
	}
	/** Unlock router session */
        rses_end_locked_router_action(router_cli_ses);
        
lock_failed:
        return;
}
/**
 * Execute in backends used by current router session.
 * Save session variable commands to router session property
 * struct. Thus, they can be replayed in backends which are 
 * started and joined later.
 * 
 * Suppress redundant OK packets sent by backends.
 * 
 * The first OK packet is replied to the client.
 * 
 * @param router_cli_ses	Client's router session pointer
 * @param querybuf		GWBUF including the query to be routed
 * @param inst			Router instance
 * @param packet_type		Type of MySQL packet
 * @param qtype			Query type from query_classifier
 * 
 * @return True if at least one backend is used and routing succeed to all 
 * backends being used, otherwise false.
 * 
 */
bool route_session_write(
        ROUTER_CLIENT_SES* router_cli_ses,
        GWBUF*             querybuf,
        ROUTER_INSTANCE*   inst,
        unsigned char      packet_type,
        skygw_query_type_t qtype)
{
        bool              succp;
        rses_property_t*  prop;
        backend_ref_t*    backend_ref;
        int               i;
	int               max_nslaves;
	int               nbackends;
	int 		  nsucc;
  
        LOGIF(LT, (skygw_log_write(
                LOGFILE_TRACE,
                "Session write, routing to all servers.")));
	/** Maximum number of slaves in this router client session */
	max_nslaves = rses_get_max_slavecount(router_cli_ses, 
					  router_cli_ses->rses_nbackends);
	nsucc = 0;
	nbackends = 0;
        backend_ref = router_cli_ses->rses_backend_ref;
        
        /**
         * These are one-way messages and server doesn't respond to them.
         * Therefore reply processing is unnecessary and session 
         * command property is not needed. It is just routed to all available
         * backends.
         */
        if (packet_type == MYSQL_COM_STMT_SEND_LONG_DATA ||
                packet_type == MYSQL_COM_QUIT ||
                packet_type == MYSQL_COM_STMT_CLOSE)
        {
                int rc;

		/** Lock router session */
                if (!spinlock_acquire_with_test(&router_cli_ses->rses_lock, &router_cli_ses->rses_closed, FALSE))
                {
                        goto return_succp;
                }
                                
                for (i=0; i<router_cli_ses->rses_nbackends; i++)
                {
                        DCB* dcb = backend_ref[i].bref_dcb;     
			
			if (LOG_IS_ENABLED(LOGFILE_TRACE))
			{
				LOGIF(LT, (skygw_log_write(
					LOGFILE_TRACE,
					"Route query to %s \t%s:%d%s",
					(SERVER_IS_MASTER(backend_ref[i].bref_backend->backend_server) ? 
						"master" : "slave"),
					backend_ref[i].bref_backend->backend_server->name,
					backend_ref[i].bref_backend->backend_server->port,
					(i+1==router_cli_ses->rses_nbackends ? " <" : " "))));
			}

                        if (BREF_IS_IN_USE((&backend_ref[i])))
                        {
				nbackends += 1;
                                if ((rc = dcb->func.write(dcb, gwbuf_clone(querybuf))) == 1)
				{
					nsucc += 1;
				}
                        }
                }
                rses_end_locked_router_action(router_cli_ses);
                gwbuf_free(querybuf);
                goto return_succp;
        }
        /** Lock router session */
        if (!spinlock_acquire_with_test(&router_cli_ses->rses_lock, &router_cli_ses->rses_closed, FALSE))
        {
                goto return_succp;
        }
        
        if (router_cli_ses->rses_nbackends <= 0)
	{
		LOGIF(LT, (skygw_log_write(
			LOGFILE_TRACE,
			"Router session doesn't have any backends in use. "
			"Routing failed. <")));
		
		goto return_succp;
	}
	
        /** 
         * Add the command to the list of session commands.
         */
        sescmdlist_add_command(router_cli_ses->rses_sescmd_list,querybuf);
	for(i = 0;i<router_cli_ses->rses_nbackends;i++)
	{
	    if(BREF_IS_IN_USE(&router_cli_ses->rses_backend_ref[i]))
	    {
		SCMDCURSOR* cursor;
		cursor = dcb_get_sescmdcursor(router_cli_ses->rses_backend_ref[i].bref_dcb);
		sescmdlist_execute(cursor);
	    }
	}

        gwbuf_free(querybuf);
	
        /** Unlock router session */
        rses_end_locked_router_action(router_cli_ses);
               
return_succp:
	/** 
	 * Routing must succeed to all backends that are used.
	 * There must be at leas one and at most max_nslaves+1 backends.
	 */
	succp = (nbackends > 0 && nsucc == nbackends && nbackends <= max_nslaves+1);
        return succp;
}
