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
 * @file routeresolve.c routing target resolving functions
 *
 * This file contains functions used for resolving the target server of a query
 *
 * @verbatim
 * Revision History
 *
 * Date			Who				Description
 * 16/04/2015	Markus Makela	Initial implementation
 *
 * @endverbatim
 */
#include <stdlib.h>
#include <skygw_utils.h>
#include <query_classifier.h>
#include <strings.h>
#include <modutil.h>
#include <sescmd.h>
#include <common/routeresolution.h>
/** This includes the log manager thread local variables */
LOG_MANAGER_TLS

/**
 * Hash a query based on the tables it targets. 
 * This converts the tables in the query into a unique hash value.
 * THIS IS A WORK IN PROGRESS AND IS NOT COMPLETE, THE HASHING IS NOT RELIABLE.
 * @param query Query to hash
 * @param nodes Number of valid nodes
 * @return number of the node the query is assigned to
 */
int hash_query_by_table(GWBUF* query, int nodes)
{
    int i,hash,val,tsize = 0;
    char** tables;

    if(!query_is_parsed(query))
    {
        parse_query(query);
    }

    tables = skygw_get_table_names(query,&tsize,true);

    if(tsize < 1)
    {
	return 0;
    }
    qsort(tables,tsize, sizeof(char*),(int(*)(const void*, const void*))strcasecmp);
    hash = simple_str_hash(tables[0]);
    hash = hash != 0 ? abs(hash % nodes) : 0;

    /** This is only for trace logging, for now.
     * Consider concatenating all db.table strings into a single string */

    if (tsize > 1 &&
	LOG_IS_ENABLED(LOGFILE_TRACE))
    {
	for(i = 0;i<tsize;i++)
	{
	    val = simple_str_hash(tables[i]);
	    val = val != 0 ? abs(val % nodes) : 0;
	    if(val != hash)
	    {
		char *str = modutil_get_SQL(query);
		skygw_log_write(LOGFILE_TRACE,
			 "Warning, executing statement with cross-node tables: %s",
			 str);
		free(str);
	    }
	}
    }

    for(i = 0;i<tsize;i++)
	free(tables[i]);
    free(tables);

    return hash;
}


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
route_target_t get_route_target (
        skygw_query_type_t qtype,
        bool               trx_active,
        bool uservar_in_master,
        HINT*              hint)
{
        route_target_t target = TARGET_UNDEFINED;
	/**
	 * These queries are not affected by hints
	 */
	if (QUERY_IS_TYPE(qtype, QUERY_TYPE_SESSION_WRITE) ||
		QUERY_IS_TYPE(qtype, QUERY_TYPE_PREPARE_STMT) ||
		QUERY_IS_TYPE(qtype, QUERY_TYPE_PREPARE_NAMED_STMT) ||
		/** Configured to allow writing variables to all nodes */
		(uservar_in_master == false &&
			QUERY_IS_TYPE(qtype, QUERY_TYPE_GSYSVAR_WRITE)) ||
		/** enable or disable autocommit are always routed to all */
		QUERY_IS_TYPE(qtype, QUERY_TYPE_ENABLE_AUTOCOMMIT) ||
		QUERY_IS_TYPE(qtype, QUERY_TYPE_DISABLE_AUTOCOMMIT))
	{
		/** 
		 * This is problematic query because it would be routed to all
		 * backends but since this is SELECT that is not possible:
		 * 1. response set is not handled correctly in clientReply and
		 * 2. multiple results can degrade performance.
		 */
		if (QUERY_IS_TYPE(qtype, QUERY_TYPE_READ))
		{
			LOGIF(LE, (skygw_log_write_flush(
				LOGFILE_ERROR,
				"Warning : The query can't be routed to all "
				"backend servers because it includes SELECT and "
				"SQL variable modifications which is not supported. "
				"Split the query in two parts, one where SQL variable modifications "
				"are done and another where the SELECT is done.")));
			
			target = TARGET_MASTER;
		}
		target |= TARGET_ALL;
	}
	/**
	 * Hints may affect on routing of the following queries
	 */
	else if (!trx_active && 
		(QUERY_IS_TYPE(qtype, QUERY_TYPE_READ) ||	/*< any SELECT */
		QUERY_IS_TYPE(qtype, QUERY_TYPE_SHOW_TABLES) || /*< 'SHOW TABLES' */
		QUERY_IS_TYPE(qtype, QUERY_TYPE_USERVAR_READ)||	/*< read user var */
		QUERY_IS_TYPE(qtype, QUERY_TYPE_SYSVAR_READ) ||	/*< read sys var */
		QUERY_IS_TYPE(qtype, QUERY_TYPE_EXEC_STMT) ||   /*< prepared stmt exec */
		QUERY_IS_TYPE(qtype, QUERY_TYPE_GSYSVAR_READ))) /*< read global sys var */
	{
		/** First set expected targets before evaluating hints */
		if (!QUERY_IS_TYPE(qtype, QUERY_TYPE_MASTER_READ) &&
			(QUERY_IS_TYPE(qtype, QUERY_TYPE_READ) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_SHOW_TABLES) || /*< 'SHOW TABLES' */
			/** Configured to allow reading variables from slaves */
			(uservar_in_master == false && 
			(QUERY_IS_TYPE(qtype, QUERY_TYPE_USERVAR_READ) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_SYSVAR_READ) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_GSYSVAR_READ)))))
		{
			target = TARGET_SLAVE;
		}
		else if (QUERY_IS_TYPE(qtype, QUERY_TYPE_MASTER_READ) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_EXEC_STMT)	||
			/** Configured not to allow reading variables from slaves */
			(uservar_in_master == true && 
			(QUERY_IS_TYPE(qtype, QUERY_TYPE_USERVAR_READ)	||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_SYSVAR_READ))))
		{
			target = TARGET_MASTER;
		}
		/** process routing hints */
		while (hint != NULL)
		{
			if (hint->type == HINT_ROUTE_TO_MASTER)
			{
				target = TARGET_MASTER; /*< override */
				LOGIF(LD, (skygw_log_write(
					LOGFILE_DEBUG,
					"%lu [get_route_target] Hint: route to master.",
					pthread_self())));
				break;
			}
			else if (hint->type == HINT_ROUTE_TO_NAMED_SERVER)
			{
				/** 
				 * Searching for a named server. If it can't be
				 * found, the oroginal target is chosen.
				 */
				target |= TARGET_NAMED_SERVER;
				LOGIF(LD, (skygw_log_write(
					LOGFILE_DEBUG,
					"%lu [get_route_target] Hint: route to "
					"named server : ",
					pthread_self())));
			}
			else if (hint->type == HINT_ROUTE_TO_UPTODATE_SERVER)
			{
				/** not implemented */
			}
			else if (hint->type == HINT_ROUTE_TO_ALL)
			{
				/** not implemented */
			}
			else if (hint->type == HINT_PARAMETER)
			{
				if (strncasecmp(
					(char *)hint->data, 
						"max_slave_replication_lag", 
						strlen("max_slave_replication_lag")) == 0)
				{
					target |= TARGET_RLAG_MAX;
				}
				else
				{
					LOGIF(LT, (skygw_log_write(
						LOGFILE_TRACE,
						"Error : Unknown hint parameter "
						"'%s' when 'max_slave_replication_lag' "
						"was expected.",
						(char *)hint->data)));
					LOGIF(LE, (skygw_log_write_flush(
						LOGFILE_ERROR,
						"Error : Unknown hint parameter "
						"'%s' when 'max_slave_replication_lag' "
						"was expected.",
						(char *)hint->data)));                                        
				}
			}
			else if (hint->type == HINT_ROUTE_TO_SLAVE)
			{
				target = TARGET_SLAVE;
				LOGIF(LD, (skygw_log_write(
					LOGFILE_DEBUG,
					"%lu [get_route_target] Hint: route to "
					"slave.",
					pthread_self())));                                
			}
			hint = hint->next;
		} /*< while (hint != NULL) */
		/** If nothing matches then choose the master */
		if ((target & (TARGET_ALL|TARGET_SLAVE|TARGET_MASTER)) == 0)
		{
			target = TARGET_MASTER;
		}
	}
	else
	{
		/** hints don't affect on routing */
		ss_dassert(trx_active ||
			(QUERY_IS_TYPE(qtype, QUERY_TYPE_WRITE) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_MASTER_READ) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_SESSION_WRITE) ||
			(QUERY_IS_TYPE(qtype, QUERY_TYPE_USERVAR_READ) &&
				uservar_in_master == true) ||
			(QUERY_IS_TYPE(qtype, QUERY_TYPE_SYSVAR_READ) &&
				uservar_in_master == true) ||
			(QUERY_IS_TYPE(qtype, QUERY_TYPE_GSYSVAR_READ) &&
				uservar_in_master == true) ||
			(QUERY_IS_TYPE(qtype, QUERY_TYPE_GSYSVAR_WRITE) &&
				uservar_in_master == true) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_BEGIN_TRX) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_ENABLE_AUTOCOMMIT) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_DISABLE_AUTOCOMMIT) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_ROLLBACK) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_COMMIT) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_EXEC_STMT) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_CREATE_TMP_TABLE) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_READ_TMP_TABLE) ||
			QUERY_IS_TYPE(qtype, QUERY_TYPE_UNKNOWN)));
		target = TARGET_MASTER;
	}

	return target;
}

/** 
* Resolve the query type of a MySQL query.
* @param querybuf GWBUF containing the query
* @param packet_type The command of the query e.g. COM_QUERY
* @return The type of the query or QUERY_TYPE_UNKNOWN if it can't be resolved
*/
skygw_query_type_t resolve_query_type(GWBUF* querybuf, mysql_server_cmd_t packet_type)
{
	skygw_query_type_t qtype = QUERY_TYPE_UNKNOWN;
    
	switch(packet_type) {
		case MYSQL_COM_QUIT:        /*< 1 QUIT will close all sessions */
		case MYSQL_COM_INIT_DB:     /*< 2 DDL must go to the master */
		case MYSQL_COM_REFRESH:     /*< 7 - I guess this is session but not sure */
		case MYSQL_COM_DEBUG:       /*< 0d all servers dump debug info to stdout */
		case MYSQL_COM_PING:        /*< 0e all servers are pinged */
		case MYSQL_COM_CHANGE_USER: /*< 11 all servers change it accordingly */
		case MYSQL_COM_STMT_CLOSE:  /*< free prepared statement */
		case MYSQL_COM_STMT_SEND_LONG_DATA: /*< send data to column */
		case MYSQL_COM_STMT_RESET:  /*< resets the data of a prepared statement */
			qtype = QUERY_TYPE_SESSION_WRITE;
			break;
			
		case MYSQL_COM_CREATE_DB:   /**< 5 DDL must go to the master */
		case MYSQL_COM_DROP_DB:     /**< 6 DDL must go to the master */
			qtype = QUERY_TYPE_WRITE;
			break;
			
		case MYSQL_COM_QUERY:
			qtype = query_classifier_get_type(querybuf);
			break;
			
		case MYSQL_COM_STMT_PREPARE:
			qtype = query_classifier_get_type(querybuf);
			qtype |= QUERY_TYPE_PREPARE_STMT;
			break;
			
		case MYSQL_COM_STMT_EXECUTE:
			/** Parsing is not needed for this type of packet */
			qtype = QUERY_TYPE_EXEC_STMT;
			break;
			
		case MYSQL_COM_SHUTDOWN:       /**< 8 where should shutdown be routed ? */
		case MYSQL_COM_STATISTICS:     /**< 9 ? */
		case MYSQL_COM_PROCESS_INFO:   /**< 0a ? */
		case MYSQL_COM_CONNECT:        /**< 0b ? */
		case MYSQL_COM_PROCESS_KILL:   /**< 0c ? */
		case MYSQL_COM_TIME:           /**< 0f should this be run in gateway ? */
		case MYSQL_COM_DELAYED_INSERT: /**< 10 ? */
		case MYSQL_COM_DAEMON:         /**< 1d ? */
		default:
			break;
	}

    return qtype;
}
