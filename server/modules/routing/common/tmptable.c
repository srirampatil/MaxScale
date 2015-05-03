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
 * Copyright MariaDB Corporation Ab 2013-2015
 */

#include <common/tmptable.h>

/** Defined in log_manager.cc */
extern int            lm_enabled_logfiles_bitmask;
extern size_t         log_ses_count[];
extern __thread       log_info_t tls_log_info;

/**
*Allocate a new temporary table object and initialize it.
* @return Pointer to new TMPTABLE struct or NULL if 
*/
TMPTABLE* tmptable_init()
{
    TMPTABLE* rval;
    if((rval = malloc(sizeof(TMPTABLE))) != NULL)
    {
        rval->stats.created = 0;
        rval->stats.dropped = 0;
        rval->stats.queries = 0;
        rval->stats.writes = 0;
        rval->stats.reads = 0;
        rval->hash = hashtable_alloc(25,simple_str_hash,strcasecmp);
        if(rval->hash == NULL)
        {
            free(rval);
            return NULL;
        }
	hashtable_memory_fns(rval->hash,(HASHMEMORYFN)strdup,NULL,(HASHMEMORYFN)free,NULL);
    }

    return rval;
}

/**
* Free a temporary table object.
* @param table Pointer to a TMPTABlE struct initialized by tmptable_init.
*/
void tmptable_free(TMPTABLE* table)
{
    hashtable_free(table->hash);
    free(table);
}

/**
* Parse a query and update the temporary tables managed by the TMPTABLE object.
* If the query creates or drops a temporary table, it updates the state of the TMPTABLE.
 * @param table TMPTABLE to update
 * @param db Current database
 * @param buffer Buffer with the query
 * @param type Current type of the query
*/
skygw_query_type_t tmptable_parse(TMPTABLE* table,
				  char* db,
                                  GWBUF* buffer,
                                  skygw_query_type_t type)
{
    char str[512];
    bool is_drop = false;
    int qcount = 0;
    skygw_query_type_t qtype;
    char* sptr;

    if(gwbuf_length(buffer) < 5)
        return type;

    qtype = query_classifier_get_type(buffer);

    if(QUERY_IS_TYPE(qtype,QUERY_TYPE_CREATE_TMP_TABLE))
    {
	char *tname = skygw_get_created_table_name(buffer);
	if(strchr(tname,'.') == NULL)
	{
	    /** No explicit database*/
	    sprintf(str,"%s.%s",db,tname);
	    sptr = str;
	}
	else
	{
	    sptr = tname;
	}
        hashtable_add(table->hash,(void*)sptr,"");
        atomic_add(&table->stats.created,1);
	free(tname);
	qcount++;
    }
    else if(modutil_is_SQL(buffer) || (is_drop = is_drop_table_query(buffer)))
    {
	int tablecount = 0,i;
	char** tablenames = skygw_get_table_names(buffer,&tablecount,true);

	for(i = 0;i<tablecount;i++)
	{
	    /** The query has tables in it, see if any of them are in
	     * the TMPTABLE hash table. If they are, mark the query type as
	     * temporary table query. */
	    if(strchr(tablenames[i],'.') == NULL)
	    {
		/** No explicit database*/
		sprintf(str,"%s.%s",db,tablenames[i]);
		sptr = str;
	    }
	    else
	    {
		sptr = tablenames[i];
	    }

	    if(is_drop)
	    {
		if(hashtable_delete(table->hash,sptr))
		{
		    atomic_add(&table->stats.dropped,1);
		    qcount++;
		}
	    }
	    else if(hashtable_fetch(table->hash,sptr))
	    {
		qtype |= QUERY_TYPE_READ_TMP_TABLE;
		qcount++;
		
		if(QUERY_IS_TYPE(qtype,QUERY_TYPE_WRITE))
		{
		    atomic_add(&table->stats.writes,1);
		}
		else
		{
		    atomic_add(&table->stats.reads,1);
		}
	    }
	    free(tablenames[i]);
	}
	free(tablenames);
    }
    
    if(qcount > 0)
    {
	atomic_add(&table->stats.queries,1);
    }

    return (qtype | type);
}

/**
 * Print temporary table usage statistics to a DCB.
 * @param table TMPTABLE to print
 * @param dcb DCB to use
 */
void tmptable_print(TMPTABLE* table, DCB* dcb)
{
    dcb_printf(dcb,"Temporary table statistics:\n");
    dcb_printf(dcb,"%-14s|%-14s|%-14s|%-14s|%-14s\n",
	       "Queries","Reads","Writes","Created","Dropped");
    dcb_printf(dcb,"%-14d|%-14d|%-14d|%-14d|%-14d\n",
	       table->stats.queries,
	       table->stats.reads,
	       table->stats.writes,
	       table->stats.created,
	       table->stats.dropped);
}
