#ifndef _TMP_TABLE_HG
#define _TMP_TABLE_HG
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
#include <stdlib.h>
#include <skygw_utils.h>
#include <skygw_types.h>
#include <log_manager.h>
#include <query_classifier.h>
#include <hashtable.h>
#include <modutil.h>
#include <mysql_client_server_protocol.h>

typedef struct tmp_table_stats_t{
int created;
int dropped;
int queries;
int writes;
int reads;
}TMPSTATS;

typedef struct tmp_table_t{
HASHTABLE* hash;
TMPSTATS stats;
}TMPTABLE;

TMPTABLE* tmptable_init();
skygw_query_type_t tmptable_parse(TMPTABLE*,
				  char*,
                                  GWBUF*,
                                  skygw_query_type_t);
void tmptable_free(TMPTABLE* );
#endif
