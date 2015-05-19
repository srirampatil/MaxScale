#ifndef _ROUTERES_HG
#define _ROUTERES_HG
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
#include <query_classifier.h>
#include <buffer.h>
#include <mysql_client_server_protocol.h>

typedef enum {
	TARGET_UNDEFINED    = 0x00,
        TARGET_MASTER       = 0x01,
        TARGET_SLAVE        = 0x02,
        TARGET_NAMED_SERVER = 0x04,
        TARGET_ALL          = 0x08,
        TARGET_RLAG_MAX     = 0x10,
        TARGET_ANY	    = 0x20
} route_target_t;

/** Target type macros */
#define TARGET_IS_UNDEFINED(t)	  (t == TARGET_UNDEFINED)
#define TARGET_IS_MASTER(t)       (t & TARGET_MASTER)
#define TARGET_IS_SLAVE(t)        (t & TARGET_SLAVE)
#define TARGET_IS_NAMED_SERVER(t) (t & TARGET_NAMED_SERVER)
#define TARGET_IS_ALL(t)          (t & TARGET_ALL)
#define TARGET_IS_RLAG_MAX(t)     (t & TARGET_RLAG_MAX)

int hash_query_by_table(GWBUF* query, int nodes);
route_target_t get_route_target (
        skygw_query_type_t qtype,
        bool               trx_active,
        bool uservar_in_master,
        HINT*              hint);
skygw_query_type_t resolve_query_type(GWBUF* querybuf,
                                      mysql_server_cmd_t packet_type);
#endif
