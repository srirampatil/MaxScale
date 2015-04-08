#ifndef GALERAROUTER_HG
#define GALERAROUTER_HG
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
 * @file galerarouter.h - Galera cluster load balancer
 *
 * @verbatim
 * Revision History
 *
 * 08/04/2015	Markus Mäkelä	Initial implementation
 *
 * @endverbatim
 */

typedef struct galera_instance_t{
// References to all nodes
// 
}GALERA_INSTANCE;

typedef struct galera_session_t{
// Transaction open or not
// Autocommit on or off
// Current database (This should be done somewhere else than the router)
// Session commands)
}GALERA_SESSION;

#endif
