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

/**
 * @file transactions.c Session transaction state tracking
 *
 * This file contains functions for tracking the transaction and autocommit 
 * states of a MySQL session.
 *
 * @verbatim
 * Revision History
 *
 * Date			Who				Description
 * 03/05/2015	Markus Makela	Initial implementation
 *
 * @endverbatim
 */

#include <common/transactions.h>
#include <query_classifier.h>

/**
 * Initialize a TRXSTATE structure.
 * Autocommit is on and no trannsactions are open.
 * @param state State to initialize
 */
void init_transaction_state(TRXSTATE* state)
{
    if(state)
    {
	state->autocommit_on = true;
	state->transaction_active = false;
    }
}

/**
 * Update the transaction and autocommit state of a session.
 * If autocommit is disabled or transaction is explicitly started
 * transaction becomes active and master gets all statements until
 * transaction is committed and autocommit is enabled again.
 * @param qtype Type of the query
 * @param trx_active Is transacation active or not
 * @param autocommit Is autocommit enabled or not
 */
void update_transaction_state(GWBUF* buffer, TRXSTATE* state)
{
    skygw_query_type_t qtype = query_classifier_get_type(buffer);
    bool* trx_active = &state->transaction_active;
    bool* autocommit = &state->autocommit_on;

	if (*autocommit &&
		QUERY_IS_TYPE(qtype, QUERY_TYPE_DISABLE_AUTOCOMMIT))
	{
		*autocommit = false;
		
		if (!*trx_active)
		{
			*trx_active = true;
		}
	}
	else if (!*trx_active &&
             QUERY_IS_TYPE(qtype, QUERY_TYPE_BEGIN_TRX))
	{
		*trx_active = true;
	}
	/** 
	 * Explicit COMMIT and ROLLBACK, implicit COMMIT.
	 */
	if (*autocommit &&
		*trx_active &&
		(QUERY_IS_TYPE(qtype,QUERY_TYPE_COMMIT) ||
         QUERY_IS_TYPE(qtype,QUERY_TYPE_ROLLBACK)))
	{
		*trx_active = false;
	} 
	else if (!*autocommit &&
             QUERY_IS_TYPE(qtype, QUERY_TYPE_ENABLE_AUTOCOMMIT))
	{
		*autocommit = true;
		*trx_active = false;
	}
}
