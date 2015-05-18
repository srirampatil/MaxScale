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

/** Including this shouldn't be required after session command integration */
#include <shardrouter.h>
#include <service_connection.h>

/**
 * Validate the status of the subservice.
 * @param sub Subservice to validate
 * @return True if the subservice is valid, false if the session or it's router
 * are NULL or the session or the service is not in a valid state.
 */
bool subsvc_is_valid(SUBSERVICE* sub)
{

    if(sub->session == NULL ||
       sub->service->router == NULL)
    {
        return false;
    }

    spinlock_acquire(&sub->session->ses_lock);
    session_state_t ses_state = sub->session->state;
    spinlock_release(&sub->session->ses_lock);

    spinlock_acquire(&sub->service->spin);
    int svc_state = sub->service->state;
    spinlock_release(&sub->service->spin);

    if(ses_state == SESSION_STATE_ROUTER_READY &&
       (svc_state != SERVICE_STATE_FAILED ||
        svc_state != SERVICE_STATE_STOPPED))
    {
        return true;
    }

    return false;
}

void
subsvc_set_state(SUBSERVICE* svc,subsvc_state_t state)
{
    if(state & SUBSVC_WAITING_RESULT)
    {

        /** Increase waiter count */
       atomic_add(&svc->n_res_waiting, 1);
    }
    
    svc->state |= state;
}

void
subsvc_clear_state(SUBSERVICE* svc,subsvc_state_t state)
{
    

    if(state & SUBSVC_WAITING_RESULT)
    {
        /** Decrease waiter count */
        atomic_add(&svc->n_res_waiting, -1);
    }
    
    svc->state &= ~state;
}

bool 
get_shard_subsvc(SUBSERVICE** subsvc,ROUTER_CLIENT_SES* session,char* target)
{
    int i;
    
    if(subsvc == NULL || session == NULL || target == NULL)
	return false;

    for(i = 0;i<session->n_subservice;i++)
    {
        if(strcmp(session->subservice[i]->service->name,target) == 0)
        {
            
            if (SUBSVC_IS_OK(session->subservice[i]))
            {
                if(subsvc_is_valid(session->subservice[i])){
                    *subsvc = session->subservice[i];
                    return true;
                }
                
                /**
                 * The service has failed 
                 */
                
                subsvc_set_state(session->subservice[i],SUBSVC_FAILED);
            }
        } 
    }
    
    return false;
}
