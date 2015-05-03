#ifndef _TRX_HG_
#define _TRX_HG_
#include <stdlib.h>
#include <buffer.h>
typedef struct transaction_state_t{
    bool transaction_active;
    bool autocommit_on;
}TRXSTATE;

void update_transaction_state(GWBUF* buffer, TRXSTATE* state);

#endif
