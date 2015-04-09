#include <galerarouter.h>
#include <stdlib.h>
#include <skygw_utils.h>

int hash_query(GWBUF* query, int nodes)
{
    int tsize = 0;
    char** tables = skygw_get_table_names(query,&tsize,true);

    if(tsize < 1)
	return 0;
    
    int hash = simple_str_hash();
    return hash != 0 ? abs(hash % nodes) : 0;
}
