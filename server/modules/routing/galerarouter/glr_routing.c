#include <galerarouter.h>
#include <stdlib.h>
#include <skygw_utils.h>
#include <query_classifier.h>

int hash_query(GWBUF* query, int nodes)
{
    int tsize = 0;
    char** tables;

    if(!query_is_parsed(query))
    {
        parse_query(query);
    }

    tables = skygw_get_table_names(query,&tsize,true);

    if(tsize < 1)
	return 0;
    
    int hash = simple_str_hash(tables[0]);
    return hash != 0 ? abs(hash % nodes) : 0;
}

#ifdef BUILD_TOOLS

#include <modutil.h>
#include <getopt.h>

struct option longopt[] =
{
    {"help",no_argument,0,'?'}
};

static char* server_options[] = {
    "MariaDB Corporation MaxScale",
    "--no-defaults",
    "--datadir=.",
    "--language=.",
    "--skip-innodb",
    "--default-storage-engine=myisam",
	NULL
};

const int num_elements = (sizeof(server_options) / sizeof(char *)) - 1;

static char* server_groups[] = {
	"embedded",
	"server",
	"server",
	NULL
};

int main(int argc,char** argv)
{
    int printhelp = 0;
    char c;
    int longind = 0;

    while((c = getopt_long(argc,argv,"h?",longopt,&longind)) != -1)
    {
	switch(c)
	{
	case 'h':
	case '?':
	    printhelp = 1;
	    break;
	}
    }

    if(argc < 3 || printhelp)
    {
	printf("Usage: %s QUERY NODES\n\n"
		"Parses the QUERY and prints to which node it would be routed to\n"
		"by the Galera router. "
		"The NODES parameter is the number of galera nodes\n\n",argv[0]);
	return 1;
    }

    if(mysql_library_init(num_elements, server_options, server_groups))
    {
	printf("Error: Cannot initialize embedded mysqld library.\n");
	return 1;
    }

    GWBUF* buffer = modutil_create_query(argv[1]);
    int hval = hash_query(buffer,atoi(argv[2]));
    printf("Query routed to node %d\n",hval);
    gwbuf_free(buffer);
    return 0;
}
#endif
