/*************************************************************************/
/* This program runs on distributed helpers. It is called by worker      */
/* (by socket) in minigoogle. When it eatablishes a connection with      */
/* worker, it first tell the name server that it is busy, then start     */
/* the work count on the file. After done, it will tell name server      */
/* that it is idle.                                                      */
/*************************************************************************/

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <netdb.h>
#include <pthread.h>
// #include "name_func.h"

#define BUFSIZ 1024
#define N 10 // max number of reducers
#define NUM_THREADS 5

int operate_ns(char *op, char *type, char *address, char *port);
void get_value_by_index(char *s, char *str, int index);
void seperate_file(char *file, char start_char, char end_char, int i);
void send_work_to_reducer_and_listen(void *threadarg);

struct thread_data
{
    char * mapper_num;
    char * split_name;
    char * origin_file_name;
    char * reducer_addr;
    char * reducer_port;
    int i;
    int worker_sk;
};
struct thread_data thread_data_array[NUM_THREADS];
int reducer_num = 0; // init by k
int number_of_reducer_done = 0;

main()
{
    struct sockaddr_in local;
    struct sockaddr_in remote;
    int sk, len = sizeof(local), worker_sk, reducers_sk[N], rc;
    struct hostent *hp, *gethostbyname();

    char buf[BUFSIZ];

    // store reducer address and port
    char mapper_num[10];
    char split_name[20];
    char origin_file_name[100];
    char count_result_name[30];
    char reducer_str[N][BUFSIZ];
    char reducer_addr[N][128];
    char reducer_port[N][10];
    char mapper_sent_str[BUFSIZ];
    pthread_t threads[NUM_THREADS];
    char cmd[200];

    /* Create an internet domain stream socket */
    sk = socket(AF_INET, SOCK_STREAM, 0);

    /*Construct and bind the name using default values*/
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = INADDR_ANY;
    local.sin_port = 0;
    bind(sk, (struct sockaddr *)&local, sizeof(local));

    /*Find out and publish socket name */
    getsockname(sk, (struct sockaddr *)&local, &len);
    printf("Socket has port %d\n", local.sin_port);

    operate_ns("register", "mapper", "localhost", local.sin_port);

    /* Start accepting connections */
    /*Declare willingness to accept a connection*/
    listen(sk, 5);

    while (1)
    {
        worker_sk = accept(sk, 0, 0); /*Accept new request for a connection*/





        // TODO: receive the file and store it on helper local disk
        //       Do word count and combine the result.






        read(worker_sk, buf, BUFSIZ);
        printf("%s", buf);

        int i = 0, j = 0, k = 0, l = 0;
        char c;

        // get the mapper number to send to reducer, 
        // in order to let reducer to know how many 
        // seperated file should be received;
        while ((c = buf[i++]) != '\n')
        {
            mapper_num[j++] = c;
        }
        mapper_num[j] = '\0';
        j = 0;

        // get the split file name;
        while ((c = buf[i++]) != '\n')
        {
            split_name[j++] = c;
        }
        split_name[j] = '\0';
        j = 0;

        // get the original file name;
        while ((c = buf[i++]) != '\n')
        {
            origin_file_name[j++] = c;
        }
        origin_file_name[j] = '\0';
        j = 0;

        // parse reducer address
        while ((c = buf[i++]) != '\0')
        {
            if (c != '\n')
            {
                reducer_str[k][j++] = c;
                continue;
            }
            if (j < 10) break; // in case non-vaild address string
            reducer_str[k][j] = '\0';

            get_value_by_index(reducer_addr[k], reducer_str[k], 1);
            get_value_by_index(reducer_port[k], reducer_str[k], 2);

            j = 0;
            k++;
        }

        reducer_num = k; // set up reducer number global variable to enable thread communcation


        printf("Origin Name: %s\n", origin_file_name);
        printf("Split Name: %s\n", split_name);


        /*************************************************************************/
        /*
        /*  Count word using command line tool
        /*
        /* 1) tr:   turn space to newline
        /* 2) tr:   turn upper case to lower case
        /* 3) sed:  get rid of char besides a-z
        /* 4) sort: sort by char
        /* 5) uniq: count unique line
        /* 6) awk:  print out to file
        /*
        /*************************************************************************/

        sprintf(cmd, "(tr ' ' '\\n' | tr '[:upper:]' '[:lower:]' | sed -e 's/[^a-z]//g' | sort | uniq -c | awk '{print $2\" \"$1}') < ./tmp/%s > ./tmp/%s_count ", split_name, split_name);
        //printf("%s\n", cmd);
        system(cmd);

        // TODO:
        // seperate the word count result by number of reducers (k)
        // then send it each seperate to each reducer.
        // use the alphabetical to split reducer's task (namely shuffle)

        int step = (25 + k) / k; // round up
        j = 0, l = 0;
        char *alpha = "abcdefghijklmnopqrstuvwxyz";

        for (i = 0; i < 26; i++)
        {
        	if (i == 0) continue;
            if ((i + 1) % step == 0 || (i + 1) == 26)
            {
                sprintf(count_result_name, "./tmp/%s_count", split_name);
                seperate_file(count_result_name, alpha[j], alpha[i], l++);
                j = i + 1;
            }
        }

        // shuffle to reducers
        for (i = 0; i < k; i++)
        {
            printf("Reducer Address: %s, Port: %s\n", reducer_addr[i], reducer_port[i]);

            thread_data_array[i].mapper_num = mapper_num;
            thread_data_array[i].split_name = split_name;
            thread_data_array[i].origin_file_name = origin_file_name;
            thread_data_array[i].reducer_addr = reducer_addr[i];
            thread_data_array[i].reducer_port = reducer_port[i];
            thread_data_array[i].i = i;
            thread_data_array[i].worker_sk = worker_sk;

	        rc = pthread_create(&threads[i], NULL, send_work_to_reducer_and_listen,
	                            (void *) &thread_data_array[i]);
	        if (rc)
            {
                printf("ERROR; return code from pthread_create() is %d\n", rc);
                exit(-1);
            }
            printf("thread: %d running\n", i);
        }


        // TODO: HOW TO Communicate with Reducers?
        // SOLVED: use multi-thread to listening connection to those reducers
        // and a global variable to determine whether if all reducers finished.

    }
}

void send_work_to_reducer_and_listen(void *threadarg)
{	
	// variable for socket
    struct sockaddr_in local;
    struct sockaddr_in remote;
    int reducer_sk;
    struct hostent *hp, *gethostbyname();
    char buf[BUFSIZ];


    // variable for thread argument passing
    struct thread_data *my_data;
    my_data = (struct thread_data *) threadarg;

    char mapper_num[10];
    char split_name[20];
    char origin_file_name[100];
    char reducer_addr[128];
    char reducer_port[10];
    int i;
    int worker_sk;

    sprintf(mapper_num, "%s", my_data->mapper_num);
    sprintf(split_name, "%s", my_data->split_name);
    sprintf(origin_file_name, "%s", my_data->origin_file_name);
    sprintf(reducer_addr, "%s", my_data->reducer_addr);
    sprintf(reducer_port, "%s", my_data->reducer_port);
    i = my_data->i;
    worker_sk = my_data->worker_sk;

    char reducer_sent_str[BUFSIZ];


    reducer_sk = socket(AF_INET, SOCK_STREAM, 0);
    remote.sin_family = AF_INET;

    hp = gethostbyname(reducer_addr);
    printf("address: %s\n", reducer_addr);

    bcopy(hp->h_addr, (char *)&remote.sin_addr, hp->h_length);
    remote.sin_port = atoi(reducer_port);
    printf("port: %s\n", reducer_port);

    connect(reducer_sk, (struct sockaddr *)&remote, sizeof(remote));
    sprintf(reducer_sent_str, "%d\n%s\n%s\n%s_count_%d\n"
    	, i 				// current reducer index
    	, mapper_num		// mapper number
    	, origin_file_name	// original file name
    	, split_name		// filename: split file that reduce should work on
    	, i);				// filename: index of current split

    printf("reducer_sent_str:\n");
    printf("%s\n", reducer_sent_str);

    write(reducer_sk, reducer_sent_str, strlen(reducer_sent_str) + 1);

    // listening to reducers
	char status[10];

    read(reducer_sk, buf, BUFSIZ);
    get_value_by_index(status, buf, 0);
    if (strcmp(status, "done") == 0) {
    	printf("one reducer done!\n");
    	number_of_reducer_done++;
    }
    if (number_of_reducer_done >= reducer_num) {
    	printf("All reducers done!\n");
    	write(worker_sk, "status:done|type:mapper", BUFSIZ);
        number_of_reducer_done = 0;
    }
    close(reducer_sk);
    pthread_exit(NULL);

    // TODO: Keep trace the helper and make sure they are
    //       not dead. But HOW?
}

int operate_ns(char *op, char *type, char *address, char *port)
{
    struct sockaddr_in remote;
    int sk;
    struct hostent *hp, *gethostbyname();
    char buf[BUFSIZ];

    sk = socket(AF_INET, SOCK_STREAM, 0);

    remote.sin_family = AF_INET;
    hp = gethostbyname("localhost"); // name server address
    bcopy(hp->h_addr, (char *)&remote.sin_addr, hp->h_length);
    remote.sin_port = atoi("33444"); // name server port

    connect(sk, (struct sockaddr *)&remote, sizeof(remote));

    sprintf(buf, "op:%s|type:%s|address:%s|port:%d", op, type, address, port);
    printf("registering helpers: %s", buf);
    write(sk, buf, strlen(buf) + 1);
    // printf("%s",buf);
    printf("register Done.\n");
    close(sk);
    return 1;
}

void get_value_by_index(char *s, char *str, int index)
{
    int i = 0, j = 0, c1 = 0, c2 = 0;
    for (i = 0; i < BUFSIZ && str[i] != '\0'; i++)
    {
        if (str[i] == ':')
        {
            c1++;
            continue;
        }
        if (str[i] == '|')
            c2++;
        if (c2 >= index + 1)
            break;
        if (c1 == index + 1 && c2 < index + 1)
            s[j++] = str[i];
    }
    s[j] = '\0';
}

void seperate_file(char *file, char start_char, char end_char, int i)
{
	char str[30];
    char line[100];
    sprintf(str, "%s_%d", file, i);
    printf("seperate: %s\n", str);
    FILE * fp, * fp_sep;

    // printf("%s\n", str);
    fp = fopen(file, "r");
    fp_sep = fopen(str, "w");

    while (fgets(line, sizeof(line), fp))
    {
    	if (line[0] >= start_char && line[0] <= end_char)
    		fprintf(fp_sep, "%s", line);
    	if (line[0] > end_char) 
    		break;
    }
    fclose(fp_sep);
    fclose(fp);
}




