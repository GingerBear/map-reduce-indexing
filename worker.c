/*************************************************************************/
/* This program implements the service provided by the server.           */
/* It is a child of the server and receives message from the stdin.      */
/* To execute this program read the instructions in minigoogle.c.        */
/* It split file (already splited) and send splited file to helpers      */
/* (machine).                                                            */
/*************************************************************************/
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#define BUFSIZ 1024
#define N 5 	// number of mappers
#define NUM_THREADS 5

void split_file(char **tmp_file_path, char *file_path, int k);
void lookup_ns(char *mappers_str, char *type, int num);
void get_value_by_index(char *s, char *str, int index);
void get_origin_name(char * origin_file_name, char * path_name);
void gen_random(char *s, const int len);
void listening_mappers(void *threadarg);

struct thread_data
{
	int mapper_sk;
};

struct thread_data thread_data_array[NUM_THREADS];
pthread_t threads[NUM_THREADS];
int number_of_mapper_done = 0;
int mapper_num = 0; // init by k

main()
{
    char buf[BUFSIZ];

    struct sockaddr_in remote;
    int sk;
    int ns_sk[NUM_THREADS];
    int i, j, k, l;
    struct hostent *hp, *gethostbyname();
    char mappers_str[BUFSIZ];
    char reducers_str[BUFSIZ];
    char helper_str[N][BUFSIZ];
    char helper_addr[N][128];
    char helper_port[N][10];
    char origin_file_name[100];
    char mapper_sent_str[BUFSIZ];

    char c;
    int sz;
    FILE *fp;

    // init with '\0'
    for (i = 0; i < BUFSIZ; i++)
        buf[i] = '\0';

    read(sk, buf, BUFSIZ); // get files dir path from client

    // get at most N helper_mappers, maybe less.
    // The actual number of acquired helpers is stored in the k

    lookup_ns(mappers_str, "mapper", N);
    printf("Get helper_map: %s\n", mappers_str);

    lookup_ns(reducers_str, "reducer", N);
    printf("Get helper_reducer: %s\n", reducers_str);

    i = 0, j = 0, k = 0;
    while ((c = mappers_str[i++]) != '\0')
    {
        if (c != '\n')
        {
            helper_str[k][j++] = c;
            continue;
        }
        if (j < 10) break; // in case non-vaild address string
        helper_str[k][j] = '\0';
        // printf("%d, helper_str: %s\n", j, helper_str);
        get_value_by_index(helper_addr[k], helper_str[k], 1);
        get_value_by_index(helper_port[k], helper_str[k], 2);

        j = 0;
        k++;
    }

    mapper_num = k;

    char random_str[12];
    char *tmp_file_path[k];
    char tmp_ref[k][20];


    for (i = 0; i < k; i++)
    {
    	// generate a random string to store splited files
        gen_random(random_str, 10);
    	sprintf(tmp_ref[i], "%s", random_str);
    	tmp_file_path[i] = tmp_ref[i];
    }


    split_file(tmp_file_path, buf, k);
    get_origin_name(origin_file_name, buf);

    // TODO: 	how to get the array of string provided by a function.
    // SOLOVED: do not use a function


    for (i = 0; i < k; i++)
    {
        printf("Addr %d: %s:%s, %s\n", i, helper_addr[i], helper_port[i], tmp_file_path[i]);

	    // TODO: send the file to helper_map by socket and keep track

	    ns_sk[i] = socket(AF_INET, SOCK_STREAM, 0);

	    remote.sin_family = AF_INET;

	    hp = gethostbyname(helper_addr[i]);
	    printf("address: %s\n", helper_addr[i]);

	    bcopy(hp->h_addr, (char *)&remote.sin_addr, hp->h_length);
	    remote.sin_port = atoi(helper_port[i]);
	    printf("port: %s\n", helper_port[i]);

	    connect(ns_sk[i], (struct sockaddr *)&remote, sizeof(remote));
	    sprintf(mapper_sent_str, "%d\n%s\n%s\n%s", k, tmp_file_path[i], origin_file_name, reducers_str);
	    printf("mapper_sent_str:\n");
	    printf("%s\n", mapper_sent_str);
	    write(ns_sk[i], mapper_sent_str, strlen(mapper_sent_str) + 1);



	    // TODO: Keep trace the helper and make sure they are
	    //       not dead. But HOW?

	    thread_data_array[i].mapper_sk = ns_sk[i];
	    int rc = 0;
    	rc = pthread_create(&threads[i], NULL, listening_mappers,
                            (void *) &thread_data_array[i]);
        if (rc)
        {
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
        }
        printf("thread: %d running\n", i);

	    // close(ns_sk[i]);



	    // TODO: Send reponse back to client
    }
}

void listening_mappers(void *threadarg) 
{

	int mapper_sk; 
	char buf[BUFSIZ];

    struct thread_data *my_data;
    my_data = (struct thread_data *) threadarg;

	mapper_sk = my_data->mapper_sk;
	while(1) {
		read(mapper_sk, buf, BUFSIZ);
		number_of_mapper_done++;
		if (number_of_mapper_done >= mapper_num) {
			write(0, "Indexing Done!", BUFSIZ);
		}
	}
}

void split_file(char **tmp_file_path, char *file_path, int k )
{

    char c;
    int i, j, l, sz;
    FILE *fp, *fpw[k];
    char tmp_path[50];
    // read the document requested by client

    sprintf(tmp_path, "./data/%s", file_path);
    printf("file: %s\n", tmp_path);
    fp = fopen(tmp_path, "r");

    // get the file size
    fseek(fp, 0L, SEEK_END);
    sz = ftell(fp);
    fseek(fp, 0L, SEEK_SET);

    i = 0, j = 0, l = 0;

    for (i = 0; i < k; i++)
    {
    	sprintf(tmp_path, "./tmp/%s", tmp_file_path[i]);
        fpw[i] = fopen(tmp_path, "w");
    }

    while ((c = fgetc(fp)) != EOF)
    {
    	if (i >= sz) break;
        fputc(c, fpw[l]);

        if (i % (sz / k) == 0 && i != 0)
        {
            // go till next space, in order to get a full word.
            while (c != ' ' && c != '\n' && c != EOF)
            {
                fputc(c, fpw[l]);
                c = fgetc(fp);
                i++;
            }
            l++;
        }
        i++;
    }
    fclose(fp);
    for (i = 0; i < k; i++)
    {
        fclose(fpw[i]);
    }
    printf("Split Done: \n");
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

void lookup_ns(char *return_str, char *type, int num)
{
    struct sockaddr_in remote;
    int sk;
    struct hostent *hp, *gethostbyname();
    char buf[BUFSIZ];
    char read_buf[BUFSIZ];
    char op_type[20];

    sk = socket(AF_INET, SOCK_STREAM, 0);

    remote.sin_family = AF_INET;
    hp = gethostbyname("localhost"); // name server address
    bcopy(hp->h_addr, (char *)&remote.sin_addr, hp->h_length);
    remote.sin_port = atoi("33444"); // name server port

    connect(sk, (struct sockaddr *)&remote, sizeof(remote));

    // look up string
    sprintf(buf, "op:lookup|type:%s|num:%d", type, num);
    write(sk, buf, strlen(buf) + 1);

    // get response
    read(sk, read_buf, BUFSIZ);
    int i;
    for (i = 0; i < strlen(read_buf); i++)
    {
        *return_str++ = read_buf[i];
    }
    return_str = read_buf;

    close(sk);
}

void get_origin_name(char * origin_file_name, char * path_name)
{
	int i = 0, j = 0;
    j = 0;
    for (i = 0; i < strlen(path_name); i++)
	{
		if (path_name[i] == '/') {
			j = 0;
			origin_file_name[0] = '\0';
			continue;
		}
		if (path_name[i] == '.') {
			break;
		}
		origin_file_name[j++] = path_name[i];
	}
	origin_file_name[j] = '\0';
}

void gen_random(char *s, const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

