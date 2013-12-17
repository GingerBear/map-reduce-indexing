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

#define BUFSIZ 1024
#define N 10 // max number of mappers
#define NUM_THREADS 5
#define CMD_LEN 1024

void partition_result_to_master(char *reduce_file_name);
int compare_first_term(char *str1, char *str2);
int operate_ns(char *op, char *type, char *address, char *port);
void merge_into_master(void *threadarg);

struct thread_data
{
    char *reduce_file_name;
    int  start_line;
    int  end_line;
};

int number_of_thread_done = 0;
int expect_map_num = 0;
int rsk[N];

main()
{
    struct sockaddr_in local;
    int i, j, k;
    int sk, len = sizeof(local);
    char index_num_s[10];
    int index_num = 0;
    char expect_map_num_s[10];
    int curr_map_num = 0;
    char origin_file_name[100];
    char reduce_file_name[100];
    char back_words[BUFSIZ];
    char c;

    char sep_file[N][50];

    char buf[BUFSIZ];

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

    operate_ns("register", "reducer", "localhost", local.sin_port);

    /* Start accepting connections */
    /*Declare willingness to accept a connection*/
    listen(sk, 5);

    char seperate_files[200];
    char cmd[CMD_LEN];
    while (1)
    {
        rsk[curr_map_num] = accept(sk, 0, 0); /*Accept new request for a connection*/





        // TODO: receive the file and store it on helper local disk
        //       Do word count and combine the result.


        read(rsk[curr_map_num], buf, BUFSIZ);

        i = 0;

        j = 0;
        // get index number of current reduce
        while ((c = buf[i++]) != '\n')
        {
            index_num_s[j++] = c;
        }
        index_num_s[j] = '\0';
        index_num = atoi(index_num_s);

        // Assume there are 4 reducers and 4 splits of master index
        printf("index_of_reduce: %d\n", index_num);


        j = 0;
        // get expected number of seperated file
        while ((c = buf[i++]) != '\n')
        {
            expect_map_num_s[j++] = c;
        }
        expect_map_num_s[j] = '\0';
        expect_map_num = atoi(expect_map_num_s);
        printf("curr_map_num: %d, expect_map_num: %d\n", curr_map_num, expect_map_num);

        j = 0;
        // get the original file name;
        while ((c = buf[i++]) != '\n')
        {
            origin_file_name[j++] = c;
        }
        origin_file_name[j] = '\0';
        printf("origin_file_name: %s\n", origin_file_name);

        j = 0;
        // parse reducer address
        while ((c = buf[i++]) != '\n')
        {
            sep_file[curr_map_num][j++] = c;
        }
        sep_file[curr_map_num][j] = '\0';

        printf("sep_file: %s\n", sep_file[curr_map_num]);

        curr_map_num++;

        if (curr_map_num >= expect_map_num)
        {
            printf("All seperated files from mapper received.\n");


            // do reducing here


            for (i = 0; i < expect_map_num; i++)
            {
                sprintf(seperate_files, "%s ./tmp/%s", seperate_files, sep_file[i]);
            }

            sprintf(reduce_file_name, "./tmp/%s_%d", origin_file_name, index_num);


            // do reduce on file received, by shell
            sprintf(cmd, "cat %s | awk '{arr[$1]+=$2;} END { for (i in arr) print i, \"%s\", arr[i]}' | sort > %s\n", seperate_files, origin_file_name, reduce_file_name);
            printf("%s\n", cmd);
            system(cmd);

            printf("Reduce Done, Sending to master index\n");

            partition_result_to_master(reduce_file_name);

            // restore
            seperate_files[0] = '\0';
            curr_map_num = 0;

        }
    }
}

void partition_result_to_master(char *reduce_file_name)
{

    // partition file to 5 parts, for each to create a thread to work on.
    FILE *fp;
    char c;
    int total_line = 0;
    int i = 0, j = 0, step = 0, k = 5, l = 0; // partition into 5
    struct thread_data thread_data_array[NUM_THREADS];
    pthread_t threads[NUM_THREADS];
    int rc;

    fp = fopen(reduce_file_name, "r");
    while ((c = fgetc(fp)) != EOF)
    {
        if (c == '\n')
            total_line++;
    }
    fclose(fp);
    printf("Line Number: %d\n", total_line);

    step = (total_line - 1 + k) / k; // round up

    for (i = 0; i < total_line; i++)
    {
        if (i == 0) continue;
        if ((i + 1) % step == 0 || (i + 1) == total_line)
        {
            thread_data_array[l].reduce_file_name = reduce_file_name;
            thread_data_array[l].start_line = j;
            thread_data_array[l].end_line = i;
            // Call funciton by thread

            //merge_into_master(reduce_file_name, j, i);
            rc = pthread_create(&threads[l], NULL, merge_into_master,
                                (void *) &thread_data_array[l]);
            if (rc)
            {
                printf("ERROR; return code from pthread_create() is %d\n", rc);
                exit(-1);
            }
            printf("thread: %d running\n", l);
            j = i + 1;
            l++;
        }
    }
}

void merge_into_master(void *threadarg)
{
    struct thread_data *my_data;
    my_data = (struct thread_data *) threadarg;

    char reduce_file_name[100];
    int start_line;
    int end_line;

    sprintf(reduce_file_name, "%s", my_data->reduce_file_name);
    start_line = my_data->start_line;
    end_line = my_data->end_line;

    FILE *fp[27];
    FILE *fp_r;
    char tmp[10];
    char line[200];
    char *alpha = "abcdefghijklmnopqrstuvwxyz";
    int index_of_char;
    int i, j = 0;

    for (i = 0; i < 26; i++)
    {
        sprintf(tmp, "./index/%c", alpha[i]);
        fp[i] = fopen(tmp, "a+");
    }
    printf("File open done...\n");
    fp_r = fopen(reduce_file_name, "r");
    printf("Start writing...\n");

    while (fgets(line, sizeof(line), fp_r) != NULL)
    {
        if (j < start_line)
        {
            j++;
            continue;
        }
        if (j > end_line) break;
        index_of_char = line[0] - 'a';
        fprintf(fp[index_of_char], "%s", line);
        j++;
    }

    printf("Merge from %d - %d done...\n", start_line, end_line);

    fclose(fp_r);

    for (i = 0; i < 26; i++)
    {
        fclose(fp[i]);
    }
    // add global number of thread 
    number_of_thread_done++;

    // if all finished, call back to all mappers
    if (number_of_thread_done >= NUM_THREADS) {
        printf("All Threads done!\n");

        // send word back to mapper
        char back_words[BUFSIZ];

        sprintf(back_words, "status:done|type:reducer\n");
        for (i = 0; i < expect_map_num; i++)
        {
            write(rsk[i], back_words, BUFSIZ);
        }
        number_of_thread_done = 0;

    }
    pthread_exit(NULL);
}

int compare_first_term(char *str1, char *str2)
{
    int i = 0;
    char c;
    char term1[50];
    char term2[50];
    while ((c = str1[i]) != '\0')
    {
        if (c == ' ')
            break;
        term1[i++] = c;
    }
    while ((c = str2[i]) != '\0')
    {
        if (c == ' ')
            break;
        term2[i++] = c;
    }
    return strcmp(term1, term2);
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
    printf("registering helpers: %s\n", buf);
    write(sk, buf, strlen(buf) + 1);
    // printf("%s",buf);
    printf("register Done.\n");
    close(sk);
    return 1;
}