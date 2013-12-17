/*************************************************************************/

// This program running on naming server. It will keep running and waiting for
// helpers to register when they are ready to server. It will also provide
// worker with function to lookup availble helpers. All registeration data
// is stored on file in name server.

/*************************************************************************/

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#define BUFSIZ 1024

void get_value_by_index(char *s, char *str, int index);
void remove_op(char *s, char *str);
int get_num(char *s);
void lookup(char * result, char * lookup_type, int rsk, int helper_num);
int match_type(char *str, char *type);

main()
{
    struct sockaddr_in local;
    int sk, len = sizeof(local), rsk, i;

    char buf[BUFSIZ];
    char op_type[20];
    char lookup_type[20];
    char store_no_op[300];
    char *line;
    size_t len_f = 0;
    ssize_t read_f;
    FILE *fp;

    /* Create an internet domain stream socket */
    sk = socket(AF_INET, SOCK_STREAM, 0);

    /*Construct and bind the name using default values*/
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = INADDR_ANY;
    local.sin_port = 33444;
    bind(sk, (struct sockaddr *)&local, sizeof(local));

    /*Find out and publish socket name */
    getsockname(sk, (struct sockaddr *)&local, &len);
    printf("Socket has port %d\n", local.sin_port);

    /* Start accepting connections */
    /*Declare willingness to accept a connection*/
    listen(sk, 5);
    while (1)
    {
        rsk = accept(sk, 0, 0); /*Accept new request for a connection*/

        read(rsk, buf, BUFSIZ);
        get_value_by_index(op_type, buf, 0);
        get_value_by_index(lookup_type, buf, 1);
        printf("|%s|\n", op_type);
        printf("%s\n", buf);
        if (strcmp(op_type, "register") == 0)
        {
            fp = fopen("./name_data", "a+");
            remove_op(store_no_op, buf);
            fprintf(fp, "%s\n", store_no_op);
            fclose(fp);
        }
        else if (strcmp(op_type, "lookup") == 0)
        {
            char result[BUFSIZ];
            int helper_num;
            helper_num = get_num(buf);
            lookup(result, lookup_type, rsk, helper_num);
            write(rsk, result, BUFSIZ);
            result[0] = '\0';
        }
    }
}

void lookup(char * result, char * lookup_type, int rsk, int helper_num)
{
    FILE *fp;
    char line[BUFSIZ];
    printf("Helper num: %d\n", helper_num);
    fp = fopen("./name_data", "r");
    // TODO: should check type first
    int i = 0, j = 0;
    while (fgets(line, sizeof(line), fp))
    {
        if (i >= helper_num) break;
        if (match_type(line, lookup_type) > 0) {
            strcat(result, line);
            i++;
        }
    }
    printf("Result: %s\n", result);
    fclose(fp);
}

int get_num(char *s)
{
    char helper_num_s[10];
    int helper_num;
    get_value_by_index(helper_num_s, s, 2);
    helper_num = atoi(helper_num_s);
    return helper_num;
}

int match_type(char *str, char *type)
{
    int i = 0, j = 0, count = 0;
    char tmp1[10];
    char c;
    while ((c = str[i]) != '\0')
    {
        if (c == '|')
        {
            tmp1[j] = '\0';
            return (strcmp(tmp1, type) == 0);
        }
        if (count == 1)
            tmp1[j++] = c;
        if (c == ':')
            count++;
        i++;
    }
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

void remove_op(char *s, char *str)
{
    //char s[20];
    int i = 0, j = 0, c = 0;
    for (i = 0; i < BUFSIZ && str[i] != '\0'; i++)
    {
        if (c >= 1)
            s[j++] = str[i];
        if (str[i] == '|')
            c++;
    }
    s[j] = '\0';
}