/* example 6c.c */
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <netdb.h>
#define BUFSIZ 1024
#define MSG1 "I got your request"
#define MSG2 "Yes, sir"

main(argc, argv)
int argc;
char *argv[];
{
    char buf[BUFSIZ];
    struct sockaddr_in remote;
    int sk;
    int i;
    struct hostent *hp, *gethostbyname();
    char *path;

    sk = socket(AF_INET, SOCK_STREAM, 0);

    remote.sin_family = AF_INET;
    hp = gethostbyname(argv[1]);
    bcopy(hp->h_addr, (char *)&remote.sin_addr, hp->h_length);
    remote.sin_port = atoi(argv[2]);
    path = argv[3];

    printf("address: %s, port: %s, path: %s \n", argv[1], argv[2], path);
    connect(sk, (struct sockaddr *)&remote, sizeof(remote));

    /*Read input from stdin an send it to the server*/
    // for (i = 0; i < BUFSIZ; i++)
    //     buf[i] = '\0';
    // printf("Type the first message to be echoed\n");
    // while (read(0, buf, BUFSIZ) > 1)
    // {
    send(sk, path, strlen(path) + 1, 0);
    while(recv(sk, buf, BUFSIZ, 0) > 1) {
        printf("%s\n", buf);
    }

    //     for (i = 0; i < BUFSIZ; i++)
    //         buf[i] = '\0';
    //     printf("Type the next message to be echoed\n");
    // }
    // close(sk);
}