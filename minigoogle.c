/*************************************************************************/
/* This program creates a well-known Internet domain socket.             */
/* Each time it accepts a connection, it forks a child to print          */
/* out a message from the client. It then goes on to accept new          */
/* connections. The child executes recho.                                */
/* To execute this program :                                             */
/*                                                                       */
/* At the local  machine:                                                */
/* 1. cc -o ex6a ex6a.c OR gcc -o ex6a ex6a.c -lsocket                   */
/* 2. cc -o recho ex6b.c OR gcc -o recho ex6b.c                          */
/*                                                                       */
/* At the remote machine:                                                */
/* 3. cc -o ex6c ex6c.c OR gcc -o ex6c ex6c.c -lsocket -lnsl             */
/* (-lsocket and -lnsl may not be required for gcc in all machines)      */
/*                                                                       */
/* local> ex6a (or ./ex6a)                                               */
/*         socket has port 1892                                          */
/*                      remote> ex6c (or ./ex6c) local.cs.pitt.edu 1892  */
/* (where local.cs.pitt.edu represents the hostname running ex6a, such   */
/*   as unixs4.cis.pitt.edu)                                             */
/*************************************************************************/

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>

main()
{
    struct sockaddr_in local;
    int sk, len = sizeof(local), rsk;

    /* Create an internet domain stream socket */
    sk = socket(AF_INET, SOCK_STREAM, 0);

    /*Construct and bind the name using default values*/
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = INADDR_ANY;
    local.sin_port = 9002;
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

        if (fork() == 0) /*Create one child to serve each client*/
        {
            dup2(rsk, 0); /*Connect the new connection to "stdin"*/
            execl("worker", "worker", 0);
        }
        else
            close(rsk);
    }
}
