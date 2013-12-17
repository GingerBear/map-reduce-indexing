all: client minigoogle name_service worker helper_map helper_reduce
        
client: 
        gcc -o client client.c -std=c99

minigoogle:
        gcc -o minigoogle minigoogle.c -std=c99

name_service:
        gcc -o name_service name_service.c -std=c99

worker:
        gcc -o worker worker.c -std=c99

helper_map:
        gcc -o helper_map helper_map.c -pthread -std=c99

helper_reduce:
        gcc -o helper_reduce helper_reduce.c -pthread -std=c99