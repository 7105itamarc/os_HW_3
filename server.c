#include "segel.h"
#include "request.h"
#include "log.h"

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//

struct Task {
    int connfd;
    struct timeval arrival_time;
};

// global vars:

// define all needed elements for producer/ consumer problem
pthread_mutex_t global_lock;
pthread_cond_t get_new_task; // when unhandled_tasks > 0
pthread_cond_t get_new_space; // when unhandled_tasks > MAX (q size)

struct Task* tasks_q;
int queue_size;
int tasks_p; // producer
int threads_p; // consumer
int unhandled_tasks; // gap
int udp_sd; // server's UDP socket to which we will/read data to/from


// Create the global server log
server_log global_log;

// TODO: HW3 — Extend getargs() to parse the full argument list. check V

// Parses command-line arguments
void getargs(int *port, int *udp_port, int *worker_threads_amount, int *queue_size, double *debug_sleep_time, int argc,
             char *argv[]) {
    if (argc < 6) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(1);
    }


    *port = atoi(argv[1]);
    *udp_port = atoi(argv[2]);
    *worker_threads_amount = atoi(argv[3]);
    *queue_size = atoi(argv[4]);
    *debug_sleep_time = atof(argv[5]);
    
    //check for the validity of all the provided server initialization arguments

    // TCP port argument check
    if(*port <= 1024){

        fprintf(stderr, "Error: TCP port must be above 1024.\n");
        exit(1);

    }

    // UDP port argument check
    if (*udp_port <= 1024 || *udp_port == *port){

        fprintf(stderr, "Error: UDP port must be above 1024 and different than Tcp_portnum.\n");
        exit(1);

    }

    // Worker threads amount argument check
    if (*worker_threads_amount <= 0){

        fprintf(stderr, "Error: Number of worker threads must be a positive integer.\n");
        exit(1);

    }

    // Queue size argument check
    if (*queue_size <= 0){

        fprintf(stderr, "Error: Queue size must be a positive integer.\n");
        exit(1);

    }

}



// TODO: HW3 — Task 1: Initialize the thread pool and request queue. check V
// This server currently handles all requests in the main thread.


// consumer protocol
void *worker_func(void *arg) {
    threads_stats t_state = (threads_stats)arg;
    time_stats timeStats;

    while (1) {
        pthread_mutex_lock(&global_lock);

        // trying to get a new task
        while (unhandled_tasks == 0 && t_state->pending_udp_list_head == NULL) {
            pthread_cond_wait(&get_new_task, &global_lock);
        }

        //handle pending worker's UDP pings
        if(t_state->pending_udp_list_head != NULL){

            //get the head UDP ping and advance our pings queue
            udp_ping_node* udp_ping_to_process = t_state->pending_udp_list_head;
            t_state->pending_udp_list_head = udp_ping_to_process->next;
            
            if(t_state->pending_udp_list_head == NULL){

                //pings queue got emptied
                t_state->pending_udp_list_tail = NULL;

            }
            
            //exctrat the ping's source address
            struct sockaddr_in clientaddr = udp_ping_to_process->clientaddr;

            pthread_mutex_unlock(&global_lock);
            
            // free the popped node's memory
            free(udp_ping_to_process);

            //write the requested thread statistics back to the client
            char threadStatsBuffer[MAXBUF];

            sprintf(threadStatsBuffer, "Stat-Thread-Id:: %d\r\nStat-Thread-Count:: %d\r\nStat-Thread-Static:: %d\r\nStat-Thread-Dynamic:: %d\r\nStat-Thread-Post:: %d\r\n",
                    t_state->id, t_state->total_req, t_state->stat_req, t_state->dynm_req, t_state->post_req);
            
            UDP_Write(udp_sd, &clientaddr, threadStatsBuffer, strlen(threadStatsBuffer));

            // continue looping and check if there are more UDP pings waiting for the thread
            continue; 

        }

        //record the time when a thread has took a queued job 
        struct timeval timeThreadTookJob;
        gettimeofday(&timeThreadTookJob, NULL);

        // if we are here unhandled_tasks > 0
        // save cur task - we will handel it outside the lock
        int curr_connfd = tasks_q[threads_p].connfd;
        struct timeval curr_arrival = tasks_q[threads_p].arrival_time;

        // update pointers
        threads_p = (threads_p + 1) % queue_size;
        unhandled_tasks--;

        // announce new task was created
        pthread_cond_signal(&get_new_space);

        //unlock
        pthread_mutex_unlock(&global_lock);

        //write all the time statistics that we have gathered
        timeStats.task_arrival = curr_arrival;
        timeStats.task_dispatch = timeThreadTookJob;

        // log times for now will be 0 (we will fill these in Task 5)
        timeStats.log_enter.tv_sec = 0;
        timeStats.log_enter.tv_usec = 0;
        timeStats.log_exit.tv_sec = 0;
        timeStats.log_exit.tv_usec = 0;

        // worker handel last task from tasks_list - Call the request handler
        // we do have jobs outSide the lock !
        // we will update with time_state and thread_state later
        requestHandle(curr_connfd, timeStats, t_state, global_log);

        // Close the connection
        Close(curr_connfd);
    }

    return NULL;
    
}

// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).


int main(int argc, char *argv[]) {

    int listenfd, connfd, port, clientlen;
    struct sockaddr_in clientaddr;

    // initial from the given starting params
    int worker_threads_amount = 0;
    int udp_port = 0;
    double debug_sleep_time = 0;


    pthread_mutex_init(&global_lock, NULL);
    pthread_cond_init(&get_new_task, NULL);
    pthread_cond_init(&get_new_space, NULL);

    // given args to initial with
    getargs(&port, &udp_port, &worker_threads_amount, &queue_size, &debug_sleep_time, argc, argv);

    //create the server's log with the provided debug sleep time
    global_log = create_log(debug_sleep_time);


    // create threads arr
    pthread_t *threads = malloc(sizeof(pthread_t) * worker_threads_amount);

    // initial tasks_q
    tasks_q = malloc(sizeof(struct Task) * queue_size);

    // create threads state arr
    threads_stats threads_s= malloc(sizeof(struct Threads_stats) * worker_threads_amount);

    for (int i = 0; i < worker_threads_amount; i++) {
        threads_s[i].id = i;          // Thread ID (placeholder)
        threads_s[i].stat_req = 0;    // Static request count
        threads_s[i].dynm_req = 0;    // Dynamic request count
        threads_s[i].post_req = 0;    // POST request count
        threads_s[i].total_req = 0;   // Total request count
        threads_s[i].pending_udp_list_head= NULL;   // thread's UDP ping queue
        threads_s[i].pending_udp_list_tail = NULL;  // thread's UDP ping queue

        pthread_create(&threads[i], NULL, worker_func, &threads_s[i]);
    }

    listenfd = Open_listenfd(port);

    //open the udp port
    udp_sd = UDP_Open(udp_port);

    fd_set read_set;
    int maxfd = (listenfd > udp_sd) ? listenfd : udp_sd;

    while (1) {

        FD_ZERO(&read_set);
        FD_SET(listenfd, &read_set);
        FD_SET(udp_sd, &read_set);

        //determine using select if new information arrived to our TCP/UDP sockets
        //prioritise the UDP socket first
        Select(maxfd + 1, &read_set, NULL, NULL, NULL);

        //check if a UDP ping has been sent
        if(FD_ISSET(udp_sd, &read_set)){

            struct sockaddr_in clientAddress;
            char messageBuffer[MAXLINE];

            //read MAXLINE bytes from the UDP socket into the message buffer
            int numOfReadBytes = UDP_Read(udp_sd, &clientAddress, messageBuffer, MAXLINE);

            if(numOfReadBytes > 0){

                //format the message we have read with null termination
                messageBuffer[numOfReadBytes] = '\0';

                //convert the UDP sent thread id into an int
                int targetThreadId = atoi(messageBuffer);
                
                if(targetThreadId >= 0 && targetThreadId < worker_threads_amount){

                    // add the UDP ping to the designated thread's queue
                    udp_ping_node* newUdpPingNode = malloc(sizeof(udp_ping_node));
                    newUdpPingNode->clientaddr = clientAddress;
                    newUdpPingNode->next = NULL;

                    //take the lock because we must modify the UDP pings queue 
                    //which can be accessed by the master thread synchronically as well

                    pthread_mutex_lock(&global_lock);
                    
                    if(threads_s[targetThreadId].pending_udp_list_tail == NULL) {

                        //the thread's UDP list is empty we will update the tail and head accordingly
                        threads_s[targetThreadId].pending_udp_list_head = newUdpPingNode;
                        threads_s[targetThreadId].pending_udp_list_tail = newUdpPingNode;

                    }else{

                        threads_s[targetThreadId].pending_udp_list_tail->next = newUdpPingNode;
                        threads_s[targetThreadId].pending_udp_list_tail = newUdpPingNode;

                    }
                    
                    //broadcast to all the worker threads so the designated thread will have a chance
                    //to wake up in case it sleeps and respond to the UDP ping

                    pthread_cond_broadcast(&get_new_task); 

                    //release the lock
                    pthread_mutex_unlock(&global_lock);

                }
            }
        }


        //after checking the UDP pings check if there are TCP jobs that need handaling

        if(FD_ISSET(listenfd, &read_set)){

            clientlen = sizeof(clientaddr);
            connfd = Accept(listenfd, (SA *) &clientaddr, (socklen_t *) &clientlen);

            //write the time that a request has arrived
            struct timeval arrival;
            gettimeofday(&arrival, NULL);

            // the producer protocol in the consumer/producer cycle
            pthread_mutex_lock(&global_lock);

            while (unhandled_tasks == queue_size) {
                pthread_cond_wait(&get_new_space,&global_lock);
            }

            // if we are here there is new space in the tasks_q
            // lets include the new task
            tasks_q[tasks_p].connfd =connfd; // saves the curr task_fd into the task in the queue
            tasks_q[tasks_p].arrival_time = arrival; //save the arrival time of the task

            // update pointers
            tasks_p = (tasks_p + 1) % queue_size;
            unhandled_tasks++;

            // announce new task was created
            pthread_cond_signal(&get_new_task);

            //unlock
            pthread_mutex_unlock(&global_lock);

        }

        
    }




    // TODO: HW3 — Record the request arrival time here.

    // TODO: HW3 — Add cleanup code for the thread pool and queue.
    // Clean up the server log before exiting
    destroy_log(global_log);
    free(threads_s);
    free(threads);
    free(tasks_q);

}
