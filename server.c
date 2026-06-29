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
    *debug_sleep_time = atof(argv[5]);}



// TODO: HW3 — Task 1: Initialize the thread pool and request queue. check V
// This server currently handles all requests in the main thread.


// consumer protocol
void *worker_func(void *arg) {
    threads_stats t_state = (threads_stats)arg;
    time_stats timeStats;

    while (1) {
        pthread_mutex_lock(&global_lock);

        // trying to get a new task
        while (unhandled_tasks == 0) {
            pthread_cond_wait(&get_new_task, &global_lock);
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

    global_log = create_log();

    pthread_mutex_init(&global_lock, NULL);
    pthread_cond_init(&get_new_task, NULL);
    pthread_cond_init(&get_new_space, NULL);

    // given args to initial with
    getargs(&port, &udp_port, &worker_threads_amount, &queue_size, &debug_sleep_time, argc, argv);

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

        pthread_create(&threads[i], NULL, worker_func, &threads_s[i]);
    }

    listenfd = Open_listenfd(port);

    while (1) {
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




    // TODO: HW3 — Record the request arrival time here.

    // TODO: HW3 — Add cleanup code for the thread pool and queue.
    // Clean up the server log before exiting
    destroy_log(global_log);
    free(threads_s);
}
