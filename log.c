#include <stdlib.h>
#include <string.h>
#include "log.h"
#include "segel.h"

#include <pthread.h>


// Opaque struct definition
struct Server_Log {
    // TODO: Implement internal log storage (e.g., dynamic buffer, linked list, etc.)
    //filed
    int readers_inside, writers_inside, readers_waiting, writers_waiting;
    // sync filed
    pthread_cond_t read_allowed;
    pthread_cond_t write_allowed;
    pthread_mutex_t read_write_lock;
    // storage - dynamic buffer
    char *dynamic_buffer;
    int curr_size; // for saving O(N) using strlen during lock
};

// Creates a new server log instance (stub)
server_log create_log() {
    // TODO: Allocate and initialize internal log structure
    server_log log = (server_log) malloc(sizeof(struct Server_Log));
    if (!log) { return NULL; } // if malloc fails

    log->readers_inside = 0;
    log->writers_inside = 0;
    log->readers_waiting = 0;
    log->writers_waiting = 0;

    pthread_mutex_init(&log->read_write_lock, NULL);
    pthread_cond_init(&log->read_allowed, NULL);
    pthread_cond_init(&log->write_allowed, NULL);

    // TODO: Allocate memory for the actual text log later
    log->dynamic_buffer = malloc(1);
    log->dynamic_buffer[0] = '\0';
    log->curr_size = 0;

    return log;
}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    pthread_mutex_destroy(&log->read_write_lock);
    pthread_cond_destroy(&log->read_allowed);
    pthread_cond_destroy(&log->write_allowed);
    // TODO: Free the actual text log memory here
    free(log->dynamic_buffer);
    free(log);
}

// readers logic
// Returns dummy log content as string (stub)
int get_log(server_log log, char **dst) {
    // lock
    pthread_mutex_lock(&log->read_write_lock);

    log->readers_waiting++;
    while (log->writers_inside > 0 || log->writers_waiting > 0) {
        pthread_cond_wait(&log->read_allowed, &log->read_write_lock);
    }

    // if we are here writers_waiting == 0 -> readers can read
    log->readers_waiting--;
    log->readers_inside++;

    pthread_mutex_unlock(&log->read_write_lock);
    // todo: [do_reading_action]
    int log_len = log->curr_size;
    char *log_copy = malloc(log_len + 1);
    if (log_copy != NULL) // if malloc didn't fail
    {
        strcpy(log_copy, log->dynamic_buffer);
    }
    *dst = log_copy;

    pthread_mutex_lock(&log->read_write_lock);

    log->readers_inside--;

    if (log->readers_inside == 0 && log->writers_waiting > 0) {
        pthread_cond_signal(&log->write_allowed);
    }
    pthread_mutex_unlock(&log->read_write_lock);
    return log_len;
}

// writer logics
// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char *data, int data_len) {
    //locking before starts
    pthread_mutex_lock(&log->read_write_lock);

    log->writers_waiting++;
    while (log->readers_inside + log->writers_inside > 0) {
        // currently occupied
        pthread_cond_wait(&log->write_allowed, &log->read_write_lock);
    }

    // if we are here writers_waiting == 0 -> do writing action
    log->writers_waiting--;
    log->writers_inside++;

    // todo: [do writing action]
    int allocate_size = log->curr_size + data_len + 1 + 1; // new size + '#' + '\0'
    log->dynamic_buffer = realloc(log->dynamic_buffer, allocate_size);
    strcat(log->dynamic_buffer, data);
    strcat(log->dynamic_buffer, "#");
    log->curr_size = allocate_size - 1; //real size without '\0'

    log->writers_inside--;
    if (log->writers_waiting > 0) {
        pthread_cond_signal(&log->write_allowed);
    } else {
        pthread_cond_broadcast(&log->read_allowed);
    }
    pthread_mutex_unlock(&log->read_write_lock);
}
