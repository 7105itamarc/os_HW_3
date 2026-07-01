#ifndef SERVER_LOG_H
#define SERVER_LOG_H

// TODO:
// Implement a thread-safe server log system.
// - The log should support concurrent access from multiple threads.
// - You must implement a multiple-readers/single-writer synchronization model.
// - Writers must have priority over readers.
//   This means that if a writer is waiting, new readers should be blocked until the writer is done.
// - Use appropriate synchronization primitives (e.g., pthread mutexes and condition variables).
// - The log should allow appending entries and returning the full log content.

typedef struct Server_Log* server_log;

//forward declaration
struct Threads_stats;
struct Time_stats;

// Creates a new server log instance with the provided debug sleep time
server_log create_log(double debug_sleep_time);

// Destroys and frees the log
void destroy_log(server_log log);

// Returns the log contents as a string (null-terminated)
// NOTE: caller is responsible for freeing dst
int get_log(server_log log, char** dst, struct Time_stats* tm_stats);

// Appends a new entry to the log
void add_to_log(server_log log, struct Threads_stats* t_stats, struct Time_stats* tm_stats);

#endif // SERVER_LOG_H
