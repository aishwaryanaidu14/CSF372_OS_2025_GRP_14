#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <stdarg.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <mqueue.h>
#include <signal.h>

#define GRID_SIZE 10
#define MAX_STRING_LEN 64
#define MAX_CLIENTS 10
#define MAX_MSG_SIZE 1024
#define MAX_MSGS 10
#define IDLE_TIMEOUT_SEC 10

// --- SHARED PROTOCOL ---
#define SERVER_QUEUE_NAME   "/osdoc_server_queue"
#define CLIENT_QUEUE_FMT    "/osdoc_client_queue_%d"

typedef enum {
    REQ_READ, REQ_WRITE, REQ_WRITE_UNLOCK, REQ_PRINT_READ,
    RESP_SUCCESS, RESP_DENIED, RESP_ERROR
} op_type_t;

typedef struct {
    int client_id;
    op_type_t type;
    int row;
    int col;
    char word[MAX_STRING_LEN];
    int duration;
} ipc_msg_t;

// --- SERVER STATE ---
typedef struct {
    char data[MAX_STRING_LEN];
    int locked_by; 
} Cell;

Cell grid[GRID_SIZE][GRID_SIZE];
mqd_t server_queue = (mqd_t)-1;
mqd_t client_queues[MAX_CLIENTS]; 

// --- HELPER FUNCTIONS ---
static void ts_printf(const char *fmt, ...) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm tm_info;
    localtime_r(&tv.tv_sec, &tm_info);
    char tbuf[32];
    strftime(tbuf, sizeof(tbuf), "%H:%M:%S", &tm_info);
    fprintf(stdout, "%s.%03ld ", tbuf, tv.tv_usec / 1000);
    va_list args;
    va_start(args, fmt);
    vfprintf(stdout, fmt, args);
    va_end(args);
    fflush(stdout);
}
#define printf(...) ts_printf(__VA_ARGS__)

void cleanup() {
    static int cleaned = 0;
    if (cleaned) return;
    cleaned = 1;
    printf("Server: Shutdown complete\n");
    if (server_queue != (mqd_t)-1) {
        mq_close(server_queue);
        mq_unlink(SERVER_QUEUE_NAME);
    }
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (client_queues[i] != (mqd_t)-1) mq_close(client_queues[i]);
    }
    FILE *fp = fopen("output.txt", "w");
    if (fp) {
        for (int i = 0; i < GRID_SIZE; i++) {
            int has_data = 0;
            for (int j = 0; j < GRID_SIZE; j++) if (strlen(grid[i][j].data) > 0) has_data = 1;
            if (has_data) {
                int first_word = 1;
                for (int j = 0; j < GRID_SIZE; j++) {
                    if (strlen(grid[i][j].data) > 0) {
                        if (!first_word) fprintf(fp, " ");
                        fprintf(fp, "%s", grid[i][j].data);
                        first_word = 0;
                    }
                }
                fprintf(fp, "\n");
            }
        }
        fclose(fp);
    }
}

void handle_sigint(int sig) { (void)sig; exit(0); }

mqd_t get_client_queue(int client_id) {
    if (client_id < 0 || client_id >= MAX_CLIENTS) return (mqd_t)-1;
    if (client_queues[client_id] == (mqd_t)-1) {
        char q_name[64];
        snprintf(q_name, sizeof(q_name), CLIENT_QUEUE_FMT, client_id);
        // CRITICAL: O_NONBLOCK prevents server freeze if client queue is full
        client_queues[client_id] = mq_open(q_name, O_WRONLY | O_NONBLOCK);
    }
    return client_queues[client_id];
}

void send_response(int client_id, op_type_t type, const char* payload) {
    mqd_t q = get_client_queue(client_id);
    if (q == (mqd_t)-1) return; 

    ipc_msg_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = type;
    msg.client_id = -1; 
    if (payload) strncpy(msg.word, payload, MAX_STRING_LEN - 1);

    // Send with retry on EINTR, ignore EAGAIN (Queue Full)
    while (mq_send(q, (const char*)&msg, sizeof(msg), 0) == -1) {
        if (errno == EINTR) continue;
        break; 
    }
}

int main() {
    setvbuf(stdout, NULL, _IONBF, 0); 
    signal(SIGINT, handle_sigint);
    signal(SIGTERM, handle_sigint);
    atexit(cleanup);

    for (int i = 0; i < GRID_SIZE; i++) {
        for (int j = 0; j < GRID_SIZE; j++) {
            grid[i][j].locked_by = -1;
            memset(grid[i][j].data, 0, MAX_STRING_LEN);
        }
    }
    for (int i = 0; i < MAX_CLIENTS; i++) client_queues[i] = (mqd_t)-1;

    struct mq_attr attr;
    attr.mq_flags = 0;
    attr.mq_maxmsg = MAX_MSGS;
    attr.mq_msgsize = sizeof(ipc_msg_t);
    attr.mq_curmsgs = 0;

    mq_unlink(SERVER_QUEUE_NAME); 
    server_queue = mq_open(SERVER_QUEUE_NAME, O_CREAT | O_RDONLY, 0644, &attr);
    if (server_queue == (mqd_t)-1) {
        perror("Server: mq_open failed");
        return 1;
    }

    printf("Server: Started\n");

    ipc_msg_t msg;
    while (1) {
        // TIMED RECEIVE: Auto-shutdown if idle for 10 seconds
        struct timespec tm;
        clock_gettime(CLOCK_REALTIME, &tm);
        tm.tv_sec += IDLE_TIMEOUT_SEC;

        ssize_t bytes_read = mq_timedreceive(server_queue, (char*)&msg, sizeof(msg), NULL, &tm);
        
        if (bytes_read < 0) {
            if (errno == EINTR) continue;
            if (errno == ETIMEDOUT) break; // Idle timeout -> Exit loop -> Cleanup
            perror("Server: mq_receive failed");
            break;
        }

        int id = msg.client_id;
        int r = msg.row;
        int c = msg.col;

        if (r < 0 || r >= GRID_SIZE || c < 0 || c >= GRID_SIZE) {
            send_response(id, RESP_ERROR, "Bounds Error");
            continue;
        }

        switch (msg.type) {
            case REQ_WRITE:
                if (grid[r][c].locked_by != -1 && grid[r][c].locked_by != id) {
                    printf("Server: Client %d WRITE LOCK(%d,%d) DENIED\n", id, r, c);
                    send_response(id, RESP_DENIED, NULL);
                } else {
                    grid[r][c].locked_by = id;
                    strncpy(grid[r][c].data, msg.word, MAX_STRING_LEN - 1);
                    printf("Server: Client %d WRITE LOCK(%d,%d) GRANTED\n", id, r, c);
                    send_response(id, RESP_SUCCESS, NULL);
                }
                break;
            case REQ_WRITE_UNLOCK:
                if (grid[r][c].locked_by == id) {
                    grid[r][c].locked_by = -1;
                    printf("Server: Client %d UNLOCK(%d,%d)\n", id, r, c);
                }
                break;
            case REQ_READ:
                if (grid[r][c].locked_by != -1) {
                    printf("Server: Client %d READ LOCK(%d,%d) DENIED\n", id, r, c);
                    send_response(id, RESP_DENIED, NULL);
                } else {
                    printf("Server: Client %d READ LOCK(%d,%d) GRANTED\n", id, r, c);
                    send_response(id, RESP_SUCCESS, grid[r][c].data);
                }
                break;
            case REQ_PRINT_READ:
                if (grid[r][c].locked_by != -1) {
                    printf("Server: Client %d PRINT_DOC READ(%d,%d) DROPPED\n", id, r, c);
                    send_response(id, RESP_DENIED, NULL);
                } else {
                    send_response(id, RESP_SUCCESS, grid[r][c].data);
                }
                break;
            default: break;
        }
    }
    return 0;
}