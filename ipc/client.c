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
#include <pthread.h>

#define GRID_SIZE 10
#define MAX_STRING_LEN 64
#define MAX_MSGS 10

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

// --- HELPERS ---
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

void msleep(int milliseconds) {
    struct timespec ts;
    ts.tv_sec = milliseconds / 1000;
    ts.tv_nsec = (milliseconds % 1000) * 1000000;
    nanosleep(&ts, NULL);
}

// --- GLOBAL STATE ---
int client_id_global;
mqd_t server_queue;
mqd_t my_queue;
char my_queue_name[64];
volatile int running = 1;
volatile int snapshot_taken = 0; 
pthread_mutex_t comm_lock = PTHREAD_MUTEX_INITIALIZER; 

// --- IPC HELPERS ---
void send_request(ipc_msg_t *msg) {
    msg->client_id = client_id_global;
    
    struct timespec tm;
    clock_gettime(CLOCK_REALTIME, &tm);
    tm.tv_sec += 2; 

    while (mq_timedsend(server_queue, (const char*)msg, sizeof(ipc_msg_t), 0, &tm) == -1) {
        if (errno == EINTR) continue;
        if (errno == ETIMEDOUT) {
            fprintf(stderr, "Client %d: Send timeout. Server busy/dead.\n", client_id_global);
            exit(1); 
        }
        perror("Client: mq_send failed");
        break; 
    }
}

int get_response(ipc_msg_t *resp) {
    struct timespec tm;
    clock_gettime(CLOCK_REALTIME, &tm);
    tm.tv_sec += 5; 

    while (1) {
        ssize_t sz = mq_timedreceive(my_queue, (char*)resp, sizeof(ipc_msg_t), NULL, &tm);
        if (sz >= 0) return 1;
        if (errno == EINTR) continue;
        if (errno == ETIMEDOUT) return 0;
        return 0;
    }
}

// --- SNAPSHOT LOGIC ---
void capture_snapshot() {
    char doc_buffer[4096]; 
    char words[GRID_SIZE][MAX_STRING_LEN];
    doc_buffer[0] = '\0';

    for (int i = 0; i < GRID_SIZE; i++) {
        int line_has_data = 0;
        for (int j = 0; j < GRID_SIZE; j++) {
            pthread_mutex_lock(&comm_lock);
            
            ipc_msg_t req;
            memset(&req, 0, sizeof(req));
            req.type = REQ_PRINT_READ;
            req.row = i;
            req.col = j;
            send_request(&req);

            ipc_msg_t resp;
            if (get_response(&resp)) {
                if (resp.type == RESP_SUCCESS) {
                    strncpy(words[j], resp.word, MAX_STRING_LEN);
                } else {
                    strcpy(words[j], "???"); 
                }
            } else {
                strcpy(words[j], "ERR");
            }
            pthread_mutex_unlock(&comm_lock);

            if (strlen(words[j]) > 0) line_has_data = 1;
        }

        if (line_has_data) {
            char line_buf[1024] = {0};
            int first_word = 1;
            for (int j = 0; j < GRID_SIZE; j++) {
                if (strlen(words[j]) > 0) {
                    if (!first_word) strcat(line_buf, " ");
                    strcat(line_buf, words[j]);
                    first_word = 0;
                }
            }
            strcat(line_buf, "\n");
            strcat(doc_buffer, line_buf);
        }
    }

    char filename[64];
    snprintf(filename, sizeof(filename), "output_client%d.txt", client_id_global);
    FILE* fp = fopen(filename, "w");
    if (fp) {
        fprintf(fp, "%s", doc_buffer);
        fclose(fp);
    }
    snapshot_taken = 1;
}

void* print_doc_thread(void* arg) {
    (void)arg;
    while (running) {
        sleep(2); 
        if (!running) break;
        capture_snapshot();
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <client_id>\n", argv[0]);
        return 1;
    }

    int client_id = atoi(argv[1]);
    client_id_global = client_id;
    printf("Client %d: Starting\n", client_id);

    // Setup Queues
    server_queue = mq_open(SERVER_QUEUE_NAME, O_WRONLY);
    if (server_queue == (mqd_t)-1) {
        sleep(1);
        server_queue = mq_open(SERVER_QUEUE_NAME, O_WRONLY);
        if (server_queue == (mqd_t)-1) {
            perror("Client: mq_open server failed");
            return 1;
        }
    }

    struct mq_attr attr;
    attr.mq_flags = 0;
    attr.mq_maxmsg = MAX_MSGS;
    attr.mq_msgsize = sizeof(ipc_msg_t);

    snprintf(my_queue_name, sizeof(my_queue_name), CLIENT_QUEUE_FMT, client_id_global);
    mq_unlink(my_queue_name); 

    my_queue = mq_open(my_queue_name, O_CREAT | O_RDONLY, 0666, &attr);
    if (my_queue == (mqd_t)-1) {
        perror("Client: mq_open self failed");
        return 1;
    }

    pthread_t printer;
    pthread_create(&printer, NULL, print_doc_thread, NULL);

    FILE *fp = fopen("input.txt", "r");
    if (!fp) {
        running = 0;
        pthread_join(printer, NULL);
        mq_unlink(my_queue_name);
        return 1;
    }

    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        int cmd_id;
        char command[32];
        if (sscanf(line, "C%d %s", &cmd_id, command) != 2) continue;
        if (cmd_id != client_id_global) continue;

        line[strcspn(line, "\n")] = 0;

        if (strcmp(command, "SLEEP") == 0) {
            int duration;
            if (sscanf(line, "C%d SLEEP %d", &cmd_id, &duration) == 2) {
                printf("Client %d: Sleeping for %dms\n", client_id_global, duration);
                msleep(duration);
            }
        }
        else if (strcmp(command, "WRITE") == 0) {
            int r, c, duration;
            char word[MAX_STRING_LEN];
            if (sscanf(line, "C%d WRITE %d %d %s %d", &cmd_id, &r, &c, word, &duration) == 5) {
                printf("Client %d: Requesting WRITE lock for (%d,%d)\n", client_id_global, r, c);
                
                pthread_mutex_lock(&comm_lock);
                ipc_msg_t req;
                memset(&req, 0, sizeof(req));
                req.type = REQ_WRITE;
                req.row = r; req.col = c; req.duration = duration;
                strncpy(req.word, word, MAX_STRING_LEN - 1);
                send_request(&req);
                ipc_msg_t resp;
                int success = 0;
                if (get_response(&resp) && resp.type == RESP_SUCCESS) success = 1;
                pthread_mutex_unlock(&comm_lock);
                
                if (success) {
                    printf("Client %d: WRITE(%d,%d) = '%s', sleeping for %dms\n", 
                           client_id_global, r, c, word, duration);
                    msleep(duration); 
                    
                    pthread_mutex_lock(&comm_lock);
                    req.type = REQ_WRITE_UNLOCK;
                    send_request(&req);
                    pthread_mutex_unlock(&comm_lock);
                    
                    printf("Client %d: WRITE(%d,%d) COMPLETED\n", client_id_global, r, c);
                } else {
                    printf("Client %d: WRITE(%d,%d) DROPPED\n", client_id_global, r, c);
                }
            }
        }
        else if (strcmp(command, "READ") == 0) {
            int r, c;
            if (sscanf(line, "C%d READ %d %d", &cmd_id, &r, &c) == 3) {
                printf("Client %d: Requesting READ lock for (%d,%d)\n", client_id_global, r, c);
                pthread_mutex_lock(&comm_lock);
                ipc_msg_t req;
                memset(&req, 0, sizeof(req));
                req.type = REQ_READ;
                req.row = r; req.col = c;
                send_request(&req);
                ipc_msg_t resp;
                int success = 0;
                char val[MAX_STRING_LEN];
                if (get_response(&resp) && resp.type == RESP_SUCCESS) {
                    success = 1;
                    strncpy(val, resp.word, MAX_STRING_LEN);
                }
                pthread_mutex_unlock(&comm_lock);
                if (success) printf("Client %d: READ(%d,%d) SUCCESS - Value: '%s'\n", client_id_global, r, c, val);
                else printf("Client %d: READ(%d,%d) DROPPED\n", client_id_global, r, c);
            }
        }
    }

    fclose(fp);
    running = 0;
    pthread_join(printer, NULL);

    if (!snapshot_taken) capture_snapshot();
    
    mq_close(server_queue);
    mq_close(my_queue);
    mq_unlink(my_queue_name);

    return 0;
}