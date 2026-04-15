/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* --- Pipe reader argument --- */
typedef struct {
    int              read_fd;
    char             container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} pipe_reader_arg_t;

/* --- Signal handling --- */
static volatile sig_atomic_t supervisor_should_stop = 0;
static supervisor_ctx_t     *global_ctx = NULL;

static void sigchld_handler(int sig) { (void)sig; }

static void sigterm_handler(int sig)
{
    (void)sig;
    supervisor_should_stop = 1;
    if (global_ctx)
        global_ctx->should_stop = 1;
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
 
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */

void *logging_thread(void *arg)
{
    bounded_buffer_t *buf = (bounded_buffer_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(buf, &item) == 0) {
        char path[PATH_MAX];
        FILE *fp;
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fp = fopen(path, "a");
        if (fp) {
            fwrite(item.data, 1, item.length, fp);
            fclose(fp);
        }
    }
    fprintf(stderr, "[logger] Thread exiting.\n");
    return NULL;
}

/* Pipe reader thread — one per container */
static void *pipe_reader_thread(void *arg)
{
    pipe_reader_arg_t *r = (pipe_reader_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    while ((n = read(r->read_fd, item.data, LOG_CHUNK_SIZE)) > 0) {
        strncpy(item.container_id, r->container_id, CONTAINER_ID_LEN - 1);
        item.container_id[CONTAINER_ID_LEN - 1] = '\0';
        item.length = (size_t)n;
        if (bounded_buffer_push(r->buffer, &item) != 0)
            break;
    }
    close(r->read_fd);
    free(r);
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Redirect stdout/stderr to logging pipe */
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    /* Set container hostname (UTS namespace) */
    sethostname(cfg->id, strlen(cfg->id));

    /* Apply nice value */
    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    /* chroot into rootfs */
    if (chroot(cfg->rootfs) != 0) { perror("chroot"); return 1; }
    if (chdir("/") != 0)          { perror("chdir");  return 1; }

    /* Mount /proc */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc");
        return 1;
    }

    printf("Container %s starting: %s\n", cfg->id, cfg->command);
    fflush(stdout);

    char *argv[] = { "/bin/sh", "-c", cfg->command, NULL };
    execv("/bin/sh", argv);
    perror("execv");
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* --- Spawn a new container --- */
static int spawn_container(supervisor_ctx_t *ctx, const control_request_t *req)
{
    child_config_t *ccfg;
    container_record_t *rec;
    char *stack;
    int pfd[2];
    pid_t pid;

    if (pipe(pfd) < 0) { perror("pipe"); return -1; }

    ccfg = calloc(1, sizeof(*ccfg));
    if (!ccfg) { close(pfd[0]); close(pfd[1]); return -1; }
    strncpy(ccfg->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(ccfg->rootfs, req->rootfs, PATH_MAX - 1);
    strncpy(ccfg->command, req->command, CHILD_COMMAND_LEN - 1);
    ccfg->nice_value   = req->nice_value;
    ccfg->log_write_fd = pfd[1];

    stack = malloc(STACK_SIZE);
    if (!stack) { free(ccfg); close(pfd[0]); close(pfd[1]); return -1; }

    pid = clone(child_fn, stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD, ccfg);
    if (pid < 0) {
        perror("clone");
        free(stack); free(ccfg); close(pfd[0]); close(pfd[1]);
        return -1;
    }

    close(pfd[1]);

    rec = calloc(1, sizeof(*rec));
    if (!rec) { close(pfd[0]); return -1; }
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->host_pid         = pid;
    rec->started_at       = time(NULL);
    rec->state            = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, req->container_id,
                              pid, req->soft_limit_bytes, req->hard_limit_bytes);

    {
        pipe_reader_arg_t *ra = malloc(sizeof(*ra));
        if (ra) {
            pthread_t tid;
            ra->read_fd = pfd[0];
            strncpy(ra->container_id, req->container_id, CONTAINER_ID_LEN - 1);
            ra->container_id[CONTAINER_ID_LEN - 1] = '\0';
            ra->buffer = &ctx->log_buffer;
            if (pthread_create(&tid, NULL, pipe_reader_thread, ra) == 0)
                pthread_detach(tid);
            else { close(pfd[0]); free(ra); }
        } else {
            close(pfd[0]);
        }
    }

    fprintf(stderr, "[supervisor] Container '%s' started, PID %d\n",
            req->container_id, pid);
    return 0;
}

/* --- Reap exited children --- */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *r;
        pthread_mutex_lock(&ctx->metadata_lock);
        for (r = ctx->containers; r; r = r->next) {
            if (r->host_pid == pid) {
                if (WIFEXITED(status)) {
                    r->state     = CONTAINER_EXITED;
                    r->exit_code = WEXITSTATUS(status);
                } else if (WIFSIGNALED(status)) {
                    r->exit_signal = WTERMSIG(status);
                    r->state = (r->exit_signal == SIGKILL)
                                   ? CONTAINER_KILLED : CONTAINER_STOPPED;
                }
                fprintf(stderr, "[supervisor] '%s' (PID %d) -> %s\n",
                        r->id, pid, state_to_string(r->state));
                break;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* --- Handle `ps` command --- */
static void handle_ps(supervisor_ctx_t *ctx, int cfd)
{
    control_response_t resp;
    container_record_t *r;
    int off = 0;

    memset(&resp, 0, sizeof(resp));
    pthread_mutex_lock(&ctx->metadata_lock);
    off += snprintf(resp.message + off, CONTROL_MESSAGE_LEN - off,
                    "%-12s %-8s %-10s %-10s %-10s %-6s\n",
                    "ID", "PID", "STATE", "SOFT(MB)", "HARD(MB)", "EXIT");
    for (r = ctx->containers; r && off < CONTROL_MESSAGE_LEN - 120; r = r->next)
        off += snprintf(resp.message + off, CONTROL_MESSAGE_LEN - off,
                        "%-12s %-8d %-10s %-10lu %-10lu %-6d\n",
                        r->id, r->host_pid, state_to_string(r->state),
                        r->soft_limit_bytes >> 20, r->hard_limit_bytes >> 20,
                        r->exit_code);
    pthread_mutex_unlock(&ctx->metadata_lock);
    write(cfd, &resp, sizeof(resp));
}

/* --- Handle `logs` command --- */
static void handle_logs(supervisor_ctx_t *ctx, int cfd, const char *id)
{
    control_response_t resp;
    char path[PATH_MAX];
    FILE *fp;
    (void)ctx;

    memset(&resp, 0, sizeof(resp));
    snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, id);
    fp = fopen(path, "r");
    if (!fp) {
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "No log file for '%s'", id);
    } else {
        fread(resp.message, 1, CONTROL_MESSAGE_LEN - 1, fp);
        fclose(fp);
    }
    write(cfd, &resp, sizeof(resp));
}

/* --- Handle `stop` command --- */
static void handle_stop(supervisor_ctx_t *ctx, int cfd, const char *id)
{
    control_response_t resp;
    container_record_t *r;

    memset(&resp, 0, sizeof(resp));
    pthread_mutex_lock(&ctx->metadata_lock);
    for (r = ctx->containers; r; r = r->next) {
        if (strcmp(r->id, id) == 0 && r->state == CONTAINER_RUNNING) {
            kill(r->host_pid, SIGTERM);
            pthread_mutex_unlock(&ctx->metadata_lock);
            usleep(500000);
            kill(r->host_pid, SIGKILL);
            if (ctx->monitor_fd >= 0)
                unregister_from_monitor(ctx->monitor_fd, r->id, r->host_pid);
            resp.status = 0;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "Stopped container '%s'", id);
            write(cfd, &resp, sizeof(resp));
            return;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
    resp.status = -1;
    snprintf(resp.message, CONTROL_MESSAGE_LEN,
             "Container '%s' not found or not running", id);
    write(cfd, &resp, sizeof(resp));
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    struct sigaction sa;
    int rc;

    (void)rootfs;
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    if ((rc = pthread_mutex_init(&ctx.metadata_lock, NULL))) {
        errno = rc; perror("mutex_init"); return 1; }
    if ((rc = bounded_buffer_init(&ctx.log_buffer))) {
        errno = rc; perror("buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock); return 1; }

    /* 1. Open kernel monitor */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] Warning: /dev/container_monitor: %s\n",
                strerror(errno));
    else
        fprintf(stderr, "[supervisor] Kernel monitor connected.\n");

    /* 2. Create Unix domain socket */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); goto fail; }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); goto fail; }
    if (listen(ctx.server_fd, 5) < 0) { perror("listen"); goto fail; }
    fcntl(ctx.server_fd, F_SETFL, O_NONBLOCK);

    /* 3. Signal handlers */
    global_ctx = &ctx;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
    sa.sa_handler = sigterm_handler;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* 4. Logs directory */
    mkdir(LOG_DIR, 0755);

    /* 5. Logger thread */
    if ((rc = pthread_create(&ctx.logger_thread, NULL,
                             logging_thread, &ctx.log_buffer))) {
        errno = rc; perror("pthread_create"); goto fail; }

    fprintf(stderr, "[supervisor] Ready on %s\n", CONTROL_PATH);

    /* 6. Event loop */
    while (!supervisor_should_stop) {
        fd_set fds;
        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
        int cfd;
        control_request_t  req;
        control_response_t resp;

        reap_children(&ctx);

        FD_ZERO(&fds);
        FD_SET(ctx.server_fd, &fds);
        rc = select(ctx.server_fd + 1, &fds, NULL, NULL, &tv);
        if (rc < 0)  { if (errno == EINTR) continue; perror("select"); break; }
        if (rc == 0) continue;

        cfd = accept(ctx.server_fd, NULL, NULL);
        if (cfd < 0) { if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                        perror("accept"); continue; }

        if (read(cfd, &req, sizeof(req)) != (ssize_t)sizeof(req)) {
            close(cfd); continue; }

        memset(&resp, 0, sizeof(resp));

        switch (req.kind) {
        case CMD_START:
            resp.status = spawn_container(&ctx, &req) == 0 ? 0 : -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     resp.status == 0 ? "Container '%s' started"
                                      : "Failed to start '%s'",
                     req.container_id);
            write(cfd, &resp, sizeof(resp));
            break;

        case CMD_RUN:
            if (spawn_container(&ctx, &req) == 0) {
                resp.status = 0;
                snprintf(resp.message, CONTROL_MESSAGE_LEN,
                         "Container '%s' started (foreground)", req.container_id);
                write(cfd, &resp, sizeof(resp));
                { int done = 0; container_record_t *r;
                  while (!done && !supervisor_should_stop) {
                    reap_children(&ctx);
                    pthread_mutex_lock(&ctx.metadata_lock);
                    for (r = ctx.containers; r; r = r->next)
                        if (strcmp(r->id, req.container_id) == 0 &&
                            r->state != CONTAINER_RUNNING &&
                            r->state != CONTAINER_STARTING) { done = 1; break; }
                    pthread_mutex_unlock(&ctx.metadata_lock);
                    if (!done) usleep(100000);
                  }
                }
            } else {
                resp.status = -1;
                snprintf(resp.message, CONTROL_MESSAGE_LEN,
                         "Failed to start '%s'", req.container_id);
                write(cfd, &resp, sizeof(resp));
            }
            break;

        case CMD_PS:   handle_ps(&ctx, cfd);                    break;
        case CMD_LOGS: handle_logs(&ctx, cfd, req.container_id); break;
        case CMD_STOP: handle_stop(&ctx, cfd, req.container_id); break;
        default:
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN, "Unknown command");
            write(cfd, &resp, sizeof(resp));
        }
        close(cfd);
    }

    /* === CLEANUP === */
    fprintf(stderr, "[supervisor] Shutting down...\n");
    { container_record_t *r;
      pthread_mutex_lock(&ctx.metadata_lock);
      for (r = ctx.containers; r; r = r->next)
          if (r->state == CONTAINER_RUNNING) {
              kill(r->host_pid, SIGKILL);
              if (ctx.monitor_fd >= 0)
                  unregister_from_monitor(ctx.monitor_fd, r->id, r->host_pid);
          }
      pthread_mutex_unlock(&ctx.metadata_lock);
    }
    usleep(200000);
    reap_children(&ctx);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);
    { container_record_t *r = ctx.containers;
      while (r) { container_record_t *n = r->next; free(r); r = n; } }
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    if (ctx.server_fd >= 0)  close(ctx.server_fd);
    unlink(CONTROL_PATH);
    pthread_mutex_destroy(&ctx.metadata_lock);
    fprintf(stderr, "[supervisor] Clean shutdown.\n");
    return 0;

fail:
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    if (ctx.server_fd >= 0)  close(ctx.server_fd);
    return 1;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */

static int send_control_request(const control_request_t *req)
{
    int sock;
    struct sockaddr_un addr;
    control_response_t resp;
    ssize_t n;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect (is the supervisor running?)");
        close(sock); return 1;
    }
    if (write(sock, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write"); close(sock); return 1;
    }

    memset(&resp, 0, sizeof(resp));
    n = read(sock, &resp, sizeof(resp));
    if (n == (ssize_t)sizeof(resp))
        printf("%s\n", resp.message);
    else if (n > 0)
        printf("Partial response.\n");
    else
        printf("No response from supervisor.\n");

    close(sock);
    return resp.status == 0 ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}

