#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>
#include <string.h>
#include <unistd.h>

#include <glib.h>
#include <wordexp.h>

#include <sys/time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

/* Constants... */
#define DATAMAX 64000

/* Global vars... */
FILE *fp = NULL;
char *ip = NULL;
int debug = 0;
int loop = 0;
int line = 0;
int echo = 0;
int port = -1;
int max_messages = 0;
struct timespec time_spec;
int max_length = DATAMAX;
char *file = 0;
int no_eps = 0;

/* Enums and types... */
enum {
    LINES = 1, JSON
} format = LINES;

typedef enum {
    START = 0,
    STEP,
    MAX,
    INTERVAL
} rate_t;

int rate;
int period_secs;
int rate_control[4];

/* Flags... */
bool is_tcp = 0;
bool socket_bind = 0;

int  hwm       = 10000;

char sendline[DATAMAX];
int strict = 0;
int binary = 0;

/* [ TCP/UDP functions */
/**
 * Check for sysctl performance tunable settings.
 */
void 
check_socket_size(int socketfd)
{
    socklen_t nlen;
    int n;

    nlen = sizeof(n);
    getsockopt(socketfd, SOL_SOCKET, SO_SNDBUF, &n, &nlen) ;
    if (n < 2000000) {
        printf("[Warning]: Consider increasing max UDP/TCP write "
               "buffers using sysctl for better performance.\n");
        printf("Run the following command:\n'\tsudo sysctl -w net.core.wmem_max=2000000'\n");
    }
    printf("[INFO]: Using socket send buffer size: %d\n", n);
}

/**
 * Setup socket size options.
 */
void 
set_socket_size(int socketfd)
{
    int n = 8 * 1024 * 1024;
    setsockopt(socketfd, SOL_SOCKET, SO_SNDBUF, &n, sizeof(n));
}

/**
 * Setup for UDP sockets.
 * \return socket file descriptor
 */
int
use_udp_socket(char *ip, int port,  struct sockaddr_in *server_address)
{
    int socketfd;

    socketfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (!socketfd) {
        printf("[ERROR]: UDP socket creation failed.\n");
        exit(1);
    }
    printf("[INFO]: UDP send socket on %s:%i\n", ip, port);

    bzero(server_address, sizeof(struct sockaddr_in));

    server_address->sin_family = AF_INET;
    server_address->sin_addr.s_addr = inet_addr(ip);
    server_address->sin_port = htons(port);
    return socketfd;
}

/**
 * Setup for TCP sockets.
 * \return socket file descriptor
 */
int
use_tcp_socket(char *ip, int port,  struct sockaddr_in *server_address)
{
    int socketfd;
    socklen_t socketlen;
    struct linger l;

    socketfd = socket(AF_INET, SOCK_STREAM, 0);
    if (!socketfd) {
        printf("[ERROR]: TCP socket creation failed.\n");
        exit(1);
    }
    printf("[INFO]: TCP send socket on %s:%i\n", ip, port);

    /* Here we force a close to allow final processing. */
    l.l_onoff  = 1;
    l.l_linger = 30;
    socketlen  = sizeof(l);
    setsockopt(socketfd, SOL_SOCKET, SO_LINGER, &l, socketlen);

    bzero(server_address, sizeof(struct sockaddr_in));
    server_address->sin_family = AF_INET;
    server_address->sin_addr.s_addr = inet_addr(ip);
    server_address->sin_port = htons(port);

   if( connect(socketfd, (struct sockaddr *)server_address, sizeof(struct sockaddr_in)) < 0) { 
     printf("[ERROR]: TCP connection failed on %s:%i\n", ip, port);
     exit(1);
   }
    return socketfd;
}

/* ]
[ Utility functions... */
/**
 * Control the event rate (EPS).
 */
void 
control_rate(const char *option_name,
        const char *rate_str, gpointer *data, GError **error)
{
    const char *p = rate_str;
    int state = 0;
    rate_control[INTERVAL] = 5;
    while (*p != '\0') {
        if (('0' <= *p) && (*p <= '9')) {
            p ++;
            continue;
        }
        rate_control[state] = atoi(rate_str);
        printf("%d %d\n", state, rate_control[state]);
        state ++;
        p++;
        rate_str = p;
    }
    rate_control[state] = atoi(rate_str);
    rate = rate_control[START];
}

/**
 * Update the event rate (EPS).
 */
void 
update_rate()
{
    if (rate_control[STEP] == 0) {
        return;
    }
    period_secs ++;
    if (period_secs < rate_control[INTERVAL]) {
        return;
    }
    period_secs = 0;
    rate += rate_control[STEP];
    if (rate > rate_control[MAX]) {
        rate = rate_control[MAX];
    }
}

/**
 * Process any binary data to send.
 */
void *
process_binary(char *b, int maxblen, FILE *fp)
{
    int c;
    int n = 0;

    while (((c = fgetc(fp)) != EOF) && (n < maxblen)) {
        if ((c >= ' ' && c <= 127) || c == '\t') {
            b[n++] = c;
            b[n]   = 0;
        } else if (c == '\n') {
            b[n++] = c;
            b[n]   = 0;
            return (void *)b;
        } else {
            printf("[WARNING]: line=%u: special character '%d' at location=%d\n", line, c, n);
            printf("-->%.*s%c\n", n, b, c);
            if (strict) {
                exit(1);
            }
        }
    }
    return NULL;
}

/**
 * Process events data (lines, JSON, etc.)
 */
void *
process_event(char  *buf, int maximum, int format)
{
    if (debug) {
        printf("[DEBUG]: format=%d, maximum=%d\n", format, maximum);
    }

    if (format == LINES) {
        if (binary) {
            return process_binary(buf, maximum, fp);
        } else {
            return fgets(buf, maximum, fp);
        }
    } else if (format == JSON) {
        int n = 0;
        int count = 0;
        int c;

        while ((c = fgetc(fp)) != EOF) {
            if (c == '{') {
                count++;
                buf[n++] = c;
                buf[n] = 0;
                break;
            }
        }

        while ((c = fgetc(fp)) != EOF) {
            if (c == '{') {
                count++;
            }
            if (c == '}') {
                count--;
            }
            buf[n++] = c;
            buf[n]   = 0;
            if (n > maximum - 1) {
                return 0;
            }
            if (count == 0) {
                return buf;
            }
        }
        return 0;
    }
    return 0;
}

/**
 * Return the differnce between 2 time arguments.
 */
double 
t_delta(struct timeval *time_val, struct timeval *time_elapse)
{
    double x_delta, tve, tvs;
    tvs = (double) time_val->tv_sec    + 1.e-6 * time_val->tv_usec;
    tve = (double) time_elapse->tv_sec + 1.e-6 * time_elapse->tv_usec;
    x_delta = (tve - tvs);
    return x_delta;
}


/**
 * Set various format options.
 */
gboolean 
set_format_option(const char *option_name, const char *value,
                           gpointer data, GError **error)
{
    if (0 == strcmp(option_name, "-j") || 0 == strcmp(option_name, "--json")) {
        format = JSON;
    }
    return true;
}

const char *argp_program_version = "netsender 5.0";

static const char doc[] =
    "Program consumes STDIN or file data and outputs to UDP/TCP sockets."
    "Supported record definitions/delimiters include:\n"
    "\t- Linefeeds\n"
    "\t- JSON dictionaries\n"
    "Input should be normal ASCII, tabs, newlines, etc. and will be read line-by-line.\n";

static const GOptionEntry entries[] = {
    { "tcp", 0, 0, G_OPTION_ARG_NONE, &is_tcp,
        "Use TCP", NULL },
    { "echo", 'e', 0, G_OPTION_ARG_NONE, &echo,
        "echo", NULL },
    { "debug", 'd', 0, G_OPTION_ARG_NONE, &debug,
        "Enable debug output", NULL },
    { "max-messages", 'n', 0, G_OPTION_ARG_INT, &max_messages,
        "Maximum number of events to send", "NUM"},
    { "delay", 's', 0, G_OPTION_ARG_INT, &(time_spec.tv_nsec),
        "Sleep delay (nanoseconds)", "nanoseconds" },
    { "rate", 'r', 0, G_OPTION_ARG_CALLBACK, control_rate,
        "Parse rate", "start:step:max:interval" },
    { "max-length", 'm', 0, G_OPTION_ARG_INT, &max_length,
        "Maximum message length", "bytes" },
    { "loop", 'l', 0, G_OPTION_ARG_NONE, &loop,
        "Loop forever", NULL },
    { "no-eps", 'q', 0, G_OPTION_ARG_NONE, &no_eps,
        "No EPS status", NULL },
    { "file", 'f', 0, G_OPTION_ARG_FILENAME, &file, "data file", "FILE" },
    { "json", 'j', G_OPTION_FLAG_NO_ARG, G_OPTION_ARG_CALLBACK, set_format_option,
        "JSON data", NULL },
    { "binary", 0, 0, G_OPTION_ARG_NONE, &binary,
        "Loads a char at a time, checks for binary chars", NULL },
    { "strict", 0, 0, G_OPTION_ARG_NONE, &strict,
        "Aborts on binary characters, requires --binary", NULL },
    { "hwm", 'i', 0, G_OPTION_ARG_INT, &hwm,
        "High water mark", "NUM" },
    { "bind", 'b', 0, G_OPTION_ARG_NONE, &socket_bind,
        "Bind socket"},
    { NULL }
};

/* ] */
int main(int argc, char **argv)
{
    struct timespec trem;
    struct timeval tv_last;
    struct timeval tv;
    struct timeval tvs;
    struct timeval tv_start;
    long long count   = 0;
    unsigned  r_count = 0;
    unsigned  s_count = 0;
    int socketfd = 0;
    struct sockaddr_in server_address;

    double x_delta;
    double x_rate;
    long long t_bytes = 0;
    int len;

    time_spec.tv_sec = 0;
    time_spec.tv_nsec = 0;

    GError *error = NULL;
    GOptionContext *context = g_option_context_new("IP port");
    g_option_context_set_summary(context, doc);
    g_option_context_add_main_entries(context, entries, NULL);
    if (!g_option_context_parse(context, &argc, &argv, &error)) {
        fprintf(stderr, "[ERROR]: Option parsing failed: %s\n", error->message);
        exit(1);
    }

    /* GOption leaves the rest as positional args to be dealt with. */
    if (3 != argc) {
        fprintf(stderr, "[ERROR]: Need to specify IP and port (argc=%i)\n", argc);
        char *help = g_option_context_get_help(context, false, NULL);
        fprintf(stderr, "%s", help);
        free(help);
        exit(1);
    }
    ip = argv[1];
    port = atoi(argv[2]);

    g_option_context_free(context);

    wordexp_t files;

    if (file) {
        if (wordexp(file, &files, 0)) {
            perror("[ERROR]: wordexp() failed.");
            exit(1);
        }
    } else {
        fp = stdin;
    }

    if (max_messages) {
        printf("[INFO]: max-messages: %d\n", max_messages);
    }
    if (rate) {
        printf("[INFO]: Rate: %d EPS\n", rate);
    }

    if (is_tcp) {
        printf("[INFO] Setup for TCP.\n");
        socketfd = use_tcp_socket(ip, port, &server_address);
        set_socket_size(socketfd);
        check_socket_size(socketfd);
    } else {
        printf("[INFO]: Setup for UDP.\n");
        socketfd = use_udp_socket(ip, port, &server_address);
        set_socket_size(socketfd);
        check_socket_size(socketfd);
    }

    gettimeofday(&tv_last, 0);
    gettimeofday(&tv_start, 0);
    gettimeofday(&tvs, 0);

  do {
    for (size_t i = 0; i < files.we_wordc || fp; ++i) {
        line = 0;

        if (stdin != fp) {
            fp = fopen(files.we_wordv[i], "r");
        }

        while (process_event(sendline, sizeof(sendline), format) != NULL) {
            line++;
            len = strlen(sendline);
            if (len > max_length) {
                len = max_length;
            }
            if (is_tcp) {
                send(socketfd, sendline, len, 0);
            } else {
                sendto(socketfd, sendline, len, 0, (struct sockaddr *)&server_address,
                    sizeof(server_address));
            }
            t_bytes += len;
            count++;
            r_count++;
            s_count++;
            if (echo) {
                fprintf(stderr, "%.*s", len, sendline);
            }
            if (time_spec.tv_nsec) {
                nanosleep(&time_spec, &trem);
            }
            if ((max_messages > 0) && (count >= max_messages)) {
                goto finish;
            }

            /* Now, rate limit and compare every millisecond. */
            gettimeofday(&tv, 0);
            x_delta = t_delta(&tv_last, &tv);
            if (rate > 0 && x_delta >= .001) {
                x_rate = (double)r_count / x_delta;
                while (x_rate > (double)rate) {
                    gettimeofday(&tv, 0);
                    x_delta = t_delta(&tv_last, &tv);
                    x_rate = (double)r_count / x_delta;
                }
                tv_last = tv;
                r_count = 0;
            }
            /* Dump stats every second... */
            if (tv.tv_sec > tvs.tv_sec) {
                x_delta = t_delta(&tvs, &tv);
                x_rate  = (double) s_count / x_delta;
                if (!no_eps) {
                    printf("[INFO]: EPS=%g       \r", x_rate);
                }
                fflush(stdout);
                tvs = tv;
                s_count = 0;
                update_rate();
            }
        }
        if (stdin != fp) {
            fclose(fp);
            fp = NULL;
        }
    }
  }
  while (loop);

finish:
    usleep(10);
    gettimeofday(&tv, 0);
    x_delta = t_delta(&tv_start, &tv);
    /* Print statistics: */
    printf("\n[INFO]: %g EPS, %lld events, %g seconds, %g MBPS, %g MBytes, %g BPE\n",
           (double) count / x_delta,
           count,
           x_delta,
           1.e-6 * (double) t_bytes / x_delta,
           1.e-6 * t_bytes,
           (double) t_bytes / count);
    close(socketfd);
    return 0;
}
