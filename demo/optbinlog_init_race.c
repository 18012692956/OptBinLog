#include "optbinlog_shared.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static void usage(const char* prog) {
    fprintf(stderr,
        "Usage:\n"
        "  %s --eventlog-dir <dir> --shared <file> --procs N --trace <file> [--strict-perm] [--clean]\n",
        prog
    );
}

int main(int argc, char** argv) {
    const char* eventlog_dir = NULL;
    const char* shared_path = NULL;
    const char* trace_path = NULL;
    int procs = 10;
    int strict_perm = 0;
    int clean = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--eventlog-dir") == 0 && i + 1 < argc) {
            eventlog_dir = argv[++i];
        } else if (strcmp(argv[i], "--shared") == 0 && i + 1 < argc) {
            shared_path = argv[++i];
        } else if (strcmp(argv[i], "--trace") == 0 && i + 1 < argc) {
            trace_path = argv[++i];
        } else if (strcmp(argv[i], "--procs") == 0 && i + 1 < argc) {
            procs = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--strict-perm") == 0) {
            strict_perm = 1;
        } else if (strcmp(argv[i], "--clean") == 0) {
            clean = 1;
        }
    }

    if (!eventlog_dir || !shared_path || !trace_path || procs <= 0) {
        usage(argv[0]);
        return 1;
    }

    if (clean) {
        unlink(shared_path);
        unlink(trace_path);
    }

    setenv("OPTBINLOG_TRACE_PATH", trace_path, 1);

    for (int i = 0; i < procs; i++) {
        pid_t pid = fork();
        if (pid < 0) {
            fprintf(stderr, "fork failed: %s\n", strerror(errno));
            return 1;
        }
        if (pid == 0) {
            int rc = optbinlog_shared_init_from_dir(eventlog_dir, shared_path, strict_perm);
            _exit(rc == 0 ? 0 : 2);
        }
    }

    int status = 0;
    for (int i = 0; i < procs; i++) {
        int st = 0;
        wait(&st);
        if (st != 0) status = 1;
    }

    return status;
}
