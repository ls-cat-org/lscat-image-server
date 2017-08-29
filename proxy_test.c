#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zmq.h>

#define THAT_DEALER "tcp://10.1.253.10:60202"
#define THIS_ROUTER "ipc://@isv2proxy"

int main() {
  void *zctx;
  zmq_pollitem_t pis[2];
  void *frontend;
  void *backend;
  zmq_msg_t msg;
  int err;
  int more;
  int next_is_message;

  zctx = zmq_ctx_new();

  frontend = zmq_socket(zctx, ZMQ_ROUTER);
  backend  = zmq_socket(zctx, ZMQ_DEALER);

  errno = 0;
  err = zmq_bind(backend, THIS_ROUTER);
  if (err != 0) {
    fprintf(stderr, "Failed to bind address %s: %s\n", THIS_ROUTER, zmq_strerror(errno));
    exit(-1);
  }

  errno = 0;
  err = zmq_connect(frontend, THAT_DEALER);
  if (err != 0) {
    fprintf(stderr, "Failed to connect to address %s: %s\n", THAT_DEALER, zmq_strerror(errno));
    exit(-1);
  }

  pis[0].socket = frontend;
  pis[0].events = ZMQ_POLLIN;
  pis[1].socket = backend;
  pis[1].events = ZMQ_POLLIN;

  while (1) {
    zmq_poll(pis, 2, -1);
    if (pis[0].revents & ZMQ_POLLIN) {
      next_is_message = 0;
      do {
        zmq_msg_init(&msg);
        zmq_msg_recv(&msg, frontend, 0);
        more = zmq_msg_more(&msg);
        if (next_is_message) {
          fprintf(stdout, "eavesdropping: %.*s\n", zmq_msg_size(&msg), zmq_msg_data(&msg));
        }
        if (zmq_msg_size(&msg) == 0) {
          next_is_message = 1;
        }
        zmq_msg_send(&msg, backend, more ? ZMQ_SNDMORE : 0);
        zmq_msg_close(&msg);
      } while (more);
    }

    if (pis[1].revents & ZMQ_POLLIN) {
      next_is_message = 0;
      do {
        zmq_msg_init(&msg);
        zmq_msg_recv(&msg, backend, 0);
        more = zmq_msg_more(&msg);
        if (next_is_message) {
          fprintf(stdout, "eavesdropping: %.*s\n", zmq_msg_size(&msg), zmq_msg_data(&msg));
        }
        if (zmq_msg_size(&msg) == 0) {
          next_is_message = 1;
        }
        zmq_msg_send(&msg, frontend, more ? ZMQ_SNDMORE : 0);
        zmq_msg_close(&msg);
      } while (more);
    }

  }


  err = zmq_close(frontend);
  if (err != 0) {
    fprintf(stderr, "Failed to close frontend: %s\n", zmq_strerror(errno));
  }

  err = zmq_close(backend);
  if (err != 0) {
    fprintf(stderr, "Failed to close backend: %s\n", zmq_strerror(errno));
  }

  do {
    errno = 0;
    err   = zmq_ctx_term(zctx);
  } while (errno == EINTR);

  if (err != 0) {
    fprintf(stderr, "zmq_ctx_term failed: %s\n", zmq_strerror(errno));
  }
}
