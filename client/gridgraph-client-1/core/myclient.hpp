#ifndef RDMA_CLIENT_H
#define RDMA_CLIENT_H

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <fcntl.h>
#include <libgen.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
static const int RDMA_BUFFER_SIZE = 1024*1024*1024;
const char *DEFAULT_PORT = "12345";
const int TIMEOUT_IN_MS = 500;

enum message_id
{
  MSG_INVALID = 0,
  MSG_MR,
  MSG_READY,
  MSG_DONE
};

struct message
{
  int id;

  union
  {
    struct
    {
      uint64_t addr;
      uint32_t rkey;
    } mr;
  } data;
};

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct client_context
{
  char *buffer;
  struct ibv_mr *buffer_mr;

  struct message *msg;
  struct ibv_mr *msg_mr;

  uint64_t peer_addr;
  uint32_t peer_rkey;

  int fd;
  const char *file_name;
};


typedef void (*pre_conn_cb_fn)(struct rdma_cm_id *id);
typedef void (*connect_cb_fn)(struct rdma_cm_id *id);
typedef void (*completion_cb_fn)(struct ibv_wc *wc);
typedef void (*disconnect_cb_fn)(struct rdma_cm_id *id);

void rc_init(pre_conn_cb_fn, connect_cb_fn, completion_cb_fn, disconnect_cb_fn);
void rc_client_loop(const char *host, const char *port, void *context);
void rc_disconnect(struct rdma_cm_id *id);
void rc_die(const char *message);
struct ibv_pd * rc_get_pd();
void rc_server_loop(const char *port);

static struct context *s_ctx = NULL;
static pre_conn_cb_fn s_on_pre_conn_cb = NULL;
static connect_cb_fn s_on_connect_cb = NULL;
static completion_cb_fn s_on_completion_cb = NULL;
static disconnect_cb_fn s_on_disconnect_cb = NULL;

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void event_loop(struct rdma_event_channel *ec, int exit_on_disconnect);
static void * poll_cq(void *);
static int rdma_writefile_start(char* mode, char* ip, char* index);

void die(const char *reason);

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void build_connection(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));
}

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      rc_die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_params(struct rdma_conn_param *params)
{
  memset(params, 0, sizeof(*params));

  params->initiator_depth = params->responder_resources = 1;
  params->rnr_retry_count = 7; /* infinite retry */
}
//sge
void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 1000;
  qp_attr->cap.max_recv_wr = 1000;
  qp_attr->cap.max_send_sge = 4;
  qp_attr->cap.max_recv_sge = 4;
}

void event_loop(struct rdma_event_channel *ec, int exit_on_disconnect)
{
  struct rdma_cm_event *event = NULL;
  struct rdma_conn_param cm_params;

  build_params(&cm_params);

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (event_copy.event == RDMA_CM_EVENT_ADDR_RESOLVED) {
      build_connection(event_copy.id);

      if (s_on_pre_conn_cb)
        s_on_pre_conn_cb(event_copy.id);

      TEST_NZ(rdma_resolve_route(event_copy.id, TIMEOUT_IN_MS));

    } else if (event_copy.event == RDMA_CM_EVENT_ROUTE_RESOLVED) {
      TEST_NZ(rdma_connect(event_copy.id, &cm_params));

    } else if (event_copy.event == RDMA_CM_EVENT_CONNECT_REQUEST) {
      build_connection(event_copy.id);

      if (s_on_pre_conn_cb)
        s_on_pre_conn_cb(event_copy.id);

      TEST_NZ(rdma_accept(event_copy.id, &cm_params));

    } else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED) {
      if (s_on_connect_cb)
        s_on_connect_cb(event_copy.id);

    } else if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED) {
      rdma_destroy_qp(event_copy.id);

      if (s_on_disconnect_cb)
        s_on_disconnect_cb(event_copy.id);

      rdma_destroy_id(event_copy.id);

      if (exit_on_disconnect)
        break;

    } else {
      rc_die("unknown event\n");
    }
  }
}

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc)) {
      if (wc.status == IBV_WC_SUCCESS)
        s_on_completion_cb(&wc);
      else
        rc_die("poll_cq: status is not IBV_WC_SUCCESS");
    }
  }

  return NULL;
}

void rc_init(pre_conn_cb_fn pc, connect_cb_fn conn, completion_cb_fn comp, disconnect_cb_fn disc)
{
  s_on_pre_conn_cb = pc;
  s_on_connect_cb = conn;
  s_on_completion_cb = comp;
  s_on_disconnect_cb = disc;
}

void rc_client_loop(const char *host, const char *port, void *context)
{
  struct addrinfo *addr;
  struct rdma_cm_id *conn = NULL;
  struct rdma_event_channel *ec = NULL;
  struct rdma_conn_param cm_params;
  //拿到地址
  TEST_NZ(getaddrinfo(host, port, NULL, &addr));

  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));//UDP可以尝试一下？
  
  TEST_NZ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));

  freeaddrinfo(addr);

  conn->context = context;

  build_params(&cm_params);

  event_loop(ec, 1); // exit on disconnect

  rdma_destroy_event_channel(ec);
}

void rc_server_loop(const char *port)
{
  struct sockaddr_in6 addr;
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;

  memset(&addr, 0, sizeof(addr));
  addr.sin6_family = AF_INET6;
  addr.sin6_port = htons(atoi(port));

  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

  event_loop(ec, 0); // don't exit on disconnect

  rdma_destroy_id(listener);
  rdma_destroy_event_channel(ec);
}

void rc_disconnect(struct rdma_cm_id *id)
{
  rdma_disconnect(id);
}

void rc_die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

struct ibv_pd * rc_get_pd()
{
  return s_ctx->pd;
}


//from main

static void send_message(struct rdma_cm_id *id)
{
  struct client_context *ctx = (struct client_context *)id->context;

  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)id;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)ctx->msg;
  sge.length = sizeof(*ctx->msg);
  sge.lkey = ctx->msg_mr->lkey;

  TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
  printf("send_message: ibv_post_send\n");
}

static void write_remote(struct rdma_cm_id *id, uint32_t len)
{
  struct client_context *ctx = (struct client_context *)id->context;

//生成send wr
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  

  
  memset(&wr, 0, sizeof(wr));
  
  wr.wr_id = (uintptr_t)id;
  wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;//IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.imm_data = htonl(len);
  wr.wr.rdma.remote_addr = ctx->peer_addr;
  wr.wr.rdma.rkey = ctx->peer_rkey;

  if (len) {
    wr.sg_list = &sge;
    wr.num_sge = 1;//多个sge合并可以做？

    sge.addr = (uintptr_t)ctx->buffer;
    sge.length = len;
    sge.lkey = ctx->buffer_mr->lkey;
    //printf("len is %d\n", len);
  }
  TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
  printf("write_remote: ctx->buffer is : %s \n ",  ctx->buffer);
}

static void post_receive_message(struct rdma_cm_id *id)
{
  struct client_context *ctx = (struct client_context *)id->context;

  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)id;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)ctx->msg;
  sge.length = sizeof(*ctx->msg);
  sge.lkey = ctx->msg_mr->lkey;

  TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
  printf("post_receive_msg: ctx->buffer is %s \n", ctx->buffer);
 //printf("wr lenth is %d\n", wr.sg_list->length);
}


static void post_receive_buffer(struct rdma_cm_id *id)
{
  struct client_context *ctx = (struct client_context *)id->context;
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)id;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)ctx->buffer;
  sge.length = RDMA_BUFFER_SIZE;
  sge.lkey = ctx->buffer_mr->lkey;
  TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
  printf("post_receive_buffer: sge addr ctx->buffer is %s\n", sge.addr);
  printf("post_receive_buffer: ctx->buffer is %s\n", ((struct client_context *)id->context)->buffer);
}

static void send_next_chunk(struct rdma_cm_id *id)
{
  struct client_context *ctx = (struct client_context *)id->context;

  ssize_t size = 0;
//ssize_t read()会把参数fd所指的文件传送BUFFER_SIZE 个字节到buffer 指针所指的内存中
  size = read(ctx->fd, ctx->buffer, RDMA_BUFFER_SIZE);
  //printf("%zu\n", size);
  if (size == -1)
    rc_die("read() failed\n");
    
  printf(" send_next_chunk: ctx->buffer is : %s \n", ctx->buffer);
  write_remote(id, size);
}

static void send_file_name(struct rdma_cm_id *id)
{
  struct client_context *ctx = (struct client_context *)id->context;
//buffer中是filename
  printf("send_file_name: ctx->file_name is : %s \n", ctx->file_name);
  strcpy(ctx->buffer, ctx->file_name);

  write_remote(id, strlen(ctx->file_name) + 1);
}

//posix_memalign返回BUFFER_SIZE字节的动态内存，并且这块内存的地址是_SC_PAGESIZE的倍数,内存块的地址放在了ctx->buffer里面
static void on_pre_conn(struct rdma_cm_id *id)
{
  struct client_context *ctx = (struct client_context *)id->context;

  posix_memalign((void **)&ctx->buffer, sysconf(_SC_PAGESIZE), RDMA_BUFFER_SIZE);
  //pd: protect domain , addr, length, access flags(本地写，远程写，远程读，等，0是默认本地读)
  TEST_Z(ctx->buffer_mr = ibv_reg_mr(rc_get_pd(), ctx->buffer, RDMA_BUFFER_SIZE,  IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE));
  //printf("%zu\n", BUFFER_SIZE);
  //printf("%zu\n",sysconf(_SC_PAGESIZE));
  
  //ctx->msg 本地写
  posix_memalign((void **)&ctx->msg, sysconf(_SC_PAGESIZE), sizeof(*ctx->msg));
  TEST_Z(ctx->msg_mr = ibv_reg_mr(rc_get_pd(), ctx->msg, sizeof(*ctx->msg), IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE));
  //printf("%zu\n",sizeof(*ctx->msg));
  
  post_receive_message(id);
}

static void on_completion(struct ibv_wc *wc)
{
  struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)(wc->wr_id);
  struct client_context *ctx = (struct client_context *)id->context;
  printf("on_completion~~~~~wc->opcode is %d \n",wc->opcode);
  // if(wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM){
  //   printf("IBV_WC_RECV_RDMA_WITH_IMM ctx->buffer is %s \n", ctx->buffer);
  // }
   if(wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM){
    //ctx->msg->id == MSG_READY
    send_message(id);
    printf("on completion post receive 22 ctx->buffer");
    post_receive_message(id);
    printf("on completion post receive 22 ctx->buffer is %s \n", ctx->buffer);
  }
  if (wc->opcode == IBV_WC_RECV) {
    if (ctx->msg->id == MSG_MR) {
      ctx->peer_addr = ctx->msg->data.mr.addr;
      ctx->peer_rkey = ctx->msg->data.mr.rkey;
      printf("received MR, sending file name\n");
     // printf("ctx->peer_rkey is %d\n", ctx->peer_rkey);
      //printf("ctx->peer_addr is %ld\n", ctx->peer_addr);
      send_file_name(id);
      post_receive_message(id);
      printf("buffer ctx->buffer is %s \n", ctx->buffer);

    } else if (ctx->msg->id == MSG_READY) {
      printf("received READY, sending chunk\n");
      //printf("ctx->peer_rkey is %d\n", ctx->peer_rkey);
      //printf("ctx->peer_addr is %ld\n", ctx->peer_addr);
      send_next_chunk(id);
    } else if (ctx->msg->id == MSG_DONE) {
      printf("received DONE, disconnecting\n");
      rc_disconnect(id);
      return;
    }

    //if()
    printf("on_completion~post receive ctx->buffer is %s \n", ctx->buffer);
    post_receive_buffer((struct rdma_cm_id *)(uintptr_t)(wc->wr_id));
    //post_receive_buffer(id);
    printf("on_completion~post receive ee ctx->buffer is %s \n", ctx->buffer);
  }

}

static int rdma_writefile_start (char* mode, char* ip, char* index){
   struct client_context ctx;
   ctx.file_name = index;
   ctx.fd = open(ctx.file_name, O_RDONLY);
   if (ctx.fd == -1) {
    fprintf(stderr, "unable to open input file \"%s\"\n", ctx.file_name);
    return 1;
  }
   rc_init(
    on_pre_conn,
    NULL, // on connect
    on_completion,
    NULL); // on disconnect

  rc_client_loop(ip, DEFAULT_PORT, &ctx);

  close(ctx.fd);
  //printf("aaa : %s\n" , aaa);
  return 0;

}

// void on_completion(struct ibv_wc *wc)
// {
//   printf("build connection 1.1.1 start : on_completion in poll_cq in build_context \n");
//   struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;
  
//   if (wc->status != IBV_WC_SUCCESS)
//     die("on_completion: status is not IBV_WC_SUCCESS.");

//   if (wc->opcode & IBV_WC_RECV) {
//     conn->recv_state = recv_s(conn->recv_state+1);
//     //if(conn->recv_state == 3){conn->recv_state = recv_s(conn->recv_state-1);}

//     if (conn->recv_msg->type == MSG_MR) {
//       memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));
//       post_receives(conn); /* only rearm for MSG_MR */
//       //printf("post receives \n");
//       if (conn->send_state == SS_INIT) /* received peer's MR before sending ours, so send ours back */
//         send_mr(conn);
//       //printf("send state is %d.\n",conn->send_state == SS_INIT);
//     }
//      printf("remote buffer if1: %s\n", get_peer_message_region(conn));
//      printf("local buffer if 1: %s\n", (char *)get_local_message_region(conn));

//   } else {
//     //printf("recv state is %d.\n",conn->recv_state == RS_MR_RECV);
//     conn->send_state = send_s(conn->send_state+1);
//     //printf("recv state is %d.\n",conn->recv_state == RS_MR_RECV);
//     printf("send completed successfully.\n");
//     //post_receives(conn); 
//     printf("remote buffer on 1else: %s\n", get_peer_message_region(conn));
//     printf("local buffer on 1else: %s\n", (char *)get_local_message_region(conn));
//   }
//   //printf("send state is %d.\n",conn->send_state == SS_INIT);
//   //printf("remote buffer on completion: %s\n", get_peer_message_region(conn));
//   printf("conn->recv_state is %d\n", conn->recv_state);
//   printf("conn->send_state is %d\n", conn->send_state);

//   if (conn->send_state == SS_MR_SENT && (conn->recv_state == RS_MR_RECV )) {
//     struct ibv_send_wr wr, *bad_wr = NULL;
//     struct ibv_sge sge;

//     if (s_mode == M_WRITE)
//       printf("received MSG_MR. writing message to remote memory...\n");
//     else
//       printf("received MSG_MR. reading message from remote memory...\n");

//     memset(&wr, 0, sizeof(wr));

//     wr.wr_id = (uintptr_t)conn;
//     wr.opcode = (s_mode == M_WRITE) ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;
//     wr.sg_list = &sge;
//     wr.num_sge = 1;
//     wr.send_flags = IBV_SEND_SIGNALED;
//     wr.wr.rdma.remote_addr = (uintptr_t)conn->peer_mr.addr;
//     wr.wr.rdma.rkey = conn->peer_mr.rkey;

//     sge.addr = (uintptr_t)conn->rdma_local_region;
//     sge.length = RDMA_BUFFER_SIZE;
//     sge.lkey = conn->rdma_local_mr->lkey;

//      TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
//      conn->send_msg->type = MSG_DONE;
//      send_message(conn);
//   } 
//   // else if(conn->send_state == SS_MR_SENT && (conn->recv_state == RS_MR_RECV_1)){
//   //   post_receives(conn); 
//   // }
//    else if (conn->send_state == SS_DONE_SENT && conn->recv_state == RS_DONE_RECV){
//      post_receives(conn);
//     printf("remote buffer 3: %s\n", get_peer_message_region(conn));
//     //aaa = get_peer_message_region(conn);
//     //memcpy(aaa, get_peer_message_region(conn), RDMA_BUFFER_SIZE);
//     //printf("aaa after remote buffer: %s\n", aaa);
    
//     rdma_disconnect(conn->id);
//   }
//   printf("build connection 1.1.1 end : on_completion in poll_cq in build_context \n");
// }


// int on_event(struct rdma_cm_event * event,char * transfering)
// {
//   int r = 0;
//   //printf((char *) event->event);
//   if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
//     r = on_addr_resolved(event->id, transfering);
//   else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
//     r = on_route_resolved(event->id);
//   else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
//     r = on_connection(event->id);
//   else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
//     r = on_disconnect(event->id);
//   else {
//     fprintf(stderr, "on_event: %d\n", event->event);
//     die("on_event: unknown event.");
//   }

//   return r;
// }


// void usage(const char *argv0)
// {
//   fprintf(stderr, "usage: %s <mode> <server-address> <server-port>\n  mode = \"read\", \"write\"\n", argv0);
//   exit(1);
// }

// //from main
// int rdma_write_start (char* mode, char* ip, char* port, char* transfering){
//   struct addrinfo *addr;
//   struct rdma_cm_event *event = NULL;
//   struct rdma_cm_id *conn= NULL;
//   struct rdma_event_channel *ec = NULL;
//   if (strcmp(mode, "write") == 0)
//     set_mode(M_WRITE);
//   else if (strcmp(mode, "read") == 0)
//     set_mode(M_READ);
//   else 
//     usage("usage->"); 

//   TEST_NZ(getaddrinfo(ip, port, NULL, &addr));
//   TEST_Z(ec = rdma_create_event_channel());
//   TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
//   TEST_NZ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));

//   freeaddrinfo(addr);
  
//   while (rdma_get_cm_event(ec, &event) == 0) {
//     struct rdma_cm_event event_copy;

//     memcpy(&event_copy, event, sizeof(*event));
//     rdma_ack_cm_event(event);
    
//     if (on_event(&event_copy,transfering))
//       break;
//   }

//   rdma_destroy_event_channel(ec);
//   //printf("aaa : %s\n" , aaa);
//   return 0;

// }

// char * rdma_read_start (char* mode, char* ip, char* port, char* transfering){
//   struct addrinfo *addr;
//   struct rdma_cm_event *event = NULL;
//   struct rdma_cm_id *conn= NULL;
//   struct rdma_event_channel *ec = NULL;
//   if (strcmp(mode, "write") == 0)
//     set_mode(M_WRITE);
//   else if (strcmp(mode, "read") == 0)
//     set_mode(M_READ);
//   else 
//     usage("usage->"); 

//   TEST_NZ(getaddrinfo(ip, port, NULL, &addr));
//   TEST_Z(ec = rdma_create_event_channel());
//   TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
//   TEST_NZ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));

//   freeaddrinfo(addr);
//   char * remote_region ;
//   //remote_region = event_loop_read(ec, 1, char* transfering);
//   while (rdma_get_cm_event(ec, &event) == 0) {
//     struct rdma_cm_event event_copy;

//     memcpy(&event_copy, event, sizeof(*event));
//     rdma_ack_cm_event(event);
//    // if (on_event(&event_copy,transfering))
//     //  break;
//     //printf("qqremote_region on_event_read: %s\n",remote_region);
//     remote_region=on_event_read(&event_copy,transfering);
//     //printf("--remote_region on_event_read: %s\n",remote_region);
//      if (remote_region == "continue")  
//       printf("ccremote_region on_event_read: %s\n",remote_region);
//      else{
//        printf("rrremote_region on_event_read: %s\n",remote_region);
//        break;
//      }
//   }
//   //printf("00remote_region on_event_read: %s\n",remote_region);

//   rdma_destroy_event_channel(ec);
//   //printf("aaa : %s\n" , aaa);

//   return remote_region;

// }

//  char * on_event_read(struct rdma_cm_event * event,char * transfering)
// {
//   int r = 0;
//   char* result1;
//   char* result =(char *)malloc(RDMA_BUFFER_SIZE);
//   printf("event type is %d.\n",event->event);
//   if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
//     {
//       r = on_addr_resolved(event->id, transfering);
//       //result1 =result;
//     }
//   else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
//     r = on_route_resolved(event->id);
//   else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
//     r = on_connection(event->id);
//   else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
//     { 
//      result1 = get_peer_message_region((struct connection *)event->id->context);
//      memcpy(result, result1, RDMA_BUFFER_SIZE);
//      //result1 = result1;
//       printf("result: ");
//       printf(result);
//       r = on_disconnect(event->id);
//     }
//   else {
//     fprintf(stderr, "on_event: %d\n", event->event);
//     die("on_event: unknown event.");
//   }
//   if(r==0) return "continue";
//   else return result;
// }




#endif

