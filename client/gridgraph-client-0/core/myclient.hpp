#ifndef RDMA_CLIENT_H
#define RDMA_CLIENT_H

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

enum mode {
  M_WRITE,
  M_READ
};

enum send_s{
    SS_INIT,
    SS_MR_SENT,
    SS_RDMA_SENT,
    SS_DONE_SENT
  } ;

enum recv_s{
    RS_INIT,
    RS_MR_RECV,
    //RS_MR_RECV_1,
    RS_DONE_RECV
  } ;

enum type {
    MSG_MR,
    MSG_DONE
  } ;
union data{
    struct ibv_mr mr;
  } ;


static const int RDMA_BUFFER_SIZE = 1024*1024*1024;
const char *DEFAULT_PORT = "12345";
struct message {
  enum type type;
  union data data;
};

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct connection {
  struct rdma_cm_id *id;
  struct ibv_qp *qp;

  int connected;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;
  struct ibv_mr *rdma_local_mr;
  struct ibv_mr *rdma_remote_mr;

  struct ibv_mr peer_mr;

  struct message *recv_msg;
  struct message *send_msg;

  char *rdma_local_region;
  char *rdma_remote_region;

  enum send_s send_state; 
  enum recv_s recv_state;

};
//from common.h
void die(const char *reason);

void build_connection(struct rdma_cm_id *id);
void build_params(struct rdma_conn_param *params);
void destroy_connection(void *context);
void * get_local_message_region(void *context);
void on_connect(void *context);
void send_mr(void *context);
void set_mode(enum mode m);
//from common.c
static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static char * get_peer_message_region(struct connection *conn);
static void on_completion(struct ibv_wc *);
static void * poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);
static void send_message(struct connection *conn);

static struct context *s_ctx = NULL;
static enum mode s_mode = M_WRITE;

//from client.c

const int TIMEOUT_IN_MS = 500; /* ms */

static int on_addr_resolved(struct rdma_cm_id *id, char * transfering);
static int on_connection(struct rdma_cm_id *id);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event, char * transfering);
static int on_route_resolved(struct rdma_cm_id *id);
static void usage(const char *argv0);
//from main
int start (char* mode, char* ip, char* port, char * transfering);
char * rdma_read_start (char* mode, char* ip, char* port, char* transfering);
char * on_event_read(struct rdma_cm_event * event,char * transfering);
//from common.h

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void build_connection(struct rdma_cm_id *id)
{
  struct connection *conn;
  struct ibv_qp_init_attr qp_attr;

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));
  
  conn->id = id;
  conn->qp = id->qp;

  conn->send_state = SS_INIT;
  conn->recv_state = RS_INIT;

  conn->connected = 0;

  register_memory(conn);
 

  post_receives(conn);
  printf("~~ post receive in build connection\n");
}

//build connection 1: verb(ibv_context) to context,allocate PD, generate CC, CQ, create pthread
void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

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


//build connection 2: set qp attributes, type RC, send/receive wr num 10, sge 1
void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
  
}

void destroy_connection(void *context)
{
  struct connection *conn = (struct connection *)context;

  rdma_destroy_qp(conn->id);

  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);
  ibv_dereg_mr(conn->rdma_local_mr);
  ibv_dereg_mr(conn->rdma_remote_mr);

  free(conn->send_msg);
  free(conn->recv_msg);
  free(conn->rdma_local_region);
  free(conn->rdma_remote_region);

  rdma_destroy_id(conn->id);

  free(conn);

  printf("destroy_connection in disconnection\n");
}

//
void * get_local_message_region(void *context)
{
  //printf("get_local_message_region\n");
  if (s_mode == M_WRITE)
    return ((struct connection *)context)->rdma_local_region;
  else
    return ((struct connection *)context)->rdma_remote_region;
}

char * get_peer_message_region(struct connection *conn)
{
  if (s_mode == M_WRITE)
    return conn->rdma_remote_region;
  else
    return conn->rdma_local_region;
}
//from poll_cq : 
//static char aaa[RDMA_BUFFER_SIZE];
//static void *aaa = malloc(RDMA_BUFFER_SIZE);


void on_completion(struct ibv_wc *wc)
{
  printf("on_completion start : in poll_cq in build_context \n");
  struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;
  
  if (wc->status != IBV_WC_SUCCESS)
    die("on_completion: status is not IBV_WC_SUCCESS.");

  if (wc->opcode & IBV_WC_RECV) {
    conn->recv_state = recv_s(conn->recv_state+1);

    if (conn->recv_msg->type == MSG_MR) {
      memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));
      post_receives(conn); /* only rearm for MSG_MR */
      printf("post receives in if 1\n");
      if (conn->send_state == SS_INIT) /* received peer's MR before sending ours, so send ours back */
        send_mr(conn);
      //printf("send state is %d.\n",conn->send_state == SS_INIT);
    }
     printf("remote buffer if1: %s\n", get_peer_message_region(conn));
     printf("local buffer if1: %s\n", (char *)get_local_message_region(conn));

  } else {//received 
    
    conn->send_state = send_s(conn->send_state+1);
    
    printf("remote buffer on 1else: %s\n", get_peer_message_region(conn));
    printf("local buffer on 1else: %s\n", (char *)get_local_message_region(conn));
  }
  //printf("send state is %d.\n",conn->send_state == SS_INIT);
  //printf("remote buffer on completion: %s\n", get_peer_message_region(conn));
  printf("conn->recv_state is %d\n", conn->recv_state);
  printf("conn->send_state is %d\n", conn->send_state);

  if (conn->send_state == SS_MR_SENT && (conn->recv_state == RS_MR_RECV )) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    if (s_mode == M_WRITE)
      printf("received MSG_MR. writing message to remote memory...\n");
    else
      printf("received MSG_MR. reading message from remote memory...\n");

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)conn;
    wr.opcode = (s_mode == M_WRITE) ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uintptr_t)conn->peer_mr.addr;
    wr.wr.rdma.rkey = conn->peer_mr.rkey;

    sge.addr = (uintptr_t)conn->rdma_local_region;
    sge.length = RDMA_BUFFER_SIZE;
    sge.lkey = conn->rdma_local_mr->lkey;

     TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
     conn->send_msg->type = MSG_DONE;
     send_message(conn);
     printf("send message in if 2, the sge is : %s \n", sge.addr);
}
// else if(conn->send_state == SS_MR_SENT && conn->recv_state == RS_MR_RECV_1){
//       //send_message(conn);
//       conn->recv_state = recv_s(conn->recv_state+1);
//       printf("remote buffer RS_MR_RECV_1: %s\n", get_peer_message_region(conn));

// }
   else if (conn->send_state == SS_DONE_SENT && conn->recv_state == RS_DONE_RECV) {
    //post_receives(conn);
    printf("remote buffer in else 2: %s\n", get_peer_message_region(conn));
    //aaa = get_peer_message_region(conn);
    //memcpy(aaa, get_peer_message_region(conn), RDMA_BUFFER_SIZE);
    //printf("aaa after remote buffer: %s\n", aaa);
    rdma_disconnect(conn->id);
  }
}

//on connection 1
void on_connect(void *context)
{
  ((struct connection *)context)->connected = 1;
}

void * poll_cq(void *ctx)
{
  printf("build connection 1.1 : poll_cq in build_context \n");
  struct ibv_cq *cq;
  struct ibv_wc wc; 

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));
    //if (wc.status != IBV_WC_SUCCESS) printf("not IBV_WC_SUCCESS\n");
    //else printf(" IBV_WC_SUCCESS in poll_cq\n");
    // if (((struct connection *)(uintptr_t) wc.wr_id )->send_state == SS_MR_SENT) printf(" SS_MR_SENT in poll_cq\n");
    // else printf(" not SS_MR_SENT in poll_cq\n");
    // if (((struct connection *)(uintptr_t) wc.wr_id )->recv_state == RS_MR_RECV) printf(" RS_MR_RECV in poll_cq\n");
    // else printf(" not RS_MR_RECV in poll_cq\n"); 
    while (ibv_poll_cq(cq, 1, &wc))
    {
    // if (((struct connection *)(uintptr_t) wc.wr_id )->send_state == SS_MR_SENT) printf(" SS_MR_SENT in poll_cq\n");
    // else printf(" not SS_MR_SENT in poll_cq\n");
    // if (((struct connection *)(uintptr_t) wc.wr_id )->recv_state == RS_MR_RECV) printf(" RS_MR_RECV in poll_cq\n");
    // else printf(" not RS_MR_RECV in poll_cq\n");
    
      on_completion(&wc);
    }
      
  }

  return NULL;
}
//build connection 4: conn(connection) to wr(ibv_recv_wr), recv->msg to sge.addr, recv_mr->lkey to sge.lkey
//on_completion:
void post_receives(struct connection *conn)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->recv_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->recv_mr->lkey;

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
  printf("build connection 4 : ibv_post_recv in post_receives\n");
}
//build connection 3: malloc memory, register MR, send/receive MR size is size of message, local/remote region is RDMA_BUFFER_SIZE
void register_memory(struct connection *conn)
{
  conn->send_msg = (message *) malloc(sizeof(struct message));
  conn->recv_msg = (message *)malloc(sizeof(struct message));

  conn->rdma_local_region = (char *) malloc(RDMA_BUFFER_SIZE);
  conn->rdma_remote_region = (char *) malloc(RDMA_BUFFER_SIZE);

  TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->send_msg, 
    sizeof(struct message), 
    0));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->recv_msg, 
    sizeof(struct message), 
    IBV_ACCESS_LOCAL_WRITE));

  TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_local_region, 
    RDMA_BUFFER_SIZE, 
    ((s_mode == M_WRITE) ? 0 : IBV_ACCESS_LOCAL_WRITE)));

  TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_remote_region, 
    RDMA_BUFFER_SIZE, 
    ((s_mode == M_WRITE) ? (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE) : IBV_ACCESS_REMOTE_READ)));
  printf("build connection 3 : register_memory \n");
}
//on connection->send_mr :  message is transfered for exchange memory region keys between peers.
void send_message(struct connection *conn)
{
  
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_msg;
  sge.length = sizeof(struct message);
  //printf((char *)sge.length);
  sge.lkey = conn->send_mr->lkey;

  while (!conn->connected);

  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
  printf("send message : ibv_post_send \n");
}

//on connection 2: mesage type is  MSG_MR, copy send msg data mr to remote mr.
void send_mr(void *context)
{
  struct connection *conn = (struct connection *)context;

  conn->send_msg->type = MSG_MR;
  memcpy(&conn->send_msg->data.mr, conn->rdma_remote_mr, sizeof(struct ibv_mr));
  send_message(conn);
  printf("send mr\n");
}

void set_mode(enum mode m)
{
  s_mode = m;
}


//from client.c
int on_addr_resolved(struct rdma_cm_id *id, char *transfering)
{
  build_connection(id);
  //sprintf((char *)get_local_message_region(id->context), "client message from active/client side with pid %d", getpid());
  printf("transfering : %s",transfering );
  //memcpy((char *) get_local_message_region(id->context), transfering, sizeof(RDMA_BUFFER_SIZE));
  sprintf((char *) get_local_message_region(id->context),transfering);
  printf("sprintf: %s.\n", (char *)get_local_message_region(id->context));
  TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));
  printf("1. address resolved.\n");
  return 0;
}


int on_connection(struct rdma_cm_id *id)
{
  on_connect(id->context);
  send_mr(id->context);
  printf("send mr in on_connection\n");
  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  destroy_connection(id->context);
  return 1; /* exit event loop */
}

int on_event(struct rdma_cm_event * event,char * transfering)
{
  int r = 0;
  //printf((char *) event->event);
  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
    r = on_addr_resolved(event->id, transfering);
  else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
    r = on_route_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else {
    fprintf(stderr, "on_event: %d\n", event->event);
    die("on_event: unknown event.");
  }

  return r;
}
//static char * result1;
 char * on_event_read(struct rdma_cm_event * event,char * transfering)
{
  int r = 0;
  char* result1;
  char* result =(char *)malloc(RDMA_BUFFER_SIZE);
  printf("event type is %d.\n",event->event);
  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
    {
      r = on_addr_resolved(event->id, transfering);
      //result1 =result;
    }
  else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
    r = on_route_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    { 
     result1 = get_peer_message_region((struct connection *)event->id->context);
     memcpy(result, result1, RDMA_BUFFER_SIZE);
     //result1 = result1;
      printf("result: ");
      printf(result);
      r = on_disconnect(event->id);
    }
  else {
    fprintf(stderr, "on_event: %d\n", event->event);
    die("on_event: unknown event.");
  }
  if(r==0) return "continue";
  else return result;
}

int on_route_resolved(struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;
  build_params(&cm_params);
  TEST_NZ(rdma_connect(id, &cm_params));
  printf("2. route resolved end.\n");
  return 0;
}

void usage(const char *argv0)
{
  fprintf(stderr, "usage: %s <mode> <server-address> <server-port>\n  mode = \"read\", \"write\"\n", argv0);
  exit(1);
}

//from main
int rdma_write_start (char* mode, char* ip, char* port, char* transfering){
  struct addrinfo *addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *conn= NULL;
  struct rdma_event_channel *ec = NULL;
  if (strcmp(mode, "write") == 0)
    set_mode(M_WRITE);
  else if (strcmp(mode, "read") == 0)
    set_mode(M_READ);
  else 
    usage("usage->"); 

  TEST_NZ(getaddrinfo(ip, port, NULL, &addr));
  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));

  freeaddrinfo(addr);

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);
    
    if (on_event(&event_copy,transfering))
      break;
  }

  rdma_destroy_event_channel(ec);
  //printf("aaa : %s\n" , aaa);
  return 0;

}

char * rdma_read_start (char* mode, char* ip, char* port, char* transfering){
  struct addrinfo *addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *conn= NULL;
  struct rdma_event_channel *ec = NULL;
  if (strcmp(mode, "write") == 0)
    set_mode(M_WRITE);
  else if (strcmp(mode, "read") == 0)
    set_mode(M_READ);
  else 
    usage("usage->"); 

  TEST_NZ(getaddrinfo(ip, port, NULL, &addr));
  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));

  freeaddrinfo(addr);
  char * remote_region ;
  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);
   // if (on_event(&event_copy,transfering))
    //  break;
    //printf("qqremote_region on_event_read: %s\n",remote_region);
    remote_region=on_event_read(&event_copy,transfering);
    //printf("--remote_region on_event_read: %s\n",remote_region);
     if (remote_region == "continue")  
      printf("ccremote_region on_event_read: %s\n",remote_region);
     else{
       printf("rrremote_region on_event_read: %s\n",remote_region);
       break;
     }
  }
  //printf("00remote_region on_event_read: %s\n",remote_region);

  rdma_destroy_event_channel(ec);
  //printf("aaa : %s\n" , aaa);

  return remote_region;

}


#endif

