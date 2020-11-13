#include <fcntl.h>
#include <sys/stat.h>

#include "rdma-common.h"


#define MAX_FILE_NAME 256
const char *DEFAULT_PORT = "1345";
static const long BUFFER_SIZE = 1024*1024*128;

// struct conn_context
// {
//   char *buffer;
//   struct ibv_mr *buffer_mr;

//   struct message *msg;
//   struct ibv_mr *msg_mr;

//   uint64_t peer_addr;
//   uint32_t peer_rkey;

//   int fd;
//   char file_name[MAX_FILE_NAME];
// };

// struct send_context
// {
//   char *buffer;
//   struct ibv_mr *buffer_mr;

//   struct message *msg;
//   struct ibv_mr *msg_mr;

//   uint64_t peer_addr;
//   uint32_t peer_rkey;

//   int fd;
//   const char *file_name;
// };

struct server_context
{
  struct message *recv_msg;
  struct message *send_msg;

  struct ibv_mr *recv_msg_mr;
  struct ibv_mr *send_msg_mr;

  char *send_buffer;
  char *recv_buffer;
  struct ibv_mr *recv_buffer_mr;
  struct ibv_mr *send_buffer_mr;
  // struct ibv_mr peer_mr;
  uint64_t peer_addr;
  uint64_t peer_rkey;
  int fd;
  char *block_name;
};

static void send_message(struct rdma_cm_id *id)
{
  struct server_context *ctx = (struct server_context *)id->context;

  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)id;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)ctx->send_msg;
  sge.length = sizeof(*ctx->send_msg);
  sge.lkey = ctx->send_msg_mr->lkey;

  TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
  printf("send message: ibv_post_send \n");
}

static void write_remote(struct rdma_cm_id *id, uint32_t len)
{
  struct server_context *ctx = (struct server_context *)id->context;

//生成send wr
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));
  
  wr.wr_id = (uintptr_t)id;
  wr.opcode = IBV_WR_SEND;//IBV_WR_RDMA_WRITE_WITH_IMM;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.imm_data = htonl(len);
  // wr.wr.rdma.remote_addr = ctx->peer_addr;  //ctx->buffer_mr->addr
  // wr.wr.rdma.rkey = ctx->peer_rkey;
  //printf("ctx->peer_rkey is %s\n", ctx->peer_rkey);
  //printf("ctx->peer_addr is %s\n", ctx->peer_addr);
  if (len) {
    
    wr.sg_list = &sge;
    wr.num_sge = 1;//多个sge合并可以做？

    sge.addr = (uintptr_t)ctx->send_buffer;
    sge.length = len;
    sge.lkey = ctx->send_buffer_mr->lkey;
    // sge.addr = (uintptr_t)ctx->msg;
    // sge.length = sizeof(*ctx->msg);
    // sge.lkey = ctx->msg_mr->lkey;
    //printf("len is %d\n", len);
    printf("write remote sge addr is : %s \n",  sge.addr);
    //printf("write remote sge lkey is : %s \n",  sge.lkey);
  }

  TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
  printf("write_remote : ibv_post_send, ctx->send_buffer is : %s, ctx->send_msg->id is : %d \n ",  ctx->send_buffer, ctx->send_msg->id);
  //printf("post receive buffer id->buffer is %s\n", ((struct conn_context *)id->context)->buffer);
}



static void post_receive_buffer(struct rdma_cm_id *id)
{
  struct server_context *ctx = (struct server_context *)id->context;
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)id;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)ctx->recv_buffer;
  sge.length = BUFFER_SIZE;
  sge.lkey = ctx->recv_buffer_mr->lkey;
  TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
  printf("post_receive_buffer: sge addr  is %s\n", sge.addr);
  printf("post_receive_buffer: ctx->recv_buffer is %s\n", ctx->recv_buffer);
}

static void post_receive_message(struct rdma_cm_id *id)
{
  struct server_context *ctx = (struct server_context *)id->context;

  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)id;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)ctx->recv_msg;
  sge.length = sizeof(*ctx->recv_msg);
  sge.lkey = ctx->recv_msg_mr->lkey;

  TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
  printf("post_receive_message: ctx->buffer is %s \n", ctx->recv_buffer);
 //printf("wr lenth is %d\n", wr.sg_list->length);
}

static void on_pre_conn(struct rdma_cm_id *id)
{
  struct server_context *ctx = (struct server_context *)malloc(sizeof(struct server_context));
  printf("on pre_conn start \n");
  id->context = ctx;

  ctx->block_name[0] = '\0'; // take this to mean we don't have the file name

//pd: protect domain , addr, length, access flags(本地写，远程写，远程读，等，0是默认本地读)
//posix_memalign返回BUFFER_SIZE字节的动态内存，并且这块内存的地址是_SC_PAGESIZE的倍数,内存块的地址放在了ctx->buffer里面
  posix_memalign((void **)&ctx->send_buffer, sysconf(_SC_PAGESIZE), BUFFER_SIZE);
  TEST_Z(ctx->send_buffer_mr = ibv_reg_mr(rc_get_pd(), ctx->send_buffer, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE));

  posix_memalign((void **)&ctx->recv_buffer, sysconf(_SC_PAGESIZE), BUFFER_SIZE);
  TEST_Z(ctx->recv_buffer_mr = ibv_reg_mr(rc_get_pd(), ctx->recv_buffer, BUFFER_SIZE,  IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE));

  posix_memalign((void **)&ctx->send_msg, sysconf(_SC_PAGESIZE), sizeof(*ctx->send_msg));
  TEST_Z(ctx->send_msg_mr = ibv_reg_mr(rc_get_pd(), ctx->send_msg, sizeof(*ctx->send_msg), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  posix_memalign((void **)&ctx->recv_msg, sysconf(_SC_PAGESIZE), sizeof(*ctx->recv_msg));
  TEST_Z(ctx->recv_msg_mr = ibv_reg_mr(rc_get_pd(), ctx->recv_msg, sizeof(*ctx->recv_msg), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  post_receive_buffer(id);
  printf("on_pre_conn ** ctx->send_buffer is %s \n", ctx->send_buffer);
  printf("on_pre_conn ** ctx->recv_buffer is %s \n", ctx->recv_buffer);
}

static void on_connection(struct rdma_cm_id *id)
{
  struct server_context *ctx = (struct server_context *)id->context;

  ctx->send_msg->id = MSG_MR;
  ctx->send_msg->data.mr.addr = (uintptr_t)ctx->send_buffer_mr->addr;
  ctx->send_msg->data.mr.rkey = ctx->send_buffer_mr->rkey;

  send_message(id);
   //printf("on_connection   ctx->buffer is %s \n", ctx->buffer);
}
uint32_t send_state = 1;

static void on_completion(struct ibv_wc *wc)
{
  struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)wc->wr_id;
  struct server_context *ctx = (struct server_context *)id->context;
 
 printf("on_completion~~~~~wc->opcode is %d \n",wc->opcode);

  if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
    uint32_t size = ntohl(wc->imm_data);
    printf("size is %d\n", size);
    
     if (size == 0) {
    // if(send_state == 1){
    //   post_receive_buffer(id);
    //   printf("ctx->peer_rkey is %d\n", ctx->peer_rkey);
    //   printf("ctx->peer_addr is %ld\n", ctx->peer_addr);
    //   ctx->msg->id = MSG_READY;
    //   //ctx->buffer = "write remote test \n";
    //   // ctx->peer_addr = ctx->msg->data.mr.addr;
    //   // ctx->peer_rkey = ctx->msg->data.mr.rkey;
    //   write_remote(id, strlen(ctx->buffer)+1);
    //   // ctx->msg->id = MSG_READY;
    //   // ctx->buffer = "write remote test \n";
    //   // send_message(id);
    //   printf("changed ctx->buffer is %s \n", ctx->buffer);
    //   send_state = 0;
    //   printf(" write remote end \n");
      
    // }else{
      
      ctx->send_msg->id = MSG_DONE;
      send_message(id);
      printf("on_completion if ctx->send_msg->id is %s \n", ctx->send_msg->id);
      // don't need post_receive() since we're done with this connection
    //   }
    } else if (ctx->block_name[0]) { //2: receive context
      ssize_t ret;
      printf("received %i bytes.\n", size);
      printf("on_completion 2 ctx->recv_buffer is %s \n", ctx->recv_buffer);
      ret = write(ctx->fd, ctx->send_buffer, size);

      if (ret != size)
        rc_die("write() failed");
      printf("on_completion 2 post_receive ctx->recv_buffer is %s \n", ctx->recv_buffer);
      post_receive_buffer(id);
      printf("on_completion 2e post_receive ctx->recv_buffer is %s \n", ctx->recv_buffer);
      ctx->send_msg->id = MSG_READY;
      send_message(id);

    } else { //1: receive file name

    // ctx->buffer = "write remote test \n";
    // write_remote(id, BUFFER_SIZE);


      size = (size > MAX_FILE_NAME) ? MAX_FILE_NAME : size;
      memcpy(ctx->block_name, ctx->recv_buffer, size);
      ctx->block_name[size - 1] = '\0';

      printf("opening file %s\n", ctx->block_name);

      ctx->fd = open(ctx->block_name, O_WRONLY | O_CREAT | O_EXCL |O_APPEND , S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

      if (ctx->fd == -1)
        rc_die("open() failed");
      printf("on_completion 1 post receive ctx->recv_buffer is %s \n", ctx->recv_buffer);
      post_receive_buffer(id);
      printf("on_completion 1e post receive ctx->recv_buffer is %s \n", ctx->recv_buffer);
   
      ctx->send_msg->id = MSG_READY;
      send_message(id);   
      strcpy(((struct server_context *)id->context)->send_buffer, "write remote test \n");
      
	  //((struct conn_context *)id->context)->buffer = "write remote";
      //ctx->buffer = "write remote test \n";
//	   while(1){}
	  printf("my start: ctx->buffer is %s \n", ctx->send_buffer);
      ctx->peer_addr = ctx->recv_buffer_mr->addr;
      ctx->peer_rkey = ctx->recv_buffer_mr->lkey;
      write_remote(id, strlen(ctx->send_buffer)+1);
      printf(" write remote end \n");
    }
  }
}

static void on_disconnect(struct rdma_cm_id *id)
{
  struct server_context *ctx = (struct server_context *)id->context;

  close(ctx->fd);

  ibv_dereg_mr(ctx->send_buffer_mr);
  ibv_dereg_mr(ctx->recv_buffer_mr);
  ibv_dereg_mr(ctx->send_msg_mr);
  ibv_dereg_mr(ctx->recv_msg_mr);
  
  free(ctx->send_buffer);
  free(ctx->recv_buffer);

  free(ctx->send_msg);
  free(ctx->recv_msg);
  
  printf("on_disconnect and free\n");

  free(ctx);
}

int main(int argc, char **argv)
{
  rc_init(
    on_pre_conn,
    on_connection,
    on_completion,
    on_disconnect);

  printf("waiting for connections. interrupt (^C) to exit.\n");

  rc_server_loop(DEFAULT_PORT);
 printf("server loop end\n");
  return 0;
}
