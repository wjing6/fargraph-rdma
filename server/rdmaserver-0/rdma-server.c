
#include <fcntl.h>
#include <sys/stat.h>
#include "rdma-common.h"
//const size_t BUFFER_SIZE = 1024 *1024 * 1024 ;

static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(struct rdma_cm_id *id);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static void usage(const char *argv0);
const char *DEFAULT_PORT = "1234";

static char* get_filename(struct rdma_cm_id *id);
static char* get_content (char * filename);
static char* get_gridgraph_content(int next);


struct sending_block {
  int id[20];
  char filename[20][20];
   
};
struct graph_work{
  struct sending_block meta;
  struct sending_block column_offset;


  struct sending_block ;
};


int main(int argc, char **argv)
{
  struct sockaddr_in6 addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;
  uint16_t port = 0;

  if (argc != 2)
    usage(argv[0]);

  if (strcmp(argv[1], "write") == 0)
    set_mode(M_WRITE);
  else if (strcmp(argv[1], "read") == 0)
    set_mode(M_READ);
  else
    usage(argv[0]);

  memset(&addr, 0, sizeof(addr));
  addr.sin6_family = AF_INET6;
  addr.sin6_port = htons(atoi(DEFAULT_PORT));

  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

  port = ntohs(rdma_get_src_port(listener));

  printf("listening on port %d.\n", port);

  double begin_time = get_time();

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
      break;
  }
  printf("once read (one event) used %.2f seconds\n", get_time() - begin_time);

  rdma_destroy_id(listener);
  rdma_destroy_event_channel(ec);

  return 0;
}

int on_connect_request(struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;

  printf("received connection request.\n");
  build_connection(id);
  build_params(&cm_params);
  printf("remote buffer: %s\n", get_peer_message_region(id->context));
  printf("local buffer: %s\n", (char *)get_local_message_region(id->context));
  //printf("aaa message from passive/server side with pid %d\n", getpid());
  //sprintf(get_local_message_region(id->context),"aaa message from passive/server side with pid %d\n", getpid());
  //printf("getfilename is %s.\n",get_filename(id));
  //strcpy(get_local_message_region(id->context),get_content(get_filename(id)));
  TEST_NZ(rdma_accept(id, &cm_params));

  return 0;
}

char* get_filename(struct rdma_cm_id *id){
  char* name; 
  name = "/home/wangjing/GridGraph-o/aaaGrid/meta";
  printf("get_peer_message_region is %s.\n", get_peer_message_region(id->context));
  //strcpy(name, get_peer_message_region(id->context));
  //printf("filename is %s.\n",name);
  return name;
}

char* get_content(char * filename){
  char * buffer  = malloc(RDMA_BUFFER_SIZE);
  //FILE *fp;
  int fp ;
  fp = open(filename,O_RDONLY);
  ssize_t size = read(fp, buffer, RDMA_BUFFER_SIZE);
  //fgets(buffer,RDMA_BUFFER_SIZE, fp);
  //fread(buffer , RDMA_BUFFER_SIZE , sizeof(),fp );

  if (size == -1) die("read file failed.\n");
  printf("read file successfully.buffer is : %s.\n",buffer);
  return buffer;
}




int on_connection(struct rdma_cm_id *id)
{
  on_connect(id->context);
  //send_mr(id->context);
  printf("peer on_connection : %s\n", (char *)get_peer_message_region(id->context));
  printf("local on_connection : %s\n", (char *)get_local_message_region(id->context));
  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  printf("peer disconnected.\n");

  destroy_connection(id->context);
  return 0;
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
    r = on_connect_request(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

void usage(const char *argv0)
{
  fprintf(stderr, "usage: %s <mode>\n  mode = \"read\", \"write\"\n", argv0);
  exit(1);
}
