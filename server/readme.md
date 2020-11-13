# RDMA Code

main->**on_event** event:

RDMA_CM_EVENT_ADDR_RESOLVED->on_addr_resolved

RDMA_CM_EVENT_ROUTE_RESOLVED->on_route_resolved

RDMA_CM_EVENT_ESTABLISHED->on_connection

RDMA_CM_EVENT_DISCONNECTED->on_disconnect



**on_addr_resolved**-> built connection, get_local_message_region

**built_connection**->build_context, build_qp_attr, register_memory,post_receives

**build_context**->pthread_create->poll_cq 

verb(ibv_context) to context,allocate PD, generate CC, CQ, create pthread

void build_context(struct ibv_context *verbs)

​	**poll_cq** -> on_completion

**build_qp_attr**-> set qp attributes, type RC, send/receive wr num 10, sge 1

**register_memory**-> malloc memory, register MR, send/receive MR size is size of message, local/remote region is RDMA_BUFFER_SIZE

**post_receives**-> conn(connection) to wr(ibv_recv_wr), recv->msg to sge.addr, recv_mr->lkey to sge.lkey



**get_local_message_region**-> write: get local region, read: get remote region



**on_route_resolved**->build_params, rdma_connect



**on_connection**->on_connect, send_mr

send_mr->send_message

**poll_cq** -> on_completion
