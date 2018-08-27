#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#define VERSION 23
#define BUFSIZE 8096
#define ERROR      42
#define LOG        44
#define FORBIDDEN 403
#define NOTFOUND  404

#include <infiniband/verbs.h>
#include <linux/types.h>
//#include "config.h"
#include <assert.h>
#include <sys/time.h>
#include <netdb.h>
#include <malloc.h>
#include <getopt.h>
#include <time.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/mman.h>
//#include "pingpong.h"

#define EAGER_PROTOCOL_LIMIT (1 << 12) /* 4KB limit */
#define MAX_TEST_SIZE (10 * EAGER_PROTOCOL_LIMIT)
#define TEST_LOCATION "~/www/"
#define MAX_SERVER_ENTRIES 1000
#define NUM_SERVERS 2
typedef int bool;
#define true 1
#define false 0

static int page_size;
static int use_ts;
static int portNum;
static char* servername;

enum packet_type {
    EAGER_GET_REQUEST,
    EAGER_GET_RESPONSE,
    EAGER_SET_REQUEST,
    EAGER_SET_RESPONSE,

    RENDEZVOUS_GET_REQUEST,
    RENDEZVOUS_GET_RESPONSE,
    RENDEZVOUS_SET_REQUEST,
    RENDEZVOUS_SET_RESPONSE,
    TERMINATE,
#ifdef EX4
    FIND,
    LOCATION,
#endif
};
struct packet {
    enum packet_type type; /* What kind of packet/protocol is this */
    union {
        /* The actual packet type will determine which struct will be used: */

        struct {
            /* TODO */
            unsigned keyLen;
            char key[0]; //key will be an array of size len
            
        } eager_get_request;
		
        struct {
            unsigned valueLen;
            char value[0];
        } eager_get_response;

        /* EAGER PROTOCOL PACKETS */
        struct {
#ifdef EX4
            unsigned value_length; /* value is binary, so needs to have length! */
#endif
            unsigned keyLen;
            unsigned valueLen;
            char key_and_value[0]; /* null terminator between key and value */
        } eager_set_request;

        struct {
            /* TODO check what server responds to eager set req*/
            //unsigned keyLen;
            //unsigned valueLen;
            //char key_and_value[0];
        } eager_set_response;

        /* RENDEZVOUS PROTOCOL PACKETS */
        struct {
            int keyLen;
            char key[0];
        } rndv_get_request;

        struct {
            uint64_t remote_address;
            uint32_t rkey;
            int valueLen;
        } rndv_get_response;

        struct {
            int keyLen;
            int valueLen;
            char key[0];
        } rndv_set_request;

        struct {
            uint64_t remote_address;
            uint32_t rkey;
        } rndv_set_response;

		/* TODO - maybe there are more packet types? */
					
        struct {
            unsigned keyLen;
            unsigned num_of_servers;
            char key[0];
        } find;

        struct {
            unsigned selected_server;
        } location;
    };
};

struct pingpong_context {
	struct ibv_context	*context; //the connections context (channels live inside the context)
	struct ibv_comp_channel *channel;
	struct ibv_pd		*pd;
	struct ibv_mr		*mr; //this is the memory region we work with
	struct ibv_dm		*dm;
	union {
		struct ibv_cq		*cq;
		struct ibv_cq_ex	*cq_ex;
	} cq_s;
	struct ibv_qp		*qp; //this is the queue pair array we work with
	char			*buf;
	int			 size;
	int			 send_flags;
	int			 rx_depth;
	int			 pending;
	struct ibv_port_attr     portinfo;
	uint64_t		 completion_timestamp_mask;
};

struct pingpong_dest {
	int lid;
	int qpn;
	int psn;
	union ibv_gid gid;
};
struct kv_server_address {
    char *servername; /* In the last item of an array this is NULL */
    short port; /* This is useful for multiple servers on a host */
};
struct kv_handle
{
    struct pingpong_context * ctx;//context
    struct ibv_mr * registeredMR[MAX_SERVER_ENTRIES];
    
    int numRegistered;
    int entryLen;
    int * keyLen;
    int * valueLen;
    char ** keys;
    char ** values;
};

struct dkv_handle
{
    struct kv_handle * indexer;
    struct kv_handle * serverHandles[NUM_SERVERS];
};
enum ibv_mtu pp_mtu_to_enum(int mtu)
{
	switch (mtu) {
	case 256:  return IBV_MTU_256;
	case 512:  return IBV_MTU_512;
	case 1024: return IBV_MTU_1024;
	case 2048: return IBV_MTU_2048;
	case 4096: return IBV_MTU_4096;
	default:   return 0;
	}
}

enum {
	PINGPONG_RECV_WRID = 1,
	PINGPONG_SEND_WRID = 2,
};

void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
	char tmp[9];
	__be32 v32;
	int i;
	uint32_t tmp_gid[4];

	for (tmp[8] = 0, i = 0; i < 4; ++i) {
		memcpy(tmp, wgid + i * 8, 8);
		sscanf(tmp, "%x", &v32);
		tmp_gid[i] = be32toh(v32);
	}
	memcpy(gid, tmp_gid, sizeof(*gid));
}

void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
	uint32_t tmp_gid[4];
	int i;

	memcpy(tmp_gid, gid, sizeof(tmp_gid));
	for (i = 0; i < 4; ++i)
		sprintf(&wgid[i * 8], "%08x", htobe32(tmp_gid[i]));
}

int pp_get_port_info(struct ibv_context *context, int port,
		     struct ibv_port_attr *attr)
{
	return ibv_query_port(context, port, attr);
}

static struct ibv_cq *pp_cq(struct pingpong_context *ctx)
{
	return use_ts ? ibv_cq_ex_to_cq(ctx->cq_s.cq_ex) :
		ctx->cq_s.cq;
}
//connect qp_num_to_connect in context to this pingpong dest.
static int pp_connect_ctx(struct pingpong_context *ctx, int port, int my_psn,
			  enum ibv_mtu mtu, int sl,
			  struct pingpong_dest *dest, int sgid_idx)
{
	struct ibv_qp_attr attr = {
		.qp_state		= IBV_QPS_RTR,
		.path_mtu		= mtu,
		.dest_qp_num		= dest->qpn,
		.rq_psn			= dest->psn,
		.max_dest_rd_atomic	= 1,
		.min_rnr_timer		= 12,
		.ah_attr		= {
			.is_global	= 0,
			.dlid		= dest->lid,
			.sl		= sl,
			.src_path_bits	= 0,
			.port_num	= port
		}
	};

	if (dest->gid.global.interface_id) {
		attr.ah_attr.is_global = 1;
		attr.ah_attr.grh.hop_limit = 1;
		attr.ah_attr.grh.dgid = dest->gid;
		attr.ah_attr.grh.sgid_index = sgid_idx;
	}
	if (ibv_modify_qp(((*ctx).qp), &attr,
			  IBV_QP_STATE              |
			  IBV_QP_AV                 |
			  IBV_QP_PATH_MTU           |
			  IBV_QP_DEST_QPN           |
			  IBV_QP_RQ_PSN             |
			  IBV_QP_MAX_DEST_RD_ATOMIC |
			  IBV_QP_MIN_RNR_TIMER)) {
		fprintf(stderr, "Failed to modify QP to RTR\n");
		return 1;
	}

	attr.qp_state	    = IBV_QPS_RTS;
	attr.timeout	    = 14;// 0 is infinite wait
	attr.retry_cnt	    = 7;
	attr.rnr_retry	    = 7;
	attr.sq_psn	    = my_psn;
	attr.max_rd_atomic  = 1;
	if (ibv_modify_qp(((*ctx).qp), &attr,
			  IBV_QP_STATE              |
			  IBV_QP_TIMEOUT            |
			  IBV_QP_RETRY_CNT          |
			  IBV_QP_RNR_RETRY          |
			  IBV_QP_SQ_PSN             |
			  IBV_QP_MAX_QP_RD_ATOMIC)) {
		fprintf(stderr, "Failed to modify QP to RTS\n");
		return 1;
	}

	return 0;
}

static struct pingpong_dest *pp_client_exch_dest(const char *servername, int port,
						 const struct pingpong_dest *my_dest)
{
	struct addrinfo *res, *t;
	struct addrinfo hints = {
		.ai_family   = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM
	};
	char *service;
	char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
	int n;
	int sockfd = -1;
	struct pingpong_dest *rem_dest;
  struct pingpong_dest dest;
	char gid[33];

	if (asprintf(&service, "%d", port) < 0)
		return NULL;
	n = getaddrinfo(servername, service, &hints, &res);

	if (n < 0) {
		fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
		free(service);
		return NULL;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
            //sleep(1);
            
			if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
            {
               
				break;
            }
			close(sockfd);
			sockfd = -1;
		}
	}
	freeaddrinfo(res);
	free(service);
  
	if (sockfd < 0) {
		fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
		return NULL;
	}
    rem_dest = malloc((sizeof dest));
        if (!rem_dest)
            goto out;
    
    gid_to_wire_gid(&(my_dest->gid), gid);
   sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn,
                            my_dest->psn, gid);
   if (write(sockfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        goto out;
   }

    if (read(sockfd, msg, sizeof msg) != sizeof msg ||
        write(sockfd, "done", sizeof "done") != sizeof "done") {
        perror("client read/write");
        fprintf(stderr, "Couldn't read/write remote address\n");
        goto out;
    }
//
    

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
                        &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);
    
out:
	close(sockfd);
	return rem_dest;
}

static struct pingpong_context *pp_init_ctx(struct ibv_device *ib_dev, int size,
					    int rx_depth, int port,
					    int use_event)
{
    
	struct pingpong_context *ctx;
	int access_flags = IBV_ACCESS_LOCAL_WRITE;

	ctx = calloc(1, sizeof *ctx);
	if (!ctx)
		return NULL;

	ctx->size       = size;
	ctx->send_flags = IBV_SEND_SIGNALED;
	ctx->rx_depth   = rx_depth;

	ctx->buf = memalign(page_size, size);
	if (!ctx->buf) {
		fprintf(stderr, "Couldn't allocate work buf.\n");
		goto clean_ctx;
	}

	memset(ctx->buf, 0x7b, size);

	ctx->context = ibv_open_device(ib_dev);
	if (!ctx->context) {
		fprintf(stderr, "Couldn't get context for %s\n",
			ibv_get_device_name(ib_dev));
		goto clean_buffer;
	}

	if (use_event) {
		ctx->channel = ibv_create_comp_channel(ctx->context);
		if (!ctx->channel) {
			fprintf(stderr, "Couldn't create completion channel\n");
			goto clean_device;
		}
	} else
		ctx->channel = NULL;

	ctx->pd = ibv_alloc_pd(ctx->context);
	if (!ctx->pd) {
		fprintf(stderr, "Couldn't allocate PD\n");
		goto clean_comp_channel;
	}


	ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size, access_flags);

	if (!ctx->mr) {
		fprintf(stderr, "Couldn't register MR\n");
		goto clean_dm;
	}

		ctx->cq_s.cq = ibv_create_cq(ctx->context, rx_depth + 1, NULL,
					     ctx->channel, 0);
	

	if (!pp_cq(ctx)) {
		fprintf(stderr, "Couldn't create CQ\n");
		goto clean_mr;
	}
    
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr = {
        .send_cq = pp_cq(ctx),
        .recv_cq = pp_cq(ctx),
        .cap     = {
            .max_send_wr  = 1,
            .max_recv_wr  = rx_depth,
            .max_send_sge = 1,
            .max_recv_sge = 1
        },
        .qp_type = IBV_QPT_RC
    };

    ((*ctx).qp) = ibv_create_qp(ctx->pd, &init_attr);////////////
    if (!((*ctx).qp))  {
        fprintf(stderr, "Couldn't create QP\n");
        goto clean_cq;
    }

    ibv_query_qp(((*ctx).qp), &attr, IBV_QP_CAP, &init_attr);
    if (init_attr.cap.max_inline_data >= size) {
        ctx->send_flags |= IBV_SEND_INLINE;
    }



    struct ibv_qp_attr attr2 = {
        .qp_state        = IBV_QPS_INIT,
        .pkey_index      = 0,
        .port_num        = port,
        .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
    };

    if (ibv_modify_qp((*ctx).qp, &attr2,
              IBV_QP_STATE              |
              IBV_QP_PKEY_INDEX         |
              IBV_QP_PORT               |
              IBV_QP_ACCESS_FLAGS)) {
        fprintf(stderr, "Failed to modify QP to INIT\n");
        goto clean_qp;
    }
	

	return ctx;

clean_qp:
    
        ibv_destroy_qp((*ctx).qp);

clean_cq:
	ibv_destroy_cq(pp_cq(ctx));

clean_mr:
	ibv_dereg_mr(ctx->mr);

clean_dm:
	if (ctx->dm)

clean_pd:
	ibv_dealloc_pd(ctx->pd);

clean_comp_channel:
	if (ctx->channel)
		ibv_destroy_comp_channel(ctx->channel);

clean_device:
	ibv_close_device(ctx->context);

clean_buffer:
	free(ctx->buf);

clean_ctx:
	free(ctx);

	return NULL;
}


static int pp_close_ctx(struct pingpong_context *ctx)
{
   
    if ( ibv_destroy_qp((*ctx).qp)) {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }

	if (ibv_destroy_cq(pp_cq(ctx))) {
		fprintf(stderr, "Couldn't destroy CQ\n");
		return 1;
	}

	if (ibv_dereg_mr(ctx->mr)) {
		fprintf(stderr, "Couldn't deregister MR\n");
		return 1;
	}

	if (ctx->dm) {
	}

	if (ibv_dealloc_pd(ctx->pd)) {
		fprintf(stderr, "Couldn't deallocate PDcl\n");
		return 1;
	}

	if (ctx->channel) {
		if (ibv_destroy_comp_channel(ctx->channel)) {
			fprintf(stderr, "Couldn't destroy completion channel\n");
			return 1;
		}
	}

	if (ibv_close_device(ctx->context)) {
		fprintf(stderr, "Couldn't release context\n");
		return 1;
	}

	free(ctx->buf);
	free(ctx);

	return 0;
}

static int pp_post_recv(struct pingpong_context *ctx, int n)
{
    int qp_num = 0;
	struct ibv_sge list = {
		.addr	= (uintptr_t) ctx->buf,
		.length = ctx->size,
		.lkey	= ctx->mr->lkey
	};
	struct ibv_recv_wr wr = {
		.wr_id	    = PINGPONG_RECV_WRID,
		.sg_list    = &list,
		.num_sge    = 1,
	};
	struct ibv_recv_wr *bad_wr;
	int i;

	for (i = 0; i < n; ++i)
		if (ibv_post_recv((*ctx).qp, &wr, &bad_wr))
			break;

	return i;
}

static int pp_post_send(struct pingpong_context *ctx, enum ibv_wr_opcode opcode, unsigned size, const char *local_ptr,uint32_t lkey, uint64_t remote_ptr, uint32_t remote_key)
{
    printf("voosh\n");
    printf("lkey is %d vs %d\nlocal ptr is %d vs %d\n",(lkey ? lkey : ctx->mr->lkey),ctx->mr->lkey,(local_ptr ? local_ptr : ctx->buf),ctx->buf);
	printf("server data in client: rkey = %d \n remote_addr = %d\n",remote_key,remote_ptr);
    struct ibv_sge list = {
		.addr	= (uintptr_t) (local_ptr ? local_ptr : ctx->buf),
		.length = size,
		.lkey	= (lkey ? lkey : ctx->mr->lkey)
	};
	struct ibv_send_wr wr = {
		.wr_id	    = PINGPONG_SEND_WRID,
		.sg_list    = &list,
		.num_sge    = 1,
		.opcode     = opcode,
		.send_flags = IBV_SEND_SIGNALED,
		.next       = NULL
	};
	struct ibv_send_wr *bad_wr;
	
	if (remote_ptr) {
		wr.wr.rdma.remote_addr = (uintptr_t) remote_ptr;
		wr.wr.rdma.rkey = remote_key;
	}
    printf("posting send\n");
    //return ibv_post_send((*ctx).qp, &wr, &bad_wr);
	return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

////////////////////
int pp_wait_completions(struct kv_handle *handle, int iters,char ** answerBuffer,const char* valueToSet,int * valueLen)
{
    struct pingpong_context* ctx = handle->ctx;
    int rcnt, scnt, num_cq_events, use_event = 0;
	rcnt = scnt = 0;
    printf("waiting completions\n");
	while (rcnt + scnt < iters) {
		struct ibv_wc wc[2];
		int ne, i;

		do {
			ne = ibv_poll_cq(pp_cq(ctx), 2, wc);
			if (ne < 0) {
				fprintf(stderr, "poll CQ failed %d\n", ne);
				return 1;
			}

		} while (ne < 1);

		for (i = 0; i < ne; ++i) {
			if (wc[i].status != IBV_WC_SUCCESS) {
				fprintf(stderr, "Failed status %s (%d) for wr_id %d opcode %d\n",
					ibv_wc_status_str(wc[i].status),
					wc[i].status, (int) wc[i].wr_id, wc[i].opcode);
				return 1;
			}
            printf("successful wc\n");
            struct packet* gotten_packet;
			switch ((int) wc[i].wr_id) {
			case PINGPONG_SEND_WRID:
                //printf("msg sent successful\n");
                scnt = scnt + 1;
				break;

			case PINGPONG_RECV_WRID:
				//handle_server_packets_only(handle, (struct packet*)&ctx->buf);
				gotten_packet = (struct packet*)ctx->buf;
                if(gotten_packet->type == EAGER_GET_RESPONSE)
                {
                    *answerBuffer = malloc(gotten_packet->eager_get_response.valueLen * sizeof(char));
                    memcpy(*answerBuffer,gotten_packet->eager_get_response.value,gotten_packet->eager_get_response.valueLen);
                    *valueLen = gotten_packet->eager_get_response.valueLen;
                    //printf("Answer buffer:\n %s\n",*answerBuffer);
                }
                else if(gotten_packet->type == EAGER_SET_RESPONSE)
                {
                    //printf("set is done on server, continuing\n");
                }
                else if(gotten_packet->type == RENDEZVOUS_GET_RESPONSE)
                {
                    //printf("gotten rndv get response\n");
                    *answerBuffer = malloc(gotten_packet->rndv_get_response.valueLen * sizeof(char));
                    *valueLen = gotten_packet->rndv_get_response.valueLen;
                    //register memory at value in size valueLen, and sendit to packet data
                    handle->registeredMR[handle->numRegistered] = ibv_reg_mr(ctx->pd, *answerBuffer,
                                                    gotten_packet->rndv_get_response.valueLen, IBV_ACCESS_LOCAL_WRITE |
                                                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ); 
                    handle->numRegistered = handle->numRegistered + 1;
                    pp_post_send(handle->ctx,IBV_WR_RDMA_READ,gotten_packet->rndv_get_response.valueLen,
                            *answerBuffer,handle->registeredMR[handle->numRegistered-1]->lkey,
                            gotten_packet->rndv_get_response.remote_address
                            ,gotten_packet->rndv_get_response.rkey);
                    pp_wait_completions(handle, 1,NULL,NULL,NULL);//wait for comp
                    //printf("RDMA recieved\n");
                }
                else if(gotten_packet->type == RENDEZVOUS_SET_RESPONSE)
                {
                    printf("got rend set response@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
                    //printf("will set string: %s\n",valueToSet);
                    //register memory at value in size valueLen, and sendit to packet data
                    handle->registeredMR[handle->numRegistered] = ibv_reg_mr(ctx->pd,(void*) valueToSet,
                                                    (unsigned) *valueLen, IBV_ACCESS_LOCAL_WRITE |
                                                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ); 
                    if (!handle->registeredMR[handle->numRegistered]) {
                        perror( "Error, ibv_reg_mr() failed\n");
                        exit(14);
                    }
                    handle->numRegistered = handle->numRegistered + 1;
                    printf("mem registered, valueLen = %d\n",*valueLen);
                    printf("gotten keys: radd = %d\nrkey = %d\n",gotten_packet->rndv_set_response.remote_address
                            ,gotten_packet->rndv_set_response.rkey);
                    printf("sending keys: radd = %d\n",handle->registeredMR[handle->numRegistered-1]->addr);
                    printf("sending key lkey = %d\n",handle->registeredMR[handle->numRegistered-1]->lkey);
                    pp_post_send(handle->ctx,IBV_WR_RDMA_WRITE,*valueLen,
                            handle->registeredMR[handle->numRegistered-1]->addr,
                            handle->registeredMR[handle->numRegistered-1]->lkey,
                            gotten_packet->rndv_set_response.remote_address
                            ,gotten_packet->rndv_set_response.rkey);
                    printf("sent, waiting for completion\n");
                    pp_wait_completions(handle, 1,NULL,NULL,NULL);//wait for comp
                    printf("RDMA sent\n");//handle->registeredMR[handle->numRegistered-1]->addr);
                }
                else if(gotten_packet->type == LOCATION)
                {
                    printf("location packet arrived\n");
                    *answerBuffer = malloc(sizeof(char));
                    //memcpy(*answerBuffer,&gotten_packet->location.selected_server,sizeof(unsigned));
                    **answerBuffer = (char)gotten_packet->location.selected_server;
                    
                }
                pp_post_recv(ctx, 1);
                rcnt = rcnt + 1;
				break;

			default:
				fprintf(stderr, "Completion for unknown wr_id %d\n",
					(int) wc[i].wr_id);
				return 1;
			}
		}
	}
    printf("done working on packet\n");
	return 0;
}

/*struct dkv_handle
{
    struct kv_handle indexer;
    struct kv_handle serverHandles[NUM_SERVERS];
};*/
unsigned getServerNumFromIndexer(struct dkv_handle *dkv_h, const char *key)
{
    struct pingpong_context *ctx_indexer = dkv_h->indexer->ctx;
    struct packet *set_packet = (struct packet*)ctx_indexer->buf;

    unsigned packet_size = strlen(key) +1 /*+2*sizeof(unsigned)*/ + sizeof(struct packet);
    
    //printf("type is %d\n",EAGER_GET_REQUEST);
    set_packet->type = FIND;
    //printf("sending eager get.\n key = %s\n",key);
    set_packet->find.keyLen = strlen(key) + 1;
    
    memcpy(set_packet->find.key,key,strlen(key) + 1);
    set_packet->find.num_of_servers = NUM_SERVERS;
    printf("finding key: %s\nnum servers: %d\n",set_packet->find.key,set_packet->find.num_of_servers );
    /* TODO (4LOC): fill in the rest of the get_packet */
    //printf("send %s\n",set_packet->eager_get_request.key);
    printf("packet size is %d.\nchar after packet size = %c\nlast char in msg is = %c\n",packet_size,set_packet->eager_set_request.key_and_value[packet_size-sizeof(struct packet)],set_packet->eager_set_request.key_and_value[packet_size-1-sizeof(struct packet)]);
    //printf("packet type is %d\n",set_packet->type);
    pp_post_send(ctx_indexer, IBV_WR_SEND, packet_size, NULL,0, 0, 0); /* Sends the packet to the server */
    printf("packet sent\n");
    char ** value = malloc(sizeof(char*));
    int retVal = pp_wait_completions(dkv_h->indexer, 2,value,NULL,NULL);
    if(retVal != 0)
    {
        return -1;
    }
    printf("server num recieved is %d\n", **value);
    return (unsigned) **value;
}


int kv_set(struct kv_handle *kv_handle, const char *key, const char *value, unsigned valueLength)
{
    struct pingpong_context *ctx = kv_handle->ctx;
    struct packet *set_packet = (struct packet*)ctx->buf;

    unsigned packet_size = strlen(key) + 1 + valueLength + sizeof(struct packet);
    if (packet_size < (EAGER_PROTOCOL_LIMIT)) {
        /* Eager protocol - exercise part 1 */
        set_packet->type = EAGER_SET_REQUEST;
        //printf("sending eager.\n key = %s\n value = %s\n",key,value);
        set_packet->eager_set_request.keyLen = strlen(key) + 1;
        set_packet->eager_set_request.valueLen = valueLength;
        memcpy(set_packet->eager_set_request.key_and_value,key,strlen(key) + 1);
        memcpy(&(set_packet->eager_set_request.key_and_value[strlen(key) + 1]),value,valueLength);
        /* TODO (4LOC): fill in the rest of the set_packet */
        //printf("send %s\n",set_packet->eager_set_request.key_and_value);
        //printf("packet size is %d.\nchar after packet size = %c\nlast char in msg is = %c\n",packet_size,set_packet->eager_set_request.key_and_value[packet_size-sizeof(struct packet)],set_packet->eager_set_request.key_and_value[packet_size-1-sizeof(struct packet)]);
        //printf("packet type is %d\n",set_packet->type);
        pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL, 0, 0, 0); /* Sends the packet to the server */
        //printf("packet sent\n");
        return pp_wait_completions(kv_handle, 2,NULL,NULL,NULL); /* await EAGER_SET_REQUEST completion and EAGER_SET_RESPONSE */
    }

    /* Otherwise, use RENDEZVOUS - exercise part 2 */
    //Flow: send a request to send this big data, recv the rkey to the registered 
    //memory then use RDMA_WRITE
    set_packet->type = RENDEZVOUS_SET_REQUEST;
    
    printf("randevo key is %s\n",key);
    set_packet->rndv_set_request.keyLen = strlen(key) + 1;
    set_packet->rndv_set_request.valueLen = valueLength;
    memcpy(set_packet->rndv_set_request.key,key,strlen(key) + 1);
    packet_size = strlen(key) + 1 + sizeof(struct packet);
    printf("packet size is %d\n",packet_size);
    pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL,0, 0, 0); /* Sends the packet to the server */    
    int * valLen = malloc(sizeof(int));
    *valLen = valueLength;
    printf("file size is %d\n",*valLen);
    return (pp_wait_completions(kv_handle, 2,NULL,value,valLen));//sent value. wait for RD_SET_RESPONSE and RDMA_WRITE the value
    /*
    pp_post_recv(ctx, 1); // Posts a receive-buffer for RENDEZVOUS_SET_RESPONSE 
    pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL, NULL, 0); // Sends the packet to the server 
    assert(pp_wait_completions(kv_handle, 2,NULL)); // wait for both to complete 

    assert(set_packet->type == RENDEZVOUS_SET_RESPONSE);
    pp_post_send(ctx, IBV_WR_RDMA_WRITE, packet_size, value, NULL, 0);// TODO (1LOC): replace with remote info for RDMA_WRITE from packet );
    return pp_wait_completions(kv_handle, 1,NULL);*/ // wait for both to complete 
}
int dkv_set(struct dkv_handle *dkv_h, const char *key, const char *value, unsigned length)
 {
    printf("dkv setting\n");
    unsigned serverToContact = getServerNumFromIndexer(dkv_h, key);
    //printf("setting:\nkey: %s \nvalue: %s \nto server: %d\n",key,value,(int)serverToContact);
    return kv_set(dkv_h->serverHandles[serverToContact], key, value,length);
 }
int kv_get(struct kv_handle *kv_handle, const char *key, char **value, unsigned *length)
{
    struct pingpong_context *ctx = kv_handle->ctx;
    struct packet *set_packet = (struct packet*)ctx->buf;

    unsigned packet_size = strlen(key) + sizeof(struct packet);
    
    //printf("type is %d\n",EAGER_GET_REQUEST);
    set_packet->type = EAGER_GET_REQUEST;
    //printf("sending eager get.\n key = %s\n",key);
    set_packet->eager_get_request.keyLen = strlen(key) + 1;
    
    memcpy(set_packet->eager_get_request.key,key,strlen(key) + 1);

    /* TODO (4LOC): fill in the rest of the get_packet */
    //printf("send %s\n",set_packet->eager_get_request.key);
    //printf("packet size is %d.\nchar after packet size = %c\nlast char in msg is = %c\n",packet_size,set_packet->eager_set_request.key_and_value[packet_size-sizeof(struct packet)],set_packet->eager_set_request.key_and_value[packet_size-1-sizeof(struct packet)]);
    //printf("packet type is %d\n",set_packet->type);
    pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL,0, 0, 0); /* Sends the packet to the server */
    //printf("packet sent\n");
    return pp_wait_completions(kv_handle, 2,value,NULL,length); /* await EAGER_GET_REQUEST completion, and EAGER_GET_RESPONSE answer */
    
}

int dkv_get(struct dkv_handle *dkv_h, const char *key, char **value, unsigned *length)
{
    
    unsigned serverToContact = getServerNumFromIndexer(dkv_h, key);
    return kv_get(dkv_h->serverHandles[serverToContact], key, value,length);
    
}

void kv_release(char *value)
{
    /* TODO (2LOC): free value */
    free(value);
}

void dkv_release(char *value)
{
    kv_release(value);
}


#ifdef EX3
#define my_open  kv_open
#define set      kv_set
#define get      kv_get
#define release  kv_release
#define my_close kv_close
#endif /* EX3 */

#ifdef EX4
#define my_open  dkv_open
#define set      dkv_set
#define get      dkv_get
#define release  dkv_release
#define my_close dkv_close
#endif /* EX4 */


void terminateServer(struct kv_handle * handle)
{
    struct pingpong_context *ctx = handle->ctx;
    struct packet *set_packet = (struct packet*)ctx->buf;

    unsigned packet_size = sizeof(struct packet);
    set_packet->type = TERMINATE;
    pp_post_send(ctx, IBV_WR_SEND, packet_size, NULL,0, 0, 0); /* Sends the packet to the server */
}

//////////////////////
int kv_open(struct kv_server_address *server, struct kv_handle *kv_handle)
{
    //printf("a\n");
    struct ibv_device      **dev_list;
	struct ibv_device	*ib_dev;
	struct pingpong_context *context = malloc(sizeof(struct pingpong_context));
	struct pingpong_dest    my_dest;
	struct pingpong_dest    *rem_dest;
	struct timeval           timer;
	char                    *ib_devname = NULL;
	unsigned int             port = 18515;
	int                      ib_port = 1;
    page_size = sysconf(_SC_PAGESIZE); //checks the page size used by the system

	enum ibv_mtu		 mtu = IBV_MTU_1024;
	unsigned int             rx_depth = 50;
	
	int                      use_event = 0;
	int                      routs;
	int                      num_cq_events;//only one completion queue
	int                      sl = 0;
	int			 gidx = -1;
	char			 gid[33];
	//struct ts_params	 ts;

	srand48(getpid() * time(NULL));
    //printf("a\n");
    portNum = (int)server->port;
    port = portNum;
    servername = server->servername;
    //printf("a\n");
      //get our beloved device
    dev_list = ibv_get_device_list(NULL); //get devices available to this machine
	if (!dev_list) {
		perror("Failed to get IB devices list");
		return 1;
	}
    printf("device gotten is: %s\n",ibv_get_device_name(*dev_list));
    //printf("b\n");
    ib_dev = *dev_list; //chooses the first device by default
    if (!ib_dev) {
        fprintf(stderr, "No IB devices found\n");
        return 1;
    }
    //printf("a\n");
    //Create the context for this connection
    //creates context on found device, registers memory of size.
    context = pp_init_ctx(ib_dev, EAGER_PROTOCOL_LIMIT, rx_depth, ib_port, use_event); //use_event (decides if we wait blocking for completion)
    if (!context)
        return 1;
    //printf("a\n");
    if (pp_get_port_info(context->context, ib_port, &context->portinfo)) { //gets the port status and info (uses ibv_query_port)
            fprintf(stderr, "Couldn't get port info\n");
            return 1;
        }
    //printf("a\n");
    //Prepare to recieve messages. fill the recieve request queue of QP k
    routs = pp_post_recv(context, context->rx_depth); //post rx_depth recieve requests
        if (routs < context->rx_depth) {
            fprintf(stderr, "Couldn't post receive (%d)\n", routs);
            return 1;
        }
    //printf("a\n");
    //set my_dest for every QP, getting ready to connect them.
    my_dest.lid = context->portinfo.lid; //assigns lid to my dest
    if (context->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET &&
                            !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }
    //printf("a\n");
    //set the gid to 0, we are in the same subnet.
    memset(&my_dest.gid, 0, sizeof my_dest.gid); //zero the gid, we send in the same subnet
    my_dest.qpn = ((*context).qp)->qp_num; //gets the qp number
    my_dest.psn = lrand48() & 0xffffff; //randomizes the packet serial number
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid); //changes gid to text form
    //printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
     //      my_dest[k].lid, my_dest[k].qpn, my_dest[k].psn, gid);
    
    //Get the remote dest for my QPs
    rem_dest = pp_client_exch_dest(servername, port, &my_dest); //if youre a client - exchange data with server
    if (!rem_dest)
            return 1; 
    //printf("a\n");

    
      inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
      //printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
      //       rem_dest[k].lid, rem_dest[k].qpn, rem_dest[k].psn, gid);
          
    //now connect all the QPs to the server
    
    if (pp_connect_ctx(context, ib_port, my_dest.psn, mtu, sl, rem_dest,
                    gidx))
            return 1; //connect to the server
    //printf("a\n");
    
   
    //Do client work
    
    kv_handle->ctx = context;
    ibv_free_device_list(dev_list);
    //printf("a\n");
    free(rem_dest);
    //printf("a\n");
    return 0;//orig_main(server, EAGER_PROTOCOL_LIMIT, g_argc, g_argv, &kv_handle->ctx);
}



int dkv_open(struct kv_server_address **servers, /* array of servers */
 struct kv_server_address *indexer, /* single indexer */
 struct dkv_handle *dkv_h)
 {
     if(kv_open(indexer,dkv_h->indexer))
    {
        return 0;
    }
    free(indexer);
    for(int serverNum = 0; serverNum < NUM_SERVERS; serverNum++)
    {
        if(kv_open(servers[serverNum],dkv_h->serverHandles[serverNum]))
        {
            return 0;
        }
        free(servers[serverNum]);
    }
     
 }

int kv_close(struct kv_handle *kv_handle)
{
    terminateServer(kv_handle);
    for(int i = 0; i < kv_handle->numRegistered; i = i + 1)
    {
        void * memory = kv_handle->registeredMR[i]->addr;
        ibv_dereg_mr(kv_handle->registeredMR[i]);
        //free(memory);
    }
    
    return pp_close_ctx(kv_handle->ctx);
}

int dkv_close(struct dkv_handle *dkv_h)
{
    terminateServer(dkv_h->indexer);
    for(int i=0; i< NUM_SERVERS; i++)
    {
        kv_close(dkv_h->serverHandles[i]);
    }
}

/*struct dkv_handle
{
    struct kv_handle * indexer;
    struct kv_handle * serverHandles[NUM_SERVERS];
};
*/
struct {
	char *ext;
	char *filetype;
} extensions [] = {
	{"gif", "image/gif" },  
	{"jpg", "image/jpg" }, 
	{"jpeg","image/jpeg"},
	{"png", "image/png" },  
	{"ico", "image/ico" },  
	{"zip", "image/zip" },  
	{"gz",  "image/gz"  },  
	{"tar", "image/tar" },  
	{"htm", "text/html" },  
	{"html","text/html" },  
	{0,0} };

void logger(int type, char *s1, char *s2, int socket_fd)
{
	int fd ;
	char logbuffer[BUFSIZE*2];

	switch (type) {
	case ERROR: (void)sprintf(logbuffer,"ERROR: %s:%s Errno=%d exiting pid=%d",s1, s2, errno,getpid()); 
		break;
	case FORBIDDEN: 
		(void)write(socket_fd, "HTTP/1.1 403 Forbidden\nContent-Length: 185\nConnection: close\nContent-Type: text/html\n\n<html><head>\n<title>403 Forbidden</title>\n</head><body>\n<h1>Forbidden</h1>\nThe requested URL, file type or operation is not allowed on this simple static file webserver.\n</body></html>\n",271);
		(void)sprintf(logbuffer,"FORBIDDEN: %s:%s",s1, s2); 
		break;
	case NOTFOUND: 
		(void)write(socket_fd, "HTTP/1.1 404 Not Found\nContent-Length: 136\nConnection: close\nContent-Type: text/html\n\n<html><head>\n<title>404 Not Found</title>\n</head><body>\n<h1>Not Found</h1>\nThe requested URL was not found on this server.\n</body></html>\n",224);
		(void)sprintf(logbuffer,"NOT FOUND: %s:%s",s1, s2); 
		break;
	case LOG: (void)sprintf(logbuffer," INFO: %s:%s:%d",s1, s2,socket_fd); break;
	}	
	/* No checks here, nothing can be done with a failure anyway */
	if((fd = open("nweb.log", O_CREAT| O_WRONLY | O_APPEND,0644)) >= 0) {
		(void)write(fd,logbuffer,strlen(logbuffer)); 
		(void)write(fd,"\n",1);      
		(void)close(fd);
	}
	if(type == ERROR || type == NOTFOUND || type == FORBIDDEN) exit(3);
}

/* this is a child web server process, so we can exit on errors */
void web(int fd, int hit,struct dkv_handle * dkvHandle)
{
	int j, file_fd, buflen;
	long i, ret, len;
	char * fstr;
	static char buffer[BUFSIZE+1]; /* static so zero filled */

	ret =read(fd,buffer,BUFSIZE); 	/* read Web request in one go */
	if(ret == 0 || ret == -1) {	/* read failure stop now */
		logger(FORBIDDEN,"failed to read browser request","",fd);
	}
	if(ret > 0 && ret < BUFSIZE)	/* return code is valid chars */
		buffer[ret]=0;		/* terminate the buffer */
	else buffer[0]=0;
	for(i=0;i<ret;i++)	/* remove CF and LF characters */
		if(buffer[i] == '\r' || buffer[i] == '\n')
			buffer[i]='*';
	logger(LOG,"request",buffer,hit);
	if( strncmp(buffer,"GET ",4) && strncmp(buffer,"get ",4) ) {
		logger(FORBIDDEN,"Only simple GET operation supported",buffer,fd);
	}
	for(i=4;i<BUFSIZE;i++) { /* null terminate after the second space to ignore extra stuff */
		if(buffer[i] == ' ') { /* string is "GET URL " +lots of other stuff */
			buffer[i] = 0;
			break;
		}
	}
	for(j=0;j<i-1;j++) 	/* check for illegal parent directory use .. */
		if(buffer[j] == '.' && buffer[j+1] == '.') {
			logger(FORBIDDEN,"Parent directory (..) path names not supported",buffer,fd);
		}
	if( !strncmp(&buffer[0],"GET /\0",6) || !strncmp(&buffer[0],"get /\0",6) ) /* convert no filename to index file */
		(void)strcpy(buffer,"GET /index.html");

	/* work out the file type and check we support it */
	buflen=strlen(buffer);
	fstr = (char *)0;
	for(i=0;extensions[i].ext != 0;i++) {
		len = strlen(extensions[i].ext);
		if( !strncmp(&buffer[buflen-len], extensions[i].ext, len)) {
			fstr =extensions[i].filetype;
			break;
		}
	}
	if(fstr == 0) logger(FORBIDDEN,"file extension type not supported",buffer,fd);
    /////////////////////////////
    unsigned * fileSize = (unsigned *) malloc(sizeof(unsigned));
    char* fileBuffer =(char*) malloc(MAX_TEST_SIZE * sizeof(char));
    dkv_get(dkvHandle, &buffer[5], &fileBuffer, fileSize);
    len = *fileSize;
    free(fileSize);
    (void)sprintf(buffer,"HTTP/1.1 200 OK\nServer: nweb/%d.0\nContent-Length: %ld\nConnection: close\nContent-Type: %s\n\n", VERSION, len, fstr); /* Header + a blank line */
	logger(LOG,"Header",buffer,hit);
	(void)write(fd,buffer,strlen(buffer));
    (void)write(fd,fileBuffer,len);
    free(fileBuffer);
    sleep(1);	/* allow socket to drain before signalling the socket is closed */
	close(fd);
	exit(1);
    /////////////////////////////
	/*if(( file_fd = open(&buffer[5],O_RDONLY)) == -1) {  // open the file for reading 
		logger(NOTFOUND, "failed to open file",&buffer[5],fd);
	}
	logger(LOG,"SEND",&buffer[5],hit);
	len = (long)lseek(file_fd, (off_t)0, SEEK_END); // lseek to the file end to find the length 
	      (void)lseek(file_fd, (off_t)0, SEEK_SET); // lseek back to the file start ready for reading 
          (void)sprintf(buffer,"HTTP/1.1 200 OK\nServer: nweb/%d.0\nContent-Length: %ld\nConnection: close\nContent-Type: %s\n\n", VERSION, len, fstr); // Header + a blank line 
	logger(LOG,"Header",buffer,hit);
	(void)write(fd,buffer,strlen(buffer));*/
    //////////////////////////////
    
	/* send file in 8KB block - last block may be smaller */
	/*while (	(ret = read(file_fd, buffer, BUFSIZE)) > 0 ) {
		(void)write(fd,buffer,ret);
	}
	sleep(1);	// allow socket to drain before signalling the socket is closed 
	close(fd);
	exit(1);*/
}

void recursive_fill_kv(char const* dirname, struct dkv_handle *dkv_h) 
{
    struct dirent *curr_ent;
    DIR* dirp = opendir(dirname);
    if (dirp == NULL) {
        printf("dir %s didn't open\n",dirname);
        return;
    }

    while ((curr_ent = readdir(dirp)) != NULL)
    {
        if (!((strcmp(curr_ent->d_name, ".") == 0) ||
            (strcmp(curr_ent->d_name, "..") == 0))) 
            {
                char* path = malloc(strlen(dirname) + strlen(curr_ent->d_name) + 2);
                strcpy(path, dirname);
                strcat(path, "/");
                strcat(path, curr_ent->d_name);
                if (curr_ent->d_type == DT_DIR) 
                {
                    recursive_fill_kv(path, dkv_h);
                } 
                else if (curr_ent->d_type == DT_REG)
                {
                    /////
                    
                    /*int fd = open(path, O_RDONLY);
                    size_t fsize = lseek(fd, (size_t)0, SEEK_END); 
                    void *p = mmap(0, fsize, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_SHARED, fd, 0);
                    dkv_set(dkv_h, path, p, fsize);
                    munmap(p, fsize);
                    close(fd);*/
                    ///////
                    FILE *fp = fopen(path,"r");
                    fseek(fp, 0L, SEEK_END);
                    size_t fsize = ftell(fp);
                    //size_t fsize = lseek(fp, (size_t)0, SEEK_END); 
                    printf("fileSize = %d\n",fsize);
                    char * buffer = (char*) malloc(fsize*sizeof(char));
                    fseek(fp, 0, SEEK_SET);

                    /* Read and display data */
                    fread(buffer, fsize, 1, fp);
                    printf("%s\n", buffer);
                    dkv_set(dkv_h, path, buffer, fsize);
                    free(buffer);
                    fclose(fp); 
                }
                free(path);
            }
    }
    printf("done fill kv for dir %s\n",dirname);
    closedir(dirp);
 }


int main(int argc, char **argv)
{
	int i, port, pid, listenfd, socketfd, hit;
	socklen_t length;
	static struct sockaddr_in cli_addr; /* static = initialised to zeros */
	static struct sockaddr_in serv_addr; /* static = initialised to zeros */
    /////////////////////////////
    struct kv_server_address * server[NUM_SERVERS];// = malloc(NUM_SERVERS *sizeof(struct kv_server_address));
    struct kv_handle * handle[NUM_SERVERS];// = malloc(NUM_SERVERS *sizeof(struct kv_handle));
    for(int sNum = 0; sNum < NUM_SERVERS; sNum ++)
    {
        server[sNum] = malloc(sizeof(struct kv_server_address));
        handle[sNum] = malloc(sizeof(struct kv_handle));
    }
    struct kv_server_address * indexer = malloc(sizeof(struct kv_server_address));
    struct kv_handle * indexerHandle = malloc(sizeof(struct kv_handle));
    struct dkv_handle * dkvHandle = malloc(sizeof(struct dkv_handle));
    dkvHandle->indexer = indexerHandle;
    for(int k = 0 ; k < NUM_SERVERS; k ++)
    {    
        dkvHandle->serverHandles[k] = handle[k];
    }
    /////////////////////////////
    int numArgs = 3 + 2 + NUM_SERVERS*2;//prog name,port, directory, indexer ip port, server ip ports
    char* usageMessage = "usage %s server port,directory, Indexer IP port ,Server IP port, ...\n";
	if (argc > numArgs || argc < numArgs) {
       fprintf(stderr,usageMessage, argv[0]);
       exit(0);
    }
    printf("done getting input\n");
    
    /////////////////////////////
	/*if( argc < 3  || argc > 3 || !strcmp(argv[1], "-?") ) {
		(void)printf("hint: nweb Port-Number Top-Directory\t\tversion %d\n\n"
	"\tnweb is a small and very safe mini web server\n"
	"\tnweb only servers out file/web pages with extensions named below\n"
	"\t and only from the named directory or its sub-directories.\n"
	"\tThere is no fancy features = safe and secure.\n\n"
	"\tExample: nweb 8181 /home/nwebdir &\n\n"
	"\tOnly Supports:", VERSION);
		for(i=0;extensions[i].ext != 0;i++)
			(void)printf(" %s",extensions[i].ext);

		(void)printf("\n\tNot Supported: URLs including \"..\", Java, Javascript, CGI\n"
	"\tNot Supported: directories / /etc /bin /lib /tmp /usr /dev /sbin \n"
	"\tNo warranty given or implied\n\tNigel Griffiths nag@uk.ibm.com\n"  );
		exit(0);
	}*/
	if( !strncmp(argv[2],"/"   ,2 ) || !strncmp(argv[2],"/etc", 5 ) ||
	    !strncmp(argv[2],"/bin",5 ) || !strncmp(argv[2],"/lib", 5 ) ||
	    !strncmp(argv[2],"/tmp",5 ) || !strncmp(argv[2],"/usr", 5 ) ||
	    !strncmp(argv[2],"/dev",5 ) || !strncmp(argv[2],"/sbin",6) ){
		(void)printf("ERROR: Bad top directory %s, see nweb -?\n",argv[2]);
		exit(3);
	}
	if(chdir(argv[2]) == -1){ 
		(void)printf("ERROR: Can't Change to directory %s\n",argv[2]);
		exit(4);
	}
    printf("dir ok\n");
	
	logger(LOG,"nweb starting",argv[1],getpid());
    ///////////////////////////////////
    //setup the connection with storage servers
    indexer->port = (short) atoi(argv[4]);
    indexer->servername = strdupa(argv[3]);
    
    printf("here\n");
    for(int serverNum = 0; serverNum < NUM_SERVERS; serverNum++)
    {
        //printf("in\n");
        printf("portnum = %d\n",atoi(argv[6 + 2*serverNum]));
        server[serverNum]->port = (short) atoi(argv[6 + 2*serverNum]);
        printf("servername = %s\n",argv[5 + 2*serverNum]);
        server[serverNum]->servername = strdupa(argv[5 + 2*serverNum]);
        printf("server ip: %s\nserver port: %d",server[serverNum]->servername,(int) server[serverNum]->port);
        
    }
    
    dkv_open(server, indexer,dkvHandle);
    //add all the files in the directory to our server.
    recursive_fill_kv(argv[2], dkvHandle);
    printf("files uploaded to server\n");
    ///////////////////////////////////
    /* Become deamon + unstopable and no zombies children (= no wait()) */
	if(fork() != 0)
		return 0; /* parent returns OK to shell */
	(void)signal(SIGCLD, SIG_IGN); /* ignore child death */
	(void)signal(SIGHUP, SIG_IGN); /* ignore terminal hangups */
	for(i=0;i<32;i++)
		(void)close(i);		/* close open files */
	(void)setpgrp();		/* break away from process group */
	/* setup the network socket */
	if((listenfd = socket(AF_INET, SOCK_STREAM,0)) <0)
		logger(ERROR, "system call","socket",0);
	port = atoi(argv[1]);
	if(port < 0 || port >60000)
		logger(ERROR,"Invalid port number (try 1->60000)",argv[1],0);
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	serv_addr.sin_port = htons(port);
	if(bind(listenfd, (struct sockaddr *)&serv_addr,sizeof(serv_addr)) <0)
		logger(ERROR,"system call","bind",0);
	if( listen(listenfd,64) <0)
		logger(ERROR,"system call","listen",0);
	for(hit=1; ;hit++) {
		length = sizeof(cli_addr);
		if((socketfd = accept(listenfd, (struct sockaddr *)&cli_addr, &length)) < 0)
			logger(ERROR,"system call","accept",0);
		if((pid = fork()) < 0) {
			logger(ERROR,"system call","fork",0);
		}
		else {
			if(pid == 0) { 	/* child */
				(void)close(listenfd);
				web(socketfd,hit,dkvHandle); /* never returns */
			} else { 	/* parent */
				(void)close(socketfd);
			}
		}
	}
}
