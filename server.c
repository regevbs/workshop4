#define _GNU_SOURCE
#include <infiniband/verbs.h>
#include <linux/types.h>
//#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <malloc.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <time.h>
#include <inttypes.h>

#ifdef EX4
#include <fcntl.h>
#include <dirent.h>
#include <sys/mman.h>
#include <sys/stat.h>
#endif

#define EAGER_PROTOCOL_LIMIT (1 << 12) /* 4KB limit */
#define MAX_TEST_SIZE (10 * EAGER_PROTOCOL_LIMIT)
#define TEST_LOCATION "~/www/"
#define MAX_SERVER_ENTRIES 1000
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
					
#ifdef EX4
        struct {
            unsigned num_of_servers;
            char key[0];
        } find;

        struct {
            unsigned selected_server;
        } location;
#endif
    };
};

struct kv_server_address {
    char *servername; /* In the last item of an array this is NULL */
    short port; /* This is useful for multiple servers on a host */
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

struct kv_handle
{
    struct pingpong_context * ctx;//context
    int entryLen;
    int keyLen[MAX_SERVER_ENTRIES];
    int valueLen[MAX_SERVER_ENTRIES];
    struct ibv_mr * registeredMR[MAX_SERVER_ENTRIES];
    int numRegistered;
    uint32_t rkeyValue[MAX_SERVER_ENTRIES];
    uint64_t remote_addresses[MAX_SERVER_ENTRIES];
    char * keys[MAX_SERVER_ENTRIES];
    char * values[MAX_SERVER_ENTRIES];
};





////////////////////////////////////
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
	attr.timeout	    = 14;
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

static struct pingpong_dest *pp_server_exch_dest(struct pingpong_context *ctx,
						 int ib_port, enum ibv_mtu mtu,
						 int port, int sl,
						 const struct pingpong_dest *my_dest,
						 int sgid_idx)
{
	struct addrinfo *res, *t;
	struct addrinfo hints = {
		.ai_flags    = AI_PASSIVE,
		.ai_family   = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM
	};
	char *service;
	char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
	int n;
	int sockfd = -1, connfd;
	struct pingpong_dest *rem_dest = NULL;
	char gid[33];

	if (asprintf(&service, "%d", port) < 0)
		return NULL;

	n = getaddrinfo(NULL, service, &hints, &res);

	if (n < 0) {
		fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
		free(service);
		return NULL;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			n = 1;

			setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

			if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(sockfd);
			sockfd = -1;
		}
	}

	freeaddrinfo(res);
	free(service);

	if (sockfd < 0) {
		fprintf(stderr, "Couldn't listen to port %d\n", port);
		return NULL;
	}
	listen(sockfd, 1);
	connfd = accept(sockfd, NULL, NULL);
	close(sockfd);
	if (connfd < 0) {
		fprintf(stderr, "accept() failed\n");
		return NULL;
	}
    rem_dest = malloc((sizeof *rem_dest));
	if (!rem_dest)
		goto out;
  
    n = read(connfd, msg, sizeof msg);
    if (n != sizeof msg) {
        perror("server read");
        fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
        goto out;
    }



    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
                            &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);


    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn,
                            my_dest->psn, gid);
    if (write(connfd, msg, sizeof msg) != sizeof msg ||
        read(connfd, msg, sizeof msg) != sizeof "done") {
        fprintf(stderr, "Couldn't send/recv local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }
    

out:
	close(connfd);
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
		//ibv_free_dm(ctx->dm);

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
		return 1;
	}

	if (ctx->dm) {
		
	}
    
	if (ibv_dealloc_pd(ctx->pd)) {
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

static int pp_post_send(struct pingpong_context *ctx, enum ibv_wr_opcode opcode, unsigned size, const char *local_ptr, void *remote_ptr, uint32_t remote_key)
{
	struct ibv_sge list = {
		.addr	= (uintptr_t) (local_ptr ? local_ptr : ctx->buf),
		.length = size,
		.lkey	= ctx->mr->lkey
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
    return ibv_post_send((*ctx).qp, &wr, &bad_wr);
	//return ibv_post_send(ctx->qp, &wr, &bad_wr);
}
////////////////
void kv_release(char *value)
{
    /* TODO (2LOC): free value */
}
int handle_server_packets_only(struct kv_handle *handle, struct packet *packet)
{
	unsigned response_size = 0;
    struct pingpong_context *ctx = handle->ctx;
    struct packet *response_packet = (struct packet*)ctx->buf;
	bool indexFound = false;
    int i=0;
    //printf("server got packet\ntype = %d\n",packet->type);
    switch (packet->type) {
	/* Only handle packets relevant to the server here - client will handle inside get/set() calls */
    case EAGER_GET_REQUEST:/* TODO (10LOC): handle a short GET() on the server */
    //find the index of the value, get the value and send it back in a packet
        indexFound = false;
        //int i;
        //printf("key recieved: %s\n",packet->eager_get_request.key);
        for ( i = 0; i < handle->entryLen; i = i +1 )
        {
            //printf("comparing: %s with %s\n",(handle->keys)[i], packet->eager_get_request.key);
            if(strcmp((handle->keys)[i],packet->eager_get_request.key) == 0) //means we found the key
            {
                indexFound = true;
                break;
            }
        }
        //memset the buffer to 0
        memset((handle->ctx)->buf,0,(handle->ctx)->size); //TODO make sure buffer is not needed anymore
        if(indexFound)
        {
            //printf("index found! sending reply\n");
            response_packet->type = EAGER_GET_RESPONSE;
            //response_size = sizeof(struct packet) + strlen((handle->values)[i])  + 1;
            response_size = sizeof(struct packet) + handle->valueLen[i] + 1;
            if(response_size <= EAGER_PROTOCOL_LIMIT)
            {
                response_packet->eager_get_response.valueLen = strlen((handle->values)[i])  + 1;
                //memcpy the found data into the buffer
                memcpy(response_packet->eager_get_response.value,(handle->values)[i],strlen((handle->values)[i])  + 1);
            }
            else //need to respond with a rndv_get_response
            {
                //printf("oversize get, value is of size %d\n",handle->valueLen[i]);
                //sleep(3);
                //printf("value is: %s\n",handle->values[i]);
                response_packet->type = RENDEZVOUS_GET_RESPONSE;
                response_size = sizeof(struct packet);
                if(handle->remote_addresses[i] == 0 && handle->rkeyValue[i] == 0)
                {
                    //TODO register memory of this entry
                    handle->registeredMR[i] = ibv_reg_mr(ctx->pd, handle->values[i],
                                                        handle->valueLen[i], IBV_ACCESS_LOCAL_WRITE |
                                                        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ); 
                    handle->numRegistered = handle->numRegistered + 1;
                    handle->remote_addresses[i] = (uint64_t) handle->registeredMR[i]->addr;
                    handle->rkeyValue[i] = (uint32_t) handle->registeredMR[i]->rkey;
                }
                response_packet->rndv_get_response.remote_address = handle->remote_addresses[i];
                response_packet->rndv_get_response.rkey = handle->rkeyValue[i];
                response_packet->rndv_get_response.valueLen = strlen(handle->values[i]) + 1;
            }
            //memcpy((handle->ctx)->buf,(handle->values)[i],(handle->valueLen)[i]);
            
        }
        else
        {
            //printf("index not found T_T\n");
            char toSend[] = "";
            response_packet->type = EAGER_GET_RESPONSE;
            response_size = sizeof(struct packet) + strlen(toSend)  + 1;
            response_packet->eager_get_response.valueLen = strlen(toSend)  + 1;
            //memcpy the found data into the buffer
            memcpy(response_packet->eager_get_response.value,toSend,strlen(toSend)  + 1);
            
            //memcpy((handle->ctx)->buf,toSend,sizeof(char));// TODO make sure this is what we send
            //response_size = sizeof(char);
        }
        break;
    case EAGER_SET_REQUEST: /* TODO (10LOC): handle a short SET() on the server */
    //check if the key exists, if it does replace value, if it doesn't make a new entry
        indexFound = false;
        //int i;
        //printf("set string: %s\n",packet->eager_set_request.key_and_value);
        for ( i = 0; i < handle->entryLen; i = i +1 )
        {
            if(strcmp((handle->keys)[i],packet->eager_set_request.key_and_value) == 0) //means we found the key
            {
                indexFound = true;
                break;
            }
        }
        if(indexFound)
        {
            kv_release((handle->values)[i]);//TODO check if release is the func we want
            (handle->valueLen)[i] = packet->eager_set_request.valueLen;
            (handle->values)[i] = (char*) malloc((handle->valueLen)[i]);
            //memcpy the found data into the buffer
            memcpy((handle->values)[i],&(packet->eager_set_request.key_and_value[packet->eager_set_request.keyLen]),packet->eager_set_request.valueLen);
        }
        else
        {
            //printf("no index found\n");
            (handle->keyLen)[i] = packet->eager_set_request.keyLen;
            (handle->keys)[i] = (char*) malloc((handle->keyLen)[i]);
            (handle->valueLen)[i] = packet->eager_set_request.valueLen;
            (handle->values)[i] = (char*) malloc((handle->valueLen)[i]);
            handle->entryLen = handle->entryLen + 1;
            memcpy((handle->values)[i],&(packet->eager_set_request.key_and_value[packet->eager_set_request.keyLen]),packet->eager_set_request.valueLen);
            memcpy((handle->keys)[i],packet->eager_set_request.key_and_value,packet->eager_set_request.keyLen);
            //printf("key inserted = %s\nvalue inserted = %s\n",(handle->keys)[i],(handle->values)[i]);
        }
        response_packet->type = EAGER_SET_RESPONSE;
        response_size = sizeof(struct packet);
        break;
    case RENDEZVOUS_GET_REQUEST:/* TODO (10LOC): handle a long GET() on the server */
         //find the index of the value, get the value and send it back in a packet
        indexFound = false;
        //int i;
        //printf("key recieved: %s\n",packet->rndv_get_request.key);
        for ( i = 0; i < handle->entryLen; i = i +1 )
        {
           // printf("comparing: %s with %s\n",(handle->keys)[i], packet->rndv_get_request.key);
            if(strcmp((handle->keys)[i],packet->rndv_get_request.key) == 0) //means we found the key
            {
                indexFound = true;
                break;
            }
        }
        if(indexFound)
        {
           // printf("index found! sending reply\n");
            response_packet->type = RENDEZVOUS_GET_RESPONSE;
            response_size = sizeof(struct packet);
            if(handle->remote_addresses[i] == 0 && handle->rkeyValue[i] == 0)
            {
                //TODO register memory of this entry
                handle->registeredMR[i] = ibv_reg_mr(ctx->pd, handle->values[i],
                                                    handle->valueLen[i], IBV_ACCESS_LOCAL_WRITE |
                                                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ); 
                handle->remote_addresses[i] = (uint64_t) handle->registeredMR[i]->addr;
                handle->rkeyValue[i] = (uint32_t) handle->registeredMR[i]->rkey;
                handle->numRegistered = handle->numRegistered + 1;
            }
            response_packet->rndv_get_response.remote_address = handle->remote_addresses[i];
            response_packet->rndv_get_response.rkey = handle->rkeyValue[i];
            response_packet->rndv_get_response.valueLen = strlen(handle->values[i]) + 1;
            //all the info is sent.
        }
        else
        {
            //printf("index not found T_T\n");
            response_packet->type = RENDEZVOUS_GET_RESPONSE;
            response_size = sizeof(struct packet);
            response_packet->rndv_get_response.remote_address = 0;
            response_packet->rndv_get_response.rkey = 0;
            response_packet->rndv_get_response.valueLen = 0;
        }
        
        break;
    case RENDEZVOUS_SET_REQUEST: /* TODO (20LOC): handle a long SET() on the server */
        indexFound = false;
        response_packet->type = RENDEZVOUS_SET_RESPONSE;
        response_size = sizeof(struct packet);
        //int i;
        //printf("REDN SET REQUEST$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n");
        //printf("set string: %s\n",packet->rndv_set_request.key);
        for ( i = 0; i < handle->entryLen; i = i +1 )
        {
            if(strcmp((handle->keys)[i],packet->rndv_set_request.key) == 0) //means we found the key
            {
                indexFound = true;
                break;
            }
        }
        if(indexFound)
        {
           // printf("index found at rdv\n");
            kv_release((handle->values)[i]);//TODO check if release is the func we want
            (handle->valueLen)[i] = packet->rndv_set_request.valueLen;
            //TODO dereg the older MR that was here and zero all its attributes
            if(handle->remote_addresses[i] != 0 && handle->rkeyValue[i] != 0)
            {
            //    printf("deregging\n");
                ibv_dereg_mr(handle->registeredMR[i]);
                handle->remote_addresses[i] = 0;
                handle->rkeyValue[i] = 0;
             //   printf("dereg done\n");
                
            }
            free(handle->values[i]);
           // printf("free done\n");
            //TODO reg a new MR here.
           // printf("server regs memory of size: %d\n@@@@@@@@@@@$$$$$$$$$$$%%%%%%%%\n",(handle->valueLen)[i]);
            (handle->values)[i] = (char*) malloc((handle->valueLen)[i]); //this is the address for the new MR.
            handle->registeredMR[i] = ibv_reg_mr(ctx->pd, handle->values[i],
                                                    handle->valueLen[i], IBV_ACCESS_LOCAL_WRITE |
                                                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ); 
            handle->remote_addresses[i] = (uint64_t) handle->registeredMR[i]->addr;
            handle->rkeyValue[i] = (uint32_t) handle->registeredMR[i]->rkey;
           // printf("server: r_add = %d\nr_key = %d\n",handle->remote_addresses[i],handle->rkeyValue[i]);
            //TODO create the packet to return for the client to write into this MR.
            //printf("mem re - registered\n");
            response_packet->rndv_get_response.remote_address = handle->remote_addresses[i];
            response_packet->rndv_get_response.rkey = handle->rkeyValue[i];
           
        }
        else
        {
            //printf("no index found\n");
            (handle->keyLen)[i] = packet->rndv_set_request.keyLen;
            (handle->keys)[i] = (char*) malloc((handle->keyLen)[i]);
            (handle->valueLen)[i] = packet->rndv_set_request.valueLen;
            handle->entryLen = handle->entryLen + 1;
            memcpy((handle->keys)[i],packet->eager_set_request.key_and_value,packet->eager_set_request.keyLen);
            //TODO reg a new MR here.
            (handle->values)[i] = (char*) malloc((handle->valueLen)[i]);
            handle->registeredMR[i] = ibv_reg_mr(ctx->pd, handle->values[i],
                                                    handle->valueLen[i], IBV_ACCESS_LOCAL_WRITE |
                                                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ); 
            handle->remote_addresses[i] = (uint64_t) handle->registeredMR[i]->addr;
            handle->rkeyValue[i] = (uint32_t) handle->registeredMR[i]->rkey;
            handle->numRegistered = handle->numRegistered + 1;
            //TODO create the packet to return for the client to write into this MR.
           // printf("server: r_add = %d\nr_key = %d\n",handle->remote_addresses[i],handle->rkeyValue[i]);

            response_packet->rndv_get_response.remote_address = handle->remote_addresses[i];
            response_packet->rndv_get_response.rkey = handle->rkeyValue[i];

        }
       // printf("responding REDN_SET_RESPONSE%%%%%%%%%%%%%%%%%%%%%%%%%\n");
        break;
#ifdef EX4
    case FIND: /* TODO (2LOC): use some hash function */
#endif
    case TERMINATE:
        return -10;
        break;
    default:
        break;
    }
	
	if (response_size) {
		pp_post_send(handle->ctx, IBV_WR_SEND, response_size, NULL, NULL, 0);
        //sleep(3);
        //printf("server: buffer has value: %s\n",handle->values[i]);
    }
    
}

//////////////////////////////////////////////////////////
int pp_wait_completions(struct kv_handle *handle, int iters)
{
    struct pingpong_context* ctx = handle->ctx;
    int rcnt, scnt, num_cq_events, use_event = 0;
	rcnt = scnt = 0;
    //printf("pass 1\n");
	while (rcnt + scnt < iters) {
		struct ibv_wc wc[2];
		int ne, i;
        //printf("pass 2\n");
		do {
			ne = ibv_poll_cq(pp_cq(ctx), 2, wc);
            //printf("server:%d\n",ne);
			if (ne < 0) {
				fprintf(stderr, "poll CQ failed %d\n", ne);
				return 1;
			}

		} while (ne < 1);
        //printf("pass 3\n");
		for (i = 0; i < ne; ++i) {
            //printf("server is onto sumthin\n");
			if (wc[i].status != IBV_WC_SUCCESS) {
				fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
					ibv_wc_status_str(wc[i].status),
					wc[i].status, (int) wc[i].wr_id);
				return 1;
			}

			switch ((int) wc[i].wr_id) {
			case PINGPONG_SEND_WRID:
               // printf("server got send completion\n");
                scnt = scnt + 1;
				break;

			case PINGPONG_RECV_WRID:;
                int retVal;
				retVal = handle_server_packets_only(handle, (struct packet*)ctx->buf);
                if(retVal == -10)
                {
                    return -10;
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
	return 0;
}


//{



int main(int argc, char *argv[])
{
    
    struct ibv_device      **dev_list;
	struct ibv_device	*ib_dev;
	struct pingpong_context *context; //= (struct pingpong_context*) malloc(sizeof(struct pingpong_context));
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

	srand48(getpid() * time(NULL));
    
    
    //get input for the server ip and port
    //int portNum;
    int numArgs = 2;
    char* usageMessage = "usage %s port\n";
	if (argc < numArgs) {
       fprintf(stderr,usageMessage, argv[0]);
       exit(0);
    }
    portNum = atoi(argv[numArgs - 1]);
    port = portNum;
    servername = strdupa(argv[1]);
    
    //get our beloved device
    dev_list = ibv_get_device_list(NULL); //get devices available to this machine
	if (!dev_list) {
		perror("Failed to get IB devices list");
		return 1;
	}
    ib_dev = *dev_list; //chooses the first device by default
    if (!ib_dev) {
        fprintf(stderr, "No IB devices found\n");
        return 1;
    }
    //Create the context for this connection
    //creates context on found device, registers memory of size.
    context = pp_init_ctx(ib_dev, EAGER_PROTOCOL_LIMIT, rx_depth, ib_port, use_event); //use_event (decides if we wait blocking for completion)
    if (!context)
        return 1;
    
    if (pp_get_port_info(context->context, ib_port, &context->portinfo)) { //gets the port status and info (uses ibv_query_port)
            fprintf(stderr, "Couldn't get port info\n");
            return 1;
        }
    
        //Prepare to recieve messages. fill the recieve request queue of QP k
        routs = pp_post_recv(context, context->rx_depth); //post rx_depth recieve requests
            if (routs < context->rx_depth) {
                fprintf(stderr, "Couldn't post receive (%d)\n", routs);
                return 1;
            }
        //set my_dest for every QP, getting ready to connect them.
        my_dest.lid = context->portinfo.lid; //assigns lid to my dest
        if (context->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET &&
                                !my_dest.lid) {
            fprintf(stderr, "Couldn't get local LID\n");
            return 1;
        }
        //set the gid to 0, we are in the same subnet.
        memset(&my_dest.gid, 0, sizeof my_dest.gid); //zero the gid, we send in the same subnet
        my_dest.qpn = ((*context).qp)->qp_num; //gets the qp number
        my_dest.psn = lrand48() & 0xffffff; //randomizes the packet serial number
        inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid); //changes gid to text form
        //printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
         //      my_dest[k].lid, my_dest[k].qpn, my_dest[k].psn, gid);
    
    //Get the remote dest for my QPs
    
    rem_dest = pp_server_exch_dest(context, ib_port, mtu, port, sl, //if youre a server - exchange data with client
                                    &my_dest, gidx);
    
    if (!rem_dest)
            return 1; 
    

    
      inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
      //printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
            // rem_dest[k].lid, rem_dest[k].qpn, rem_dest[k].psn, gid);
         
    //now connect all the QPs to the client
    
        if (pp_connect_ctx(context, ib_port, my_dest.psn, mtu, sl, rem_dest,
                        gidx))
                return 1; //connect to the server

    
    //////////////////////////////////////////////
    //do server work
    /////////////////////
    struct kv_handle * server_handle = malloc(sizeof(struct kv_handle));
   
    //struct pingpong_context *ctx = malloc(sizeof(struct pingpong_context));
    server_handle->ctx = context;
    server_handle->entryLen = 0;
    
    
    /////////////////////
    //printf("server waiting for completions to respond\n");
    while (0 <= pp_wait_completions(server_handle, 1));//TODO will this ever exit?
    //clean after us
    for(int i = 0; i < server_handle->numRegistered; i = i + 1)
    {
        //void * memory = server_handle->registeredMR[i]->addr;
        ibv_dereg_mr(server_handle->registeredMR[i]);
        //free(memory);
    }
    for(int i = 0; i< server_handle->entryLen; i = i + 1)
    {
        free(server_handle->keys[i]);
        free(server_handle->values[i]);
    }
    
    pp_close_ctx(server_handle->ctx);

    //////////////////////////////////////////////
    ibv_free_device_list(dev_list);
    free(rem_dest);
    free(server_handle);

}
    
