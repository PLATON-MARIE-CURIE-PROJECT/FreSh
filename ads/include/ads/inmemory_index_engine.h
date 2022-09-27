//
//  Updated by Eleftherios Kosmas on May 2020.
//

#ifndef al_inmemory_index_engine_h
#define al_inmemory_index_engine_h



#include "../../config.h"
#include "../../globals.h"
#include "sax/ts.h"
#include "sax/sax.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "isax_index.h"
#include "isax_query_engine.h"
#include "parallel_query_engine.h"
#include "isax_node.h"
#include "pqueue.h"
#include "isax_first_buffer_layer.h"
#include "ads/isax_node_split.h"

void index_creation_pRecBuf_new(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_MESSI_with_enhanced_blocking_parallelism(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_EP(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_blocking_parallelism_in_subtree(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_cow(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_blocking_parallelism_in_subtree(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(const char *ifilename, long int ts_num, isax_index *index);
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_cow(const char *ifilename, long int ts_num, isax_index *index);
void index_initialization_rawBuffer_geopat (const char *ifilename, long int ts_num, isax_index *index , int subtree_parallelism);
void* index_creation_pRecBuf_worker_new(void *transferdata);
void* index_creation_pRecBuf_worker_new_ekosmas(void *transferdata);
void* index_creation_pRecBuf_worker_new_ekosmas_EP(void *transferdata);
void* index_creation_pRecBuf_worker_new_ekosmas_lock_free_full_fai(void *transferdata);
void* index_creation_pRecBuf_worker_new_ekosmas_lock_free_fai_only_after_help(void *transferdata);
root_mask_type isax_pRecBuf_index_insert_inmemory(isax_index *index,
                                    sax_type * sax,
                                    file_position_type * pos,pthread_mutex_t *lock_firstnode,int workernumber,int total_workernumber);
root_mask_type isax_pRecBuf_index_insert_inmemory_ekosmas(isax_index *index,
                                    sax_type * sax,
                                    file_position_type * pos,pthread_mutex_t *lock_firstnode,int workernumber,int total_workernumber);

enum response flush_subtree_leaf_buffers_inmemory (isax_index *index, isax_node *node);

typedef struct					// MESSI receive buffer for process worker number
{
	isax_index *index;
	int start_number,stop_number;
	ts_type * ts;
	pthread_mutex_t *lock_record;
	pthread_mutex_t *lock_fbl;
	pthread_mutex_t *lock_index;
	pthread_mutex_t *lock_cbl;
	pthread_mutex_t *lock_firstnode;							// 
	pthread_mutex_t *lock_nodeconter;
	pthread_mutex_t *lock_disk;
	int workernumber;											// worker processing this buffer
	int total_workernumber;										// 
	pthread_barrier_t *lock_barrier1, *lock_barrier2;				
	int *node_counter;
	// bool finished;
	int *nodeid;
	unsigned long *shared_start_number;
    int myid;
} buffer_data_inmemory;


typedef struct
{
	isax_index *index;
	int ts_num;
	int workernumber;
	pthread_mutex_t *lock_firstnode;
	pthread_barrier_t *wait_summaries_to_compute;				
	int *node_counter;
	// bool finished;
	unsigned long *shared_start_number;
	char parallelism_in_subtree;
	volatile unsigned long *next_iSAX_group;
} buffer_data_inmemory_ekosmas;

typedef struct
{
	isax_index *index;
	int ts_num;
	int workernumber;
	pthread_barrier_t *wait_summaries_to_compute;				
	int *node_counter;
	// bool finished;
	unsigned long *shared_start_number;
	char parallelism_in_subtree;
    sax_type *sax;
} buffer_data_inmemory_ekosmas_lf;

typedef struct transferfblinmemory
{
	int start_number,stop_number,conternumber;
  	int preworkernumber;
  	isax_index *index;
  	int *nodeid;
}transferfblinmemory;

float *rawfile;

// ----------------------------------------------
// ekosmas:
// ----------------------------------------------
volatile unsigned char *block_processed;
volatile unsigned char **group_processed;
volatile unsigned char *ts_processed;

typedef struct next_ts_grp {
	volatile unsigned long num CACHE_ALIGN;
	char pad[PAD_CACHE(sizeof(unsigned long))];
} next_ts_group;

typedef struct next_ts_in_grp {
	volatile unsigned long num CACHE_ALIGN;
	char pad[PAD_CACHE(sizeof(unsigned long))];
} next_ts_in_group;

next_ts_group *next_ts_group_read_in_block;
next_ts_in_group **next_ts_read_in_group;
next_ts_in_group **next_ts_read_in_group_fai;
volatile unsigned char all_blocks_processed;
volatile unsigned char all_RecBufs_processed;
volatile unsigned char *block_helper_exist;
volatile unsigned char **group_helpers_exist;
// volatile unsigned char *block_helpers_num;
volatile unsigned char *recBuf_helpers_num;

static __thread double my_time_for_blocks_processed = 0;
static __thread unsigned long my_num_blocks_processed = 0;
static __thread double my_time_for_subtree_construction = 0;
static __thread unsigned long my_num_subtree_construction = 0;
static __thread unsigned long my_num_subtree_nodes = 0;
static __thread struct timeval my_time_start_val;
static __thread struct timeval my_current_time_val;
static __thread double my_tS;
static __thread double my_tE;

#define COUNT_MY_TIME_START            gettimeofday(&my_time_start_val, NULL);
#define COUNT_MY_TIME_FOR_BLOCKS_END   gettimeofday(&my_current_time_val, NULL); \
                                            my_tS = my_time_start_val.tv_sec*1000000 + (my_time_start_val.tv_usec); \
                                            my_tE = my_current_time_val.tv_sec*1000000 + (my_current_time_val.tv_usec); \
                                            my_time_for_blocks_processed += (my_tE - my_tS);
#define COUNT_MY_TIME_FOR_SUBTREE_END  gettimeofday(&my_current_time_val, NULL); \
                                            my_tS = my_time_start_val.tv_sec*1000000 + (my_time_start_val.tv_usec); \
                                            my_tE = my_current_time_val.tv_sec*1000000 + (my_current_time_val.tv_usec); \
                                            my_time_for_subtree_construction += (my_tE - my_tS);    
#define BACKOFF_BLOCK_DELAY_VALUE	   (my_time_for_blocks_processed/my_num_blocks_processed)
#define BACKOFF_SUBTREE_DELAY_PER_NODE (my_time_for_subtree_construction/my_num_subtree_nodes)

static __thread unsigned long blocks_helped_cnt = 0;
static __thread unsigned long blocks_helping_avoided_cnt = 0;
static __thread unsigned long recBufs_helped_cnt = 0;
static __thread unsigned long recBufs_helping_avoided_cnt = 0;
static __thread unsigned long unique_leaves_in_arrays_cnt = 0;
static __thread unsigned long leaves_in_arrays_cnt = 0;


// ----------------------------------------------


typedef struct node_list
{
	isax_node **nlist;
	int node_amount;
} node_list;

typedef struct node_list_lf
{
	parallel_fbl_soft_buffer_ekosmas_lf **nlist;
	int node_amount;
} node_list_lf;

#endif
