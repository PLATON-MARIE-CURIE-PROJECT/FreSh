
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
#include "inmemory_index_engine.h"
#include "ads/array.h"

#ifndef al_parallel_inmemory_query_engine_h
#define al_parallel_inmemory_query_engine_h

typedef struct localStack {
    isax_node **val; 
    int top;
    int bottom;
}localStack;

typedef struct MESSI_workerdata
{
	isax_node *current_root_node;
	ts_type *paa,*ts;
	pqueue_t *pq;
	isax_index *index;
	float minimum_distance;
	int limit;
	pthread_mutex_t *lock_current_root_node;
	pthread_mutex_t *lock_queue;
	pthread_barrier_t *lock_barrier;
	pthread_rwlock_t *lock_bsf;
	query_result *bsf_result;
	volatile int *node_counter;					// EKOSMAS, AUGUST 29 2020: Added volatile
	isax_node **nodelist;
	int amountnode;
	localStack *localstk; 
	localStack *allstk;
	pthread_mutex_t *locallock,*alllock;
	int *queuelabel,*allqueuelabel;
	pqueue_t **allpq;
	int startqueuenumber;
	pqueue_bsf *pq_bsf;
	int workernumber; // EKOSMAS, AUGUST 29 2020: Added
}MESSI_workerdata;

typedef struct MESSI_workerdata_ekosmas
{
	// isax_node *current_root_node;			// EKOSMAS, 29 AUGUST 2020: REMOVED
	ts_type *paa,*ts;
	// pqueue_t *pq;							// EKOSMAS, 29 AUGUST 2020: REMOVED							
	isax_index *index;
	float minimum_distance;
	// int limit;								// EKOSMAS, 29 AUGUST 2020: REMOVED
	// pthread_mutex_t *lock_current_root_node;	// EKOSMAS, 30 AUGUST 2020: REMOVED
	// pthread_mutex_t *lock_queue;				// EKOSMAS, 29 AUGUST 2020: REMOVED
	pthread_barrier_t *lock_barrier;
	pthread_mutex_t *lock_bsf;
	query_result *bsf_result;
	volatile int *node_counter;					// EKOSMAS, AUGUST 29 2020: Added volatile
	isax_node **nodelist;
	int amountnode;
	// localStack *localstk; 					// EKOSMAS, 29 AUGUST 2020: REMOVED
	// localStack *allstk;						// EKOSMAS, 29 AUGUST 2020: REMOVED
	// pthread_mutex_t *locallock;				// EKOSMAS, 29 AUGUST 2020: REMOVED
	pthread_mutex_t *alllock;
	// int *queuelabel;							// EKOSMAS, 29 AUGUST 2020: REMOVED
	int *allqueuelabel;
	pqueue_t **allpq;
	int startqueuenumber;
	// pqueue_bsf *pq_bsf;						// EKOSMAS, 29 AUGUST 2020: REMOVED
	int workernumber; 							// EKOSMAS, AUGUST 29 2020: Added
}MESSI_workerdata_ekosmas;

typedef struct MESSI_workerdata_ekosmas_EP
{
	ts_type *paa,*ts;
	pqueue_t *pq;							
	isax_index *index;
	float minimum_distance;
	pthread_mutex_t *lock_bsf;
	query_result *bsf_result;
	volatile int *node_counter;					
	isax_node **nodelist;
	int amountnode;
	int workernumber; 							
}MESSI_workerdata_ekosmas_EP;

typedef struct MESSI_workerdata_ekosmas_lf
{
	ts_type *paa,*ts;
	pqueue_t **allpq;
	query_result ***allpq_data;	
	array_list_t *array_lists;
	sorted_array_t **sorted_arrays;
	volatile unsigned char *queue_finished;
	volatile unsigned char *helper_queue_exist;		
	volatile int *fai_queue_counters;
	volatile float **queue_bsf_distance;
	isax_index *index;
	float minimum_distance;
	query_result * volatile *bsf_result_p;
	volatile int *node_counter;
	volatile unsigned long *sorted_array_counter;					
	volatile unsigned long *sorted_array_FAI_counter;					
	isax_node **nodelist;
	int amountnode;
	int workernumber; 		
	char parallelism_in_subtree;
	volatile unsigned long *next_queue_data_pos;
	unsigned long query_id;
	pthread_barrier_t *wait_tree_pruning_phase_to_finish;
	volatile unsigned long *subtree_fai;
	volatile unsigned long *subtree;
	volatile unsigned long *subtree_prune_helpers_exist;
}MESSI_workerdata_ekosmas_lf;

float calculate_node_distance_inmemory_m (isax_index *index, isax_node *node, ts_type *query, float bsf);

query_result  approximate_search_inmemory_m (ts_type *ts, ts_type *paa, isax_index *index);
query_result refine_answer_inmemory_m (ts_type *ts, ts_type *paa, isax_index *index, query_result approximate_bsf_result,float minimum_distance, int limit);




query_result exact_search_ParISnew_inmemory_hybrid (ts_type *ts, ts_type *paa, isax_index *index,node_list *nodelist,
                           float minimum_distance, int min_checked_leaves);
void* exact_search_worker_inmemory_hybridpqueue(void *rfdata);
void insert_tree_node_m_hybridpqueue(float *paa,isax_node *node,isax_index *index,float bsf,pqueue_t **pq,pthread_mutex_t *lock_queue,int *tnumber);



query_result exact_search_ParISnew_inmemory_hybrid_ekosmas (ts_type *ts, ts_type *paa, isax_index *index,node_list *nodelist,
                           float minimum_distance);
void* exact_search_worker_inmemory_hybridpqueue_ekosmas(void *rfdata);
void insert_tree_node_m_hybridpqueue_ekosmas (float *paa,isax_node *node,isax_index *index,float bsf,pqueue_t **pq,pthread_mutex_t *lock_queue,int *tnumber);


query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_EP(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance);
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_EP(void *rfdata);
void insert_tree_node_m_hybridpqueue_ekosmas_EP(float *paa, isax_node *node, isax_index *index, float bsf, pqueue_t *my_pq);


query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_lf(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance, const char parallelism_in_subtree, const int third_phase, const int query_id);

void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_not_mixed_only_after_helper(void *rfdata);
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_only_after_helper(void *rfdata);
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_full_helping(void *rfdata);
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_no_helping(void *rfdata);
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_queue_full_help(void *rfdata);
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_sorted_array(void *rfdata);
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_only_after_helper(void *rfdata);
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_full_helping(void *rfdata);


void add_to_queue_data_lf(float *paa, isax_node *node, isax_index *index, float bsf, query_result ***pq_data, int *tnumber, volatile unsigned long *next_queue_data_pos, const int query_id);
void add_to_array_data_lf(float *paa, isax_node *node, isax_index *index, float bsf, array_list_t *array_lists, int *tnumber, volatile unsigned long *next_queue_data_pos, const int query_id);
void add_to_array_data_lf_opt(float *paa, isax_node *node, isax_index *index, float bsf, array_list_t *array_lists, int *tnumber, volatile unsigned long *next_queue_data_pos, const unsigned long query_id, volatile unsigned long *node_index, volatile unsigned long *node_index_fai, unsigned long *cur_index, unsigned long *next_index, volatile unsigned long *subtree_prune_helpers_exist, char *prunning_helpers_exist, char is_scanning, volatile unsigned long *stop, char fai_only_after_help);

float * rawfile;
void pushbottom(localStack *stk, isax_node *node);
isax_node* poptop(localStack *stk);
isax_node* popbottom(localStack *stk);
bool isemptyqueue(localStack *stk);
isax_node* poptop2(localStack *stk);
isax_node* popbottom2(localStack *stk);

#endif
