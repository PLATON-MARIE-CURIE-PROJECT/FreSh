
#define _GNU_SOURCE


#ifdef VALUES
#include <values.h>
#endif
#include <float.h>
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include <stdbool.h>
#include <sys/wait.h>
#include <sched.h>
#include <malloc.h>


#include "omp.h"  
#include "ads/isax_query_engine.h"
#include "ads/inmemory_index_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_inmemory_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/pqueue.h"
#include "ads/array.h"
#include "ads/sax/sax.h"
#include "ads/isax_node_split.h"
#include "ads/inmemory_topk_engine.h"

#define left(i)   ((i) << 1)
#define right(i)  (((i) << 1) + 1)
#define parent(i) ((i) >> 1)

#define ULONG_CACHE_PADDING (CACHE_LINE_SIZE/sizeof(unsigned long))

int NUM_PRIORITY_QUEUES;

inline void threadPin(int pid, int max_threads) {
    int cpu_id;

    cpu_id = pid % max_threads;
    pthread_setconcurrency(max_threads);

    cpu_set_t mask;  
    unsigned int len = sizeof(mask);

    CPU_ZERO(&mask);

    // CPU_SET(cpu_id % max_threads, &mask);                              // OLD PINNING 1

    // if (cpu_id % 2 == 0)                                             // OLD PINNING 2
    //    CPU_SET(cpu_id % max_threads, &mask);
    // else 
    //    CPU_SET((cpu_id + max_threads/2)% max_threads, &mask);

    // if (cpu_id % 2 == 0)                                             // FULL HT
    //    CPU_SET(cpu_id/2, &mask);
    // else 
    //    CPU_SET((cpu_id/2) + (max_threads/2), &mask);

    CPU_SET((cpu_id%4)*10 + (cpu_id%40)/4 + (cpu_id/40)*40, &mask);     // SOCKETS PINNING - Vader

    int ret = sched_setaffinity(0, len, &mask);
    if (ret == -1)
        perror("sched_setaffinity");
}


// -------------------------------------
// -------------------------------------

// Botao's version
query_result exact_search_ParISnew_inmemory_hybrid (ts_type *ts, ts_type *paa, isax_index *index,node_list *nodelist,
                           float minimum_distance, int min_checked_leaves) 
{   
    query_result approximate_result = approximate_search_inmemory_pRecBuf(ts, paa, index);
    query_result bsf_result = approximate_result;
    int tight_bound = index->settings->tight_bound;
    int aggressive_check = index->settings->aggressive_check;
    int node_counter=0;
    // Early termination...
    if (approximate_result.distance == 0) {
        return approximate_result;
    }

    if (maxquerythread == 1) {
        NUM_PRIORITY_QUEUES = 1;
    }
    else {
        NUM_PRIORITY_QUEUES = maxquerythread/2;
    }

    pqueue_t **allpq=malloc(sizeof(pqueue_t*)*NUM_PRIORITY_QUEUES);


    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    int queuelabel[NUM_PRIORITY_QUEUES];

    query_result *do_not_remove = &approximate_result;

    SET_APPROXIMATE(approximate_result.distance);


    if(approximate_result.node != NULL) {
        // Insert approximate result in heap.
        //pqueue_insert(pq, &approximate_result);
        //GOOD: if(approximate_result.node->filename != NULL)
        //GOOD: printf("POPS: %.5lf\t", approximate_result.distance);
    }
    // Insert all root nodes in heap.
    isax_node *current_root_node = index->first_node;

    pthread_t threadid[maxquerythread];
    MESSI_workerdata workerdata[maxquerythread];
    pthread_mutex_t lock_queue=PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t lock_current_root_node=PTHREAD_MUTEX_INITIALIZER;
    pthread_rwlock_t lock_bsf=PTHREAD_RWLOCK_INITIALIZER;
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);
 
    
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
                allpq[i]=pqueue_init(index->settings->root_nodes_size/NUM_PRIORITY_QUEUES,
                               cmp_pri, get_pri, set_pri, get_pos, set_pos);
                pthread_mutex_init(&ququelock[i], NULL);
                queuelabel[i]=1;
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].paa=paa;
        workerdata[i].ts=ts;
        workerdata[i].lock_queue=&lock_queue;                            
        workerdata[i].lock_current_root_node=&lock_current_root_node;
        workerdata[i].lock_bsf=&lock_bsf;
        workerdata[i].nodelist=nodelist->nlist;
        workerdata[i].amountnode=nodelist->node_amount;
        workerdata[i].index=index;
        workerdata[i].minimum_distance=minimum_distance;
        workerdata[i].node_counter=&node_counter;
        workerdata[i].pq=allpq[i];
        workerdata[i].bsf_result = &bsf_result;
        workerdata[i].lock_barrier=&lock_barrier;
        workerdata[i].alllock=ququelock;
        workerdata[i].allqueuelabel=queuelabel;
        workerdata[i].allpq=allpq;
        workerdata[i].startqueuenumber=i%NUM_PRIORITY_QUEUES;
        workerdata[i].workernumber=i;                                       
    }
        
    
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]),NULL,exact_search_worker_inmemory_hybridpqueue,(void*)&(workerdata[i]));
    }
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i],NULL);
    }

    // Free the nodes that where not popped.
    // Free the priority queue.
    pthread_barrier_destroy(&lock_barrier);

    //pqueue_free(pq);
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);
    bsf_result=bsf_result;

    //free(rfdata);
     //       printf("the number of LB distance calculation is %ld\t\t and the Real distance calculation is %ld\n ",LBDcalculationnumber,RDcalculationnumber);
    return bsf_result;

    // Free the nodes that where not popped.
}
// Botao's version
void* exact_search_worker_inmemory_hybridpqueue(void *rfdata)
{   
    threadPin(((MESSI_workerdata*)rfdata)->workernumber, maxquerythread);           

    isax_node *current_root_node;
    query_result *n;
    isax_index *index=((MESSI_workerdata*)rfdata)->index;
    ts_type *paa=((MESSI_workerdata*)rfdata)->paa;
    ts_type *ts=((MESSI_workerdata*)rfdata)->ts;
    pqueue_t *pq=((MESSI_workerdata*)rfdata)->pq;
    query_result *do_not_remove = ((MESSI_workerdata*)rfdata)->bsf_result;
    float minimum_distance=((MESSI_workerdata*)rfdata)->minimum_distance;
    int limit=((MESSI_workerdata*)rfdata)->limit;
    int checks = 0;
    bool finished=true;
    int current_root_node_number;
    int tight_bound = index->settings->tight_bound;
    int aggressive_check = index->settings->aggressive_check;
    query_result *bsf_result=(((MESSI_workerdata*)rfdata)->bsf_result);
    float bsfdisntance=bsf_result->distance;
    int calculate_node=0,calculate_node_quque=0;
    int tnumber=rand()% NUM_PRIORITY_QUEUES;
    int startqueuenumber=((MESSI_workerdata*)rfdata)->startqueuenumber;
    //COUNT_QUEUE_TIME_START

    if (((MESSI_workerdata*)rfdata)->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    while (1) 
    {
            current_root_node_number=__sync_fetch_and_add(((MESSI_workerdata*)rfdata)->node_counter,1);
            //printf("the number is %d\n",current_root_node_number );
            if(current_root_node_number>= ((MESSI_workerdata*)rfdata)->amountnode)
            break;
            current_root_node=((MESSI_workerdata*)rfdata)->nodelist[current_root_node_number];

            insert_tree_node_m_hybridpqueue(paa,current_root_node,index,bsfdisntance,((MESSI_workerdata*)rfdata)->allpq,((MESSI_workerdata*)rfdata)->alllock,&tnumber);
            //insert_tree_node_mW(paa,current_root_node,index,bsfdisntance,pq,((MESSI_workerdata*)rfdata)->lock_queue);

            
    }

    //COUNT_QUEUE_TIME_END
    //calculate_node_quque=pq->size;

    pthread_barrier_wait(((MESSI_workerdata*)rfdata)->lock_barrier);
    //printf("the size of quque is %d \n",pq->size);

    if (((MESSI_workerdata*)rfdata)->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    while (1)
    {
        pthread_mutex_lock(&(((MESSI_workerdata*)rfdata)->alllock[startqueuenumber]));
        n = pqueue_pop(((MESSI_workerdata*)rfdata)->allpq[startqueuenumber]);
        pthread_mutex_unlock(&(((MESSI_workerdata*)rfdata)->alllock[startqueuenumber]));
        if(n==NULL)
            break;
        //pthread_rwlock_rdlock(((MESSI_workerdata*)rfdata)->lock_bsf);
        bsfdisntance=bsf_result->distance;
        //pthread_rwlock_unlock(((MESSI_workerdata*)rfdata)->lock_bsf);
        // The best node has a worse mindist, so search is finished!

        if (n->distance > bsfdisntance || n->distance > minimum_distance) {
            break;
        }
        else 
        {
            // If it is a leaf, check its real distance.
            if (n->node->is_leaf) {

                checks++;

                float distance = calculate_node_distance2_inmemory(index, n->node, ts,paa, bsfdisntance);
                if (distance < bsfdisntance)
                {
                    pthread_rwlock_wrlock(((MESSI_workerdata*)rfdata)->lock_bsf);
                    if(distance < bsf_result->distance)
                    {
                        bsf_result->distance = distance;
                        bsf_result->node = n->node;
                    }
                    pthread_rwlock_unlock(((MESSI_workerdata*)rfdata)->lock_bsf);
                }

            }
            
        }
            free(n);
    }

    if( (((MESSI_workerdata*)rfdata)->allqueuelabel[startqueuenumber])==1)
    {
        (((MESSI_workerdata*)rfdata)->allqueuelabel[startqueuenumber])=0;
        pthread_mutex_lock(&(((MESSI_workerdata*)rfdata)->alllock[startqueuenumber]));
        while(n = pqueue_pop(((MESSI_workerdata*)rfdata)->allpq[startqueuenumber]))
        {
            free(n);
        }
        pthread_mutex_unlock(&(((MESSI_workerdata*)rfdata)->alllock[startqueuenumber]));
    }

    if (((MESSI_workerdata*)rfdata)->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    while(1)
    {   
        int offset=rand()% NUM_PRIORITY_QUEUES;
        finished=true;
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if((((MESSI_workerdata*)rfdata)->allqueuelabel[i])==1)
            {
                finished=false;
                while(1)
                {
                    pthread_mutex_lock(&(((MESSI_workerdata*)rfdata)->alllock[i]));
                    n = pqueue_pop(((MESSI_workerdata*)rfdata)->allpq[i]);
                    pthread_mutex_unlock(&(((MESSI_workerdata*)rfdata)->alllock[i]));
                    if(n==NULL)
                    break;
                    if (n->distance > bsfdisntance || n->distance > minimum_distance) {
                        break;
                    }        
                    else 
                    {
                        // If it is a leaf, check its real distance.
                        if (n->node->is_leaf) 
                        {
                            checks++;
                            float distance = calculate_node_distance2_inmemory(index, n->node, ts,paa, bsfdisntance);
                            if (distance < bsfdisntance)
                            {
                                pthread_rwlock_wrlock(((MESSI_workerdata*)rfdata)->lock_bsf);
                                if(distance < bsf_result->distance)
                                {
                                    bsf_result->distance = distance;
                                    bsf_result->node = n->node;
                                }
                                pthread_rwlock_unlock(((MESSI_workerdata*)rfdata)->lock_bsf);
                            }

                        }
            
                    }
                //add
                free(n);
                }

            }
        }
        if (finished)
        {
            break;
        }
    }

    if (((MESSI_workerdata*)rfdata)->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }


    //pthread_barrier_wait(((MESSI_workerdata*)rfdata)->lock_barrier);
    //while(n=pqueue_pop(pq))
    //{
            //free(n);
    //}
    //pqueue_free(pq);
    //
    

                                      //printf("create pq time is %f \n",worker_total_time );
    //printf("the check's node is\t %d\tthe local queue's node is\t%d\n",checks,calculate_node_quque);
}
// Botao's version
void insert_tree_node_m_hybridpqueue(float *paa,isax_node *node,isax_index *index,float bsf,pqueue_t **pq,pthread_mutex_t *lock_queue,int *tnumber)
{   
    float distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);
    if(distance < bsf)
    {
        if (node->is_leaf) 
        {   
            query_result * mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance=distance;
            pthread_mutex_lock(&lock_queue[*tnumber]);
            pqueue_insert(pq[*tnumber], mindist_result);
            pthread_mutex_unlock(&lock_queue[*tnumber]);
            *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
            added_tree_node++;
        }
        else
        {   
            if (node->left_child->isax_cardinalities != NULL)
            {
                insert_tree_node_m_hybridpqueue(paa,node->left_child,index, bsf,pq,lock_queue,tnumber);
            }
            if (node->right_child->isax_cardinalities != NULL)
            {
                insert_tree_node_m_hybridpqueue(paa,node->right_child,index,bsf,pq,lock_queue,tnumber);
            }
        }
    }
}


// -------------------------------------
// -------------------------------------

// ekosmas version
query_result exact_search_ParISnew_inmemory_hybrid_ekosmas(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance) 
{   
    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas(ts, paa, index);
    SET_APPROXIMATE(bsf_result.distance);

    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }

    if (maxquerythread == 1) {
        NUM_PRIORITY_QUEUES = 1;
    }
    else {
        NUM_PRIORITY_QUEUES = maxquerythread/2;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t*)*NUM_PRIORITY_QUEUES);
    int queuelabel[NUM_PRIORITY_QUEUES];                                           

    pthread_t threadid[maxquerythread];
    MESSI_workerdata_ekosmas workerdata[maxquerythread];
    pthread_mutex_t lock_bsf = PTHREAD_MUTEX_INITIALIZER;                   
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);

    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size/NUM_PRIORITY_QUEUES, cmp_pri, get_pri, set_pri, get_pos, set_pos);
        pthread_mutex_init(&ququelock[i], NULL);
        queuelabel[i]=1;                                                 
    }

    volatile int node_counter = 0;                                              

    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
        workerdata[i].lock_bsf = &lock_bsf;                                     // a lock to update BSF
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
        workerdata[i].bsf_result = &bsf_result;                                 // the BSF
        workerdata[i].lock_barrier = &lock_barrier;                             // barrier between queue inserts and queue pops
        workerdata[i].alllock = ququelock;                                      // all queues' locks
        workerdata[i].allqueuelabel = queuelabel;                                       
        workerdata[i].allpq = allpq;                                            // priority queues
        workerdata[i].startqueuenumber = i%NUM_PRIORITY_QUEUES;                            // initial priority queue to start
        workerdata[i].workernumber = i;
    }
        
    for (int i = 0; i < maxquerythread; i++) {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas, (void*)&(workerdata[i]));
    }
    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }

    pthread_barrier_destroy(&lock_barrier);

    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);

    return bsf_result;

    // Free the nodes that where not popped.
}
int process_queue_node_ekosmas(MESSI_workerdata_ekosmas *input_data, int i, int *checks)
{
    pthread_mutex_lock(&(input_data->alllock[i]));
    query_result *n = pqueue_pop(input_data->allpq[i]);
    pthread_mutex_unlock(&(input_data->alllock[i]));
    if(n == NULL) {
        return 0;
    }

    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    if (n->distance > bsfdisntance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
        return 0;
    }
    else {
        // If it is a leaf, check its real distance.
        if (n->node->is_leaf) {
            (*checks)++;                                                                    // This is just for debugging. It can be removed!

            float distance = calculate_node_distance2_inmemory_ekosmas(input_data->index, n->node, input_data->ts, input_data->paa, bsfdisntance);
            if (distance < bsfdisntance) {
                pthread_mutex_lock(input_data->lock_bsf);
                if(distance < bsf_result->distance)
                {
                    bsf_result->distance = distance;
                    bsf_result->node = n->node;
                }
                pthread_mutex_unlock(input_data->lock_bsf);
            }

        }
        
    }
    free(n);

    return 1;
}
// ekosmas version
void* exact_search_worker_inmemory_hybridpqueue_ekosmas(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas *input_data = (MESSI_workerdata_ekosmas*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    int startqueuenumber = input_data->startqueuenumber;
    
    // A. Populate Queues
    //COUNT_QUEUE_TIME_START
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node=input_data->nodelist[current_root_node_number];
            insert_tree_node_m_hybridpqueue_ekosmas(paa, current_root_node, index, bsfdisntance, input_data->allpq, input_data->alllock, &tnumber);            
    }

    // Wait all threads to fill in queues
    pthread_barrier_wait(input_data->lock_barrier);

    // B. Processing my queue
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }
    int checks = 0;                                                                     // This is just for debugging. It can be removed!
    while (process_queue_node_ekosmas(input_data, startqueuenumber, &checks)) {         // while a candidate queue node with smaller distance exists, compute actual distances
        ;
    }

    // C. Free any element left in my queue
    if( (input_data->allqueuelabel[startqueuenumber])==1)
    {
        (input_data->allqueuelabel[startqueuenumber])=0;
        pthread_mutex_lock(&(input_data->alllock[startqueuenumber]));
        query_result *n;
        while(n = pqueue_pop(input_data->allpq[startqueuenumber]))
        {
            free(n);
        }
        pthread_mutex_unlock(&(input_data->alllock[startqueuenumber]));
    }


    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // D. Process other uncompleted queues
    while(1)                                                                    
    {         
        bool finished = true;
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++) {
            if((input_data->allqueuelabel[i]) == 1) {
                finished = false;
                while(process_queue_node_ekosmas(input_data, i, &checks)) {
                    ;
                }
            }
        }

        if (finished) {
            break;
        }
    }   

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }
}
// ekosmas version
void insert_tree_node_m_hybridpqueue_ekosmas(float *paa, isax_node *node, isax_index *index, float bsf, pqueue_t **pq, pthread_mutex_t *lock_queue, int *tnumber)
{   

    // printf ("executing insert_tree_node_m_hybridpqueue_ekosmas\n");

    //COUNT_CAL_TIME_START
    float distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);

    if(distance < bsf)
    {
        if (node->is_leaf) 
        {   
            query_result * mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance = distance;
            pthread_mutex_lock(&lock_queue[*tnumber]);
            pqueue_insert(pq[*tnumber], mindist_result);
            pthread_mutex_unlock(&lock_queue[*tnumber]);
            *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
        }
        else
        {   
            if (node->left_child->isax_cardinalities != NULL)                                                       
            {
                insert_tree_node_m_hybridpqueue_ekosmas(paa,node->left_child,index, bsf,pq,lock_queue,tnumber);
            }
            if (node->right_child->isax_cardinalities != NULL)                                                      
            {
                insert_tree_node_m_hybridpqueue_ekosmas(paa,node->right_child,index,bsf,pq,lock_queue,tnumber);
            }
        }
    }
}

// -------------------------------------
// -------------------------------------

// ekosmas-EP version
query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_EP(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance) 
{   
    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas(ts, paa, index);
    SET_APPROXIMATE(bsf_result.distance);

    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t*) * maxquerythread);

    pthread_t threadid[maxquerythread];
    MESSI_workerdata_ekosmas_EP workerdata[maxquerythread];
    pthread_mutex_t lock_bsf = PTHREAD_MUTEX_INITIALIZER;                   
 
    for (int i = 0; i < maxquerythread; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size, cmp_pri, get_pri, set_pri, get_pos, set_pos);
    }

    volatile int node_counter = 0;                                              

    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
        workerdata[i].pq = allpq[i];                                                
        workerdata[i].lock_bsf = &lock_bsf;                                     // a lock to update BSF
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
        workerdata[i].bsf_result = &bsf_result;                                 // the BSF
        workerdata[i].workernumber = i;

    }
        
    for (int i = 0; i < maxquerythread; i++) {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_EP, (void*)&(workerdata[i]));
    }
    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }

    // Free the priority queue.
    for (int i = 0; i < maxquerythread; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);

    return bsf_result;

    // Free the nodes that where not popped.
}
int process_queue_node_ekosmas_EP(MESSI_workerdata_ekosmas_EP *input_data, pqueue_t *my_pq, int *checks)
{
    query_result *n = pqueue_pop(my_pq);
    if(n == NULL) {
        return 0;
    }

    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    if (n->distance > bsfdisntance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
        return 0;
    }
    else {
        // If it is a leaf, check its real distance.
        if (n->node->is_leaf) {
            (*checks)++;                                                                    // This is just for debugging. It can be removed!

            float distance = calculate_node_distance2_inmemory_ekosmas(input_data->index, n->node, input_data->ts, input_data->paa, bsfdisntance);
            if (distance < bsfdisntance) {
                pthread_mutex_lock(input_data->lock_bsf);
                if(distance < bsf_result->distance)
                {
                    bsf_result->distance = distance;
                    bsf_result->node = n->node;
                }
                pthread_mutex_unlock(input_data->lock_bsf);
            }
        }
    }
    free(n);

    return 1;
}
// ekosmas-EP version
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_EP(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_EP*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_EP *input_data = (MESSI_workerdata_ekosmas_EP*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    pqueue_t *my_pq = input_data->pq;  
    
    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    
    // A. Populate Queues
    //COUNT_QUEUE_TIME_START
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            insert_tree_node_m_hybridpqueue_ekosmas_EP(paa, current_root_node, index, bsfdisntance, my_pq);            

    }

    // B. Processing my queue
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }
    int checks = 0;                                                                     // This is just for debugging. It can be removed!
    while (process_queue_node_ekosmas_EP(input_data, my_pq, &checks)) {         // while a candidate queueu node with smaller distance exists, compute actual distances
        ;
    }

    // C. Free any element left in my queue
    query_result *n;
    while(n = pqueue_pop(my_pq))
    {
        free(n);
    }   


    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
    }
}
// ekosmas-EP version
void insert_tree_node_m_hybridpqueue_ekosmas_EP(float *paa, isax_node *node, isax_index *index, float bsf, pqueue_t *my_pq)
{   
    float distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);


    if(distance < bsf)
    {
        if (node->is_leaf) 
        {   
            query_result * mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance = distance;
            pqueue_insert(my_pq, mindist_result);
        }
        else
        {   
            if (node->left_child->isax_cardinalities != NULL)                                                       
            {
                insert_tree_node_m_hybridpqueue_ekosmas_EP(paa,node->left_child,index, bsf,my_pq);
            }
            if (node->right_child->isax_cardinalities != NULL)                                                     
            {
                insert_tree_node_m_hybridpqueue_ekosmas_EP(paa,node->right_child,index,bsf,my_pq);
            }
        }
    }
}

// -------------------------------------
// -------------------------------------

// ekosmas-lf version
query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_lf(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance, const char parallelism_in_subtree, const int third_phase, const int query_id) 
{   
    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas_lf(ts, paa, index, parallelism_in_subtree);
    query_result * volatile bsf_result_p = &bsf_result;
    SET_APPROXIMATE(bsf_result.distance);

    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }

    if (maxquerythread == 1) {
        NUM_PRIORITY_QUEUES = 1;
    }
    else {
        switch (third_phase) {
            case 1: 
            case 2: 
            case 3: 
            case 5:
            case 10: 
            case 12: NUM_PRIORITY_QUEUES = maxquerythread;
            break;  
            case 4: 
            case 6:
            case 11: 
            case 13: NUM_PRIORITY_QUEUES = maxquerythread/2;
            break;  
            case 7:  
            case 8: NUM_PRIORITY_QUEUES = 1;
            break;  
        }
    }    
    pqueue_t **allpq = calloc(NUM_PRIORITY_QUEUES, sizeof(pqueue_t*));
    // query_result ***allpq_data = malloc(sizeof(query_result **) * NUM_PRIORITY_QUEUES);
    array_list_t *array_lists = malloc(sizeof(array_list_t) * NUM_PRIORITY_QUEUES);
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        // allpq_data[i] = calloc (index->settings->root_nodes_size, sizeof(query_result *));
        initArrayList(&array_lists[i], index->settings->root_nodes_size);
    }
    sorted_array_t **sorted_arrays = calloc(NUM_PRIORITY_QUEUES, sizeof(sorted_array_t *));

    volatile unsigned long *next_queue_data_pos = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned long));       
    volatile unsigned char *queue_finished = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned char));
    volatile unsigned char *helper_queue_exist = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned char));
    volatile int *fai_queue_counters = calloc(NUM_PRIORITY_QUEUES, sizeof(int));
    volatile float **queue_bsf_distance = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned long));


    volatile int node_counter = 0;                                                                
    volatile unsigned long *sorted_array_counter = calloc(NUM_PRIORITY_QUEUES*ULONG_CACHE_PADDING, sizeof(unsigned long));    
    volatile unsigned long *sorted_array_FAI_counter = calloc(NUM_PRIORITY_QUEUES*ULONG_CACHE_PADDING, sizeof(unsigned long));

    pthread_barrier_t wait_tree_pruning_phase_to_finish;                                // used only for the no_help version of Fresh; it is required to ensure that query answering threads will process elements from the priority queue only after the tree pruning phase has completed.
    pthread_barrier_init(&wait_tree_pruning_phase_to_finish, NULL, maxquerythread);


    MESSI_workerdata_ekosmas_lf workerdata[maxquerythread];
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
        workerdata[i].allpq = allpq;
        workerdata[i].array_lists = array_lists;
        workerdata[i].sorted_arrays = sorted_arrays;
        workerdata[i].queue_finished = queue_finished;
        workerdata[i].fai_queue_counters = fai_queue_counters;
        workerdata[i].queue_bsf_distance = queue_bsf_distance;
        workerdata[i].helper_queue_exist = helper_queue_exist;
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
        workerdata[i].sorted_array_counter = sorted_array_counter;
        workerdata[i].sorted_array_FAI_counter = sorted_array_FAI_counter;
        workerdata[i].bsf_result_p = &bsf_result_p;                             // the BSF
        workerdata[i].workernumber = i;
        workerdata[i].parallelism_in_subtree = parallelism_in_subtree;
        workerdata[i].next_queue_data_pos = next_queue_data_pos;
        workerdata[i].query_id = query_id;
        workerdata[i].wait_tree_pruning_phase_to_finish = &wait_tree_pruning_phase_to_finish;
    }

    pthread_t threadid[maxquerythread];
    for (int i = 0; i < maxquerythread; i++) {
        switch (third_phase) {
            case 1: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_not_mixed_only_after_helper, (void*)&(workerdata[i])); 
            break;  
            case 2: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_only_after_helper, (void*)&(workerdata[i])); 
            break; 
            case 3: 
            case 4: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_full_helping, (void*)&(workerdata[i])); 
            break;  
            case 5: 
            case 6: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_no_helping, (void*)&(workerdata[i])); 
            break;  
            case 7: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_queue_full_help, (void*)&(workerdata[i])); 
            break;   
            case 8: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_sorted_array, (void*)&(workerdata[i])); 
            break;  

            case 10: 
            case 11: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_only_after_helper, (void*)&(workerdata[i])); 
            break;  
            case 12: 
            case 13: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_full_helping, (void*)&(workerdata[i])); 
            break;            
        }
        
    }
    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }

    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        if (!allpq[i]) {
            continue;
        }

        pqueue_free(allpq[i]);
    }
    free(allpq);
    free((void *)queue_finished);
    free((void *)helper_queue_exist);

    return *bsf_result_p;

    // +++ Free the nodes that where not popped.
}

static int help_queue_node(int node_id, MESSI_workerdata_ekosmas_lf *input_data, pqueue_t *pq, int pq_id, size_t pq_size, int *checks, const char parallelism_in_subtree, const unsigned char single_node, int move_num_pos, float queue_bsf)
{

    if (single_node) {                  // move inside priority queue to find the appropriate node in the path defined by queue_bsf, which is move_num_pos positions after node_id
        for (; move_num_pos > 0; move_num_pos--) {
            while (1) {
                if (left(node_id) < pq_size && ((query_result *)pq->d[node_id])->distance <= queue_bsf) {
                    node_id = left(node_id);
                    break;
                }
                
                if (right(node_id) < pq_size && ((query_result *)pq->d[node_id])->distance <= queue_bsf) {
                    node_id = right(node_id);
                    break;
                }

                while (node_id > 1 && (right(parent(node_id)) >= pq_size || node_id == right(parent(node_id))) ) {
                    node_id = parent(node_id);
                }
                
                if (node_id > 1 && right(parent(node_id)) < pq_size) {
                    node_id = right(parent(node_id));
                    if (((query_result *)pq->d[node_id])->distance <= queue_bsf) {
                        break;
                    }
                }

                return -1;
            }
        }        
    }

    if (node_id >= pq_size || input_data->queue_finished[pq_id]) {
        return -1;
    }

    query_result *n = pq->d[node_id];
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdistance = bsf_result->distance;

    if (n->distance > bsfdistance) {                                                        // The best node has a worse mindist, so search is finished! // EKOSMAS: Changed on 17 September 2020
        return node_id;
    }

    // If it is a leaf, check its real distance.
    if (n->node->is_leaf) {                                                                 
        if (n->distance >= 0) {                       
            (*checks)++;                                                                    // This is just for debugging. It can be removed!

            float distance = calculate_node_distance2_inmemory_ekosmas_lf(input_data->index, n, input_data->ts, input_data->paa, bsfdistance, parallelism_in_subtree);

            // update bsf, if needed
            while (distance < bsfdistance) {

                query_result *bsf_result_new = malloc(sizeof(query_result));
                bsf_result_new->distance = distance;
                bsf_result_new->node = n->node;

                if (!CASPTR(input_data->bsf_result_p, bsf_result, bsf_result_new)) {
                    free(bsf_result_new);
                }

                bsf_result = *(input_data->bsf_result_p);
                bsfdistance = bsf_result->distance;
            }
            
            if (n->distance >= 0) {                     
                n->distance = -1;                       //  in case of slow path, inform that this queue node has been processed
            }            
        }
        else {
            // printf("Skipping actual dinstance calculation since node is already calculated. This is good!!!\n");
        }
    }
    else {
        printf ("This queue node is not a leaf. Why???\n"); fflush(stdout);
    }

    if (!single_node) {
        help_queue_node(left(node_id), input_data, pq, pq_id, pq_size, checks, input_data->parallelism_in_subtree, 0, 0, 0);
        help_queue_node(right(node_id), input_data, pq, pq_id, pq_size, checks, input_data->parallelism_in_subtree, 0, 0, 0);
    }
    else {
        return node_id;
    }
}
static inline void help_queue(MESSI_workerdata_ekosmas_lf *input_data, pqueue_t *pq, int pq_id, int *checks, const char parallelism_in_subtree)
{


    size_t pq_size = pq->size;

    // Recall that the first element of the priority queue is not used.
    if (pq_size != 1) {     

        // initialize this queue's currently known bsf distance. This will define a single "path of interest" inside this queue.
        if (input_data->queue_bsf_distance[pq_id] == NULL) {
            float *tmp_float = malloc(sizeof(float));
            *tmp_float = (*(input_data->bsf_result_p))->distance;
            if (!CASPTR(&(input_data->queue_bsf_distance[pq_id]), NULL, tmp_float)) {
                free(tmp_float);
            }
        }

        float queue_bsf = *(input_data->queue_bsf_distance[pq_id]);


        int queue_node_path_pos = 0;
        int prev_queue_node_path_pos = 1;
        int start_queue_node_path_pos = 1;
        int move_num_pos;
        while (start_queue_node_path_pos > 0) {
            queue_node_path_pos = __sync_fetch_and_add(&(input_data->fai_queue_counters[pq_id]), 1) + 1;
            move_num_pos = queue_node_path_pos - prev_queue_node_path_pos;
            prev_queue_node_path_pos = queue_node_path_pos;
            start_queue_node_path_pos = help_queue_node(start_queue_node_path_pos, input_data, pq, pq_id, pq_size, checks, input_data->parallelism_in_subtree, 1, move_num_pos, queue_bsf);
        }

        // Search for unprocessed queue nodes in the "path of interest"
        help_queue_node(1, input_data, pq, pq_id, pq_size, checks, input_data->parallelism_in_subtree, 0, 0, 0);
    }

    if (!input_data->queue_finished[pq_id]) {
        input_data->queue_finished[pq_id] = 1;
    }
}
int process_queue_node_ekosmas_lf(MESSI_workerdata_ekosmas_lf *input_data, pqueue_t *pq, int pq_id, int *checks, const char parallelism_in_subtree)
{
    query_result *n = pqueue_top_lf(pq);        // read the min element of the queue, without removing it from the queue
    if(n == NULL) {
        if (!input_data->queue_finished[pq_id]) {
            input_data->queue_finished[pq_id] = 1;
        }
        return 0;
    }

    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdistance = bsf_result->distance;
    if (n->distance > bsfdistance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
        if (!input_data->queue_finished[pq_id]) {
            input_data->queue_finished[pq_id] = 1;
        }
        pqueue_pop_lf(pq, input_data);      
        return 0;
    }

    // If it is a leaf, check its real distance.
    if (n->node->is_leaf) {                                         
        (*checks)++;                                                

        float distance = calculate_node_distance2_inmemory_ekosmas_lf(input_data->index, n, input_data->ts, input_data->paa, bsfdistance, parallelism_in_subtree);

        // update bsf, if needed
        while (distance < bsfdistance) {
            query_result *bsf_result_new = malloc(sizeof(query_result));
            bsf_result_new->distance = distance;
            bsf_result_new->node = n->node;

            if (!CASPTR(input_data->bsf_result_p, bsf_result, bsf_result_new)) {
                free(bsf_result_new);
            }

            bsf_result = *(input_data->bsf_result_p);
            bsfdistance = bsf_result->distance;
        }

        if (n->distance >= 0) {
            n->distance = -1;           //  in case of slow path, inform that this queue node has been processed
        }
    }
    else {
        printf ("This queue node is not a leaf. Why???\n"); fflush(stdout);
    }

    pqueue_pop_lf(pq, input_data);      // remove the processed element from the queue


    return 1;
}

static inline void create_heap_from_data_queue(int pq_id, query_result ***allpq_data, volatile unsigned long *next_queue_data_pos, pqueue_t **allpq) {

    pqueue_t *local_pq = pqueue_init(next_queue_data_pos[pq_id], cmp_pri, get_pri, set_pri, get_pos, set_pos);

    // for all the elements of allpq_data[pq_id][i], that is |allpq_data[pq_id][i]| <= next_queue_data_pos[pq_id] and while
    // the heap has not been created (i.e. allpq[pq_id] == NULL), add elements to local pq. Notice that the allpq_data[pq_id][i]
    // array may contain holes of empty elements, due to helping.
    for (int i = 0 ; i<next_queue_data_pos[pq_id] && !allpq[pq_id]; i++) {
        if (!allpq_data[pq_id][i]) {
            continue;
        }
        pqueue_insert(local_pq, allpq_data[pq_id][i]);
    }

    if (allpq[pq_id] || !CASPTR(&allpq[pq_id], NULL, local_pq)) {
        pqueue_free(local_pq);
    }
}
static inline void create_heap_from_data_array(int pq_id, array_list_t *array_lists, volatile unsigned long *next_queue_data_pos, pqueue_t **allpq) {

    unsigned long array_elements = next_queue_data_pos[pq_id];
    pqueue_t *local_pq = pqueue_init(array_elements, cmp_pri, get_pri, set_pri, get_pos, set_pos);
    
    int j=0;
    int array_buckets_num = array_lists[pq_id].Top->num_node+1;
    unsigned long array_elements_traversed = 0;
    array_list_node_t *bucket = array_lists[pq_id].Top;
    for (int i = 0; i<array_buckets_num; i++, bucket = bucket->next) {
        for (int k = 0; k<array_lists[pq_id].element_size && array_elements_traversed<array_elements && !allpq[pq_id]; k++, array_elements_traversed++) {
            if (bucket->data[k].node) {
                query_result *queue_node = malloc(sizeof(query_result));

                queue_node->distance = bucket->data[k].distance;
                queue_node->node = bucket->data[k].node;

                local_pq->d[j+1] = queue_node;
                j++;
            }
        }
    }
    local_pq->size = j;              
    heap_sort(local_pq, j);

    if (allpq[pq_id] || !CASPTR(&allpq[pq_id], NULL, local_pq)) {
        pqueue_free(local_pq);
    }
}
int compare_sorted_array_items(const void *a, const void *b) {
    float dist_a = ((array_element_t*)a)->distance;
    float dist_b = ((array_element_t*)b)->distance;

    if (dist_a < dist_b) return -1;
    if (dist_a > dist_b) return 1;
    return 0;
}
static inline void create_sorted_array_from_data_queue(int pq_id, array_list_t *array_lists, volatile unsigned long *next_queue_data_pos, sorted_array_t **sorted_arrays) 
{
    sorted_array_t *local_sa = malloc(sizeof(sorted_array_t));
    unsigned long array_elements = next_queue_data_pos[pq_id];
    local_sa->data = malloc(array_elements * sizeof(array_element_t));

    size_t j = 0; 

    int array_buckets_num = array_lists[pq_id].Top->num_node+1;
    unsigned long array_elements_traversed = 0;
    array_list_node_t *bucket = array_lists[pq_id].Top;
    for (int i = 0; i<array_buckets_num; i++, bucket = bucket->next) {
        for (int k = 0; k<array_lists[pq_id].element_size && array_elements_traversed<array_elements && !sorted_arrays[pq_id]; k++, array_elements_traversed++) {
            if (bucket->data[k].node) {
                local_sa->data[j].node = bucket->data[k].node;
                local_sa->data[j].distance = bucket->data[k].distance;
                j++;
            }
        }
    }

    local_sa->num_elements = j;

    if (!sorted_arrays[pq_id]) {            
        qsort(local_sa->data, j, sizeof(array_element_t), compare_sorted_array_items);
    }

    if (sorted_arrays[pq_id] || !CASPTR(&sorted_arrays[pq_id], NULL, local_sa)) {
        free(local_sa->data);
        free(local_sa);
    }
}
static inline int process_sorted_array_element(array_element_t *n, MESSI_workerdata_ekosmas_lf *input_data, int *checks, const char parallelism_in_subtree)
{
    // process it!
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdistance = bsf_result->distance;
    if (n->distance > bsfdistance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
        return 0;
    }

    // If it is a leaf, check its real distance.
    if (n->node->is_leaf) {                                         
        if (n->distance >= 0) {                                     
            (*checks)++;                                                                    

            float distance = calculate_node_distance2_inmemory_ekosmas_lf(input_data->index, (query_result *) n, input_data->ts, input_data->paa, bsfdistance, parallelism_in_subtree);

            // update bsf, if needed
            while (distance < bsfdistance) {
                query_result *bsf_result_new = malloc(sizeof(query_result));
                bsf_result_new->distance = distance;
                bsf_result_new->node = n->node;

                if (!CASPTR(input_data->bsf_result_p, bsf_result, bsf_result_new)) {
                    free(bsf_result_new);
                }

                bsf_result = *(input_data->bsf_result_p);
                bsfdistance = bsf_result->distance;
            }

            if (n->distance >= 0) {                     
                n->distance = -1;                       //  in case of slow path, inform that this sorted array element has been processed
            }     
        }
    }
    else {
        printf ("This queue node is not a leaf. Why???\n"); fflush(stdout);
    }
    return 1;
}
static inline void help_sorted_array(MESSI_workerdata_ekosmas_lf *input_data, sorted_array_t *sa, int pq_id, int *checks, const char parallelism_in_subtree)
{
    size_t pq_size = sa->num_elements;

    if (pq_size != 0) {

        // initialize sorted_array_FAI_counter[pq_id] counter
        if (!input_data->sorted_array_FAI_counter[pq_id*ULONG_CACHE_PADDING] && input_data->sorted_array_counter[pq_id*ULONG_CACHE_PADDING]) {
            CASULONG(&input_data->sorted_array_FAI_counter[pq_id*ULONG_CACHE_PADDING], 0, input_data->sorted_array_FAI_counter[pq_id*ULONG_CACHE_PADDING]);
        }

        // repeatedly take an element with FAI
        int element_id; 
        while ((element_id = __sync_fetch_and_add(&(input_data->sorted_array_FAI_counter[pq_id*ULONG_CACHE_PADDING]), 1)) < pq_size) {
            array_element_t *n = &sa->data[element_id];
            if (!process_sorted_array_element(n, input_data, checks, parallelism_in_subtree))  
                break;
        }

        // search all previous array elements and help any unprocessed
        for (int i=0; i<pq_size && !input_data->queue_finished[pq_id]; i++) {
            array_element_t *n = &sa->data[i];
            if (n->distance >= 0 && !process_sorted_array_element(n, input_data, checks, parallelism_in_subtree))
                break;
        }
    }

    // mark this sorted array as processed/finished
    if (!input_data->queue_finished[pq_id]) {
        input_data->queue_finished[pq_id] = 1;
    }
}

// ekosmas-lf version - NOT MIXED - Version with 80 queues and helping during queue processing, only after a helper exists.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_not_mixed_only_after_helper(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
    
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    unique_leaves_in_arrays_cnt = 0;
    leaves_in_arrays_cnt = 0;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            // add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    if (DO_NOT_HELP) {                               
        pthread_barrier_wait(input_data->wait_tree_pruning_phase_to_finish);          
    }
    else {
        // A.1. Help unprocessed subtrees
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            // add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // A.2. Create my priority queue (Heap)
    if (!input_data->allpq[input_data->workernumber] && input_data->next_queue_data_pos[input_data->workernumber]) {
        create_heap_from_data_array(input_data->workernumber, input_data->array_lists, input_data->next_queue_data_pos, input_data->allpq);
    }


    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other priority queues to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < maxquerythread; i++)
        {
            if (!input_data->allpq[i] && input_data->next_queue_data_pos[i]) {
                create_heap_from_data_array(i, input_data->array_lists, input_data->next_queue_data_pos, input_data->allpq);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B. Processing my queue
    int checks = 0;                                                                     

    // B.1. while no helper exist, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    pqueue_t *my_pq = input_data->allpq[input_data->workernumber];
    if (my_pq) {
        int ret_val;
        while ( !input_data->helper_queue_exist[input_data->workernumber] &&
                (ret_val = process_queue_node_ekosmas_lf(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree))) {         
            ;
        }

        if (!ret_val && !input_data->queue_finished[input_data->workernumber]) {
            input_data->queue_finished[input_data->workernumber] = 1;
        }

        // B.2. if helpers exist and my queue has not yet finished, execute slow path
        if (input_data->helper_queue_exist[input_data->workernumber] && !input_data->queue_finished[input_data->workernumber]) {
            help_queue(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // B.3. Help non-finished queues
    if (!DO_NOT_HELP) {
        for (int i = 0; i < maxquerythread; i++)
        {
            if (input_data->queue_finished[i] || !input_data->allpq[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
                input_data->helper_queue_exist[i] = 1;
            }

            help_queue(input_data, input_data->allpq[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }

    if (leaves_in_arrays_cnt > 0) {
        COUNT_LEAVES_ARRAY(leaves_in_arrays_cnt);
    }

    if (unique_leaves_in_arrays_cnt > 0) {
        COUNT_UNIQUE_LEAVES_ARRAY(unique_leaves_in_arrays_cnt);
    }
}

// ekosmas-lf version - MIXED - Version with 80 queues and helping during queue processing, only after a helper exists.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_only_after_helper(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
  
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    unique_leaves_in_arrays_cnt = 0;
    leaves_in_arrays_cnt = 0;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    if (DO_NOT_HELP) {                               
        pthread_barrier_wait(input_data->wait_tree_pruning_phase_to_finish);          
    }
    else {
        // A.1. Help unprocessed subtrees
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // A.2. Create my priority queue (Heap)
    if (!input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_heap_from_data_array(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->array_lists, input_data->next_queue_data_pos, input_data->allpq);
    }

    // B. Processing my queue
    int checks = 0;                                                                    

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. while no helper exists, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    pqueue_t *my_pq = input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES];
    if (my_pq) {
        int ret_val;
        while ( !input_data->helper_queue_exist[input_data->workernumber] &&
                (ret_val = process_queue_node_ekosmas_lf(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree))) {         
            ;
        }

        if (!ret_val && !input_data->queue_finished[input_data->workernumber]) {
            input_data->queue_finished[input_data->workernumber] = 1;
        }

        // B.2. if helpers exist and my queue has not yet finished, execute slow path
        if (input_data->helper_queue_exist[input_data->workernumber] && !input_data->queue_finished[input_data->workernumber]) {
            help_queue(input_data, my_pq, input_data->workernumber % NUM_PRIORITY_QUEUES, &checks, input_data->parallelism_in_subtree);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other priority queues to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (!input_data->allpq[i] && input_data->next_queue_data_pos[i]) {
                create_heap_from_data_array(i, input_data->array_lists, input_data->next_queue_data_pos, input_data->allpq);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // B.3. Help non-finished queues
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (input_data->queue_finished[i] || !input_data->allpq[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
                input_data->helper_queue_exist[i] = 1;
            }

            help_queue(input_data, input_data->allpq[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }

    if (leaves_in_arrays_cnt > 0) {
        COUNT_LEAVES_ARRAY(leaves_in_arrays_cnt);
    }

    if (unique_leaves_in_arrays_cnt > 0) {
        COUNT_UNIQUE_LEAVES_ARRAY(unique_leaves_in_arrays_cnt);
    }
}

// ekosmas-lf version - MIXED - Version with 40 queues and helping during queue processing from the beggining.
// ekosmas-lf version - MIXED - Version with 80 queues and helping during queue processing from the beggining.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_full_helping(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
    
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    unique_leaves_in_arrays_cnt = 0;
    leaves_in_arrays_cnt = 0;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    if (DO_NOT_HELP) {                               
        pthread_barrier_wait(input_data->wait_tree_pruning_phase_to_finish);          
    }
    else {
        // A.1. Help unprocessed subtrees
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    


    // A.2. Create my priority queue (Heap)
    if (!input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_heap_from_data_array(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->array_lists, input_data->next_queue_data_pos, input_data->allpq);
    }

    // B. Processing my queue
    int checks = 0;                                                                     

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. while no helper exist, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    pqueue_t *my_pq = input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES];
    if (my_pq) {
        // B.2. if my queue has not yet finished, execute slow path
        if (!input_data->queue_finished[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
            help_queue(input_data, my_pq, input_data->workernumber % NUM_PRIORITY_QUEUES, &checks, input_data->parallelism_in_subtree);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other priority queues to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (!input_data->allpq[i] && input_data->next_queue_data_pos[i]) {
                create_heap_from_data_array(i, input_data->array_lists, input_data->next_queue_data_pos, input_data->allpq);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // B.3. Help non-finished queues
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (input_data->queue_finished[i] || !input_data->allpq[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            help_queue(input_data, input_data->allpq[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }

    if (leaves_in_arrays_cnt > 0) {
        COUNT_LEAVES_ARRAY(leaves_in_arrays_cnt);
    }

    if (unique_leaves_in_arrays_cnt > 0) {
        COUNT_UNIQUE_LEAVES_ARRAY(unique_leaves_in_arrays_cnt);
    }    
}

// ekosmas-lf version - MIXED - Version with 40 queues and no helping during queue processing.
// ekosmas-lf version - MIXED - Version with 80 queues and no helping during queue processing
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_no_helping(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
    
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    unique_leaves_in_arrays_cnt = 0;
    leaves_in_arrays_cnt = 0;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    if (DO_NOT_HELP) {                               
        pthread_barrier_wait(input_data->wait_tree_pruning_phase_to_finish);          
    }

    // A.1. Help unprocessed subtrees
    for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
        current_root_node = input_data->nodelist[current_root_node_number];
        add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // A.2. Create my priority queue (Heap)
    if (!input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_heap_from_data_array(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->array_lists, input_data->next_queue_data_pos, input_data->allpq);
    }

    // B. Processing my queue
    int checks = 0;                                                                     

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. while no helper exist, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    if (input_data->workernumber >= NUM_PRIORITY_QUEUES) {
        return NULL;
    }
    pqueue_t *my_pq = input_data->allpq[input_data->workernumber];
    if (my_pq) {
        int ret_val;
        while ( !input_data->helper_queue_exist[input_data->workernumber] &&
                (ret_val = process_queue_node_ekosmas_lf(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree))) {         
            ;
        }

        if (!ret_val && !input_data->queue_finished[input_data->workernumber]) {
            input_data->queue_finished[input_data->workernumber] = 1;
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }

    if (leaves_in_arrays_cnt > 0) {
        COUNT_LEAVES_ARRAY(leaves_in_arrays_cnt);
    }

    if (unique_leaves_in_arrays_cnt > 0) {
        COUNT_UNIQUE_LEAVES_ARRAY(unique_leaves_in_arrays_cnt);
    }
}

// ekosmas-lf version - Version with 1 queue and helping during queue processing from the beggining.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_queue_full_help(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
    
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    unique_leaves_in_arrays_cnt = 0;
    leaves_in_arrays_cnt = 0;    
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);

    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    if (DO_NOT_HELP) {                               
        pthread_barrier_wait(input_data->wait_tree_pruning_phase_to_finish);          
    }
    else {
        // A.1. Help unprocessed subtrees
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // A.2. Create my priority queue (Heap)
    if (!input_data->allpq[0] && input_data->next_queue_data_pos[0]) {
        create_heap_from_data_array(0, input_data->array_lists, input_data->next_queue_data_pos, input_data->allpq);
    }

    // B. Processing priority queue
    int checks = 0;                                                                    

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. if priority queue has not yet finished, process it
    pqueue_t *my_pq = input_data->allpq[0];
    if (!input_data->queue_finished[0]) {
        help_queue(input_data, my_pq, 0, &checks, input_data->parallelism_in_subtree);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
    }

    if (leaves_in_arrays_cnt > 0) {
        COUNT_LEAVES_ARRAY(leaves_in_arrays_cnt);
    }

    if (unique_leaves_in_arrays_cnt > 0) {
        COUNT_UNIQUE_LEAVES_ARRAY(unique_leaves_in_arrays_cnt);
    }
}

// ekosmas-lf version - Version with 1 sorted array and helping during sorted array processing from the beggining.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_sorted_array(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
    
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    unique_leaves_in_arrays_cnt = 0;
    leaves_in_arrays_cnt = 0;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);

    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    if (DO_NOT_HELP) {                              
        pthread_barrier_wait(input_data->wait_tree_pruning_phase_to_finish);          
    }
    else {
        // A.1. Help unprocessed subtrees
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // A.2. Create my sorted array
    if (!input_data->sorted_arrays[0] && input_data->next_queue_data_pos[0]) {
        create_sorted_array_from_data_queue(0, input_data->array_lists, input_data->next_queue_data_pos, input_data->sorted_arrays);
    }

    // B. Processing priority queue
    int checks = 0;                                                                     

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. if priority queue has not yet finished, process it
    sorted_array_t *my_array = input_data->sorted_arrays[0];
    if (!input_data->queue_finished[0]) {
        help_sorted_array(input_data, my_array, 0, &checks, input_data->parallelism_in_subtree);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
    }

    if (leaves_in_arrays_cnt > 0) {
        COUNT_LEAVES_ARRAY(leaves_in_arrays_cnt);
    }

    if (unique_leaves_in_arrays_cnt > 0) {
        COUNT_UNIQUE_LEAVES_ARRAY(unique_leaves_in_arrays_cnt);
    }    
}

// ekosmas-lf version - MIXED - Version with 80 sorted arrays and helping during sorted array processing, only after a helper exists.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_only_after_helper(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
  
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand() % NUM_PRIORITY_QUEUES;
    unique_leaves_in_arrays_cnt = 0;
    leaves_in_arrays_cnt = 0;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    if (DO_NOT_HELP) {                               
        pthread_barrier_wait(input_data->wait_tree_pruning_phase_to_finish);          
    }
    else {
        // A.1. Help unprocessed subtrees
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // A.2. Create my sorted array
    if (!input_data->sorted_arrays[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_sorted_array_from_data_queue(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->array_lists, input_data->next_queue_data_pos, input_data->sorted_arrays);
    }

    // B. Processing my sorted array
    int checks = 0;                                                                     // This is just for debugging. It can be removed!

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. while no helper exists, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    sorted_array_t *my_array = input_data->sorted_arrays[input_data->workernumber % NUM_PRIORITY_QUEUES];
    if (my_array) {
        int ret_val = 1;
        while ( !input_data->helper_queue_exist[input_data->workernumber]) {
            int element_id = input_data->sorted_array_counter[input_data->workernumber*ULONG_CACHE_PADDING];
            input_data->sorted_array_counter[input_data->workernumber*ULONG_CACHE_PADDING] = element_id + 1;

            size_t array_size = my_array->num_elements;
            if (element_id >= array_size) {
                break;
            }

            array_element_t *n = &my_array->data[element_id];
            if (!(ret_val = process_sorted_array_element(n, input_data, &checks, input_data->parallelism_in_subtree)))         
                break;
        }

        if (!ret_val && !input_data->queue_finished[input_data->workernumber] && !input_data->helper_queue_exist[input_data->workernumber]) {
            input_data->queue_finished[input_data->workernumber] = 1;
        }

        // B.2. if helpers exist and my sorted array has not yet finished, execute slow path
        if (input_data->helper_queue_exist[input_data->workernumber] && !input_data->queue_finished[input_data->workernumber]) {
            help_sorted_array(input_data, my_array, input_data->workernumber % NUM_PRIORITY_QUEUES, &checks, input_data->parallelism_in_subtree);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other sorted arrays to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (!input_data->sorted_arrays[i] && input_data->next_queue_data_pos[i]) {
                create_sorted_array_from_data_queue(i, input_data->array_lists, input_data->next_queue_data_pos, input_data->sorted_arrays);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // B.3. Help non-finished sorted arrays
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (input_data->queue_finished[i] || !input_data->sorted_arrays[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
                input_data->helper_queue_exist[i] = 1;
            }

            help_sorted_array(input_data, input_data->sorted_arrays[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }

    if (leaves_in_arrays_cnt > 0) {
        COUNT_LEAVES_ARRAY(leaves_in_arrays_cnt);
    }

    if (unique_leaves_in_arrays_cnt > 0) {
        COUNT_UNIQUE_LEAVES_ARRAY(unique_leaves_in_arrays_cnt);
    }
}

// ekosmas-lf version - MIXED - Version with 80 sorted arrays and helping during sorted array processing from the beggining.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_full_helping(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);


    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;

    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand() % NUM_PRIORITY_QUEUES;
    unique_leaves_in_arrays_cnt = 0;
    leaves_in_arrays_cnt = 0;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    if (DO_NOT_HELP) {                               
        pthread_barrier_wait(input_data->wait_tree_pruning_phase_to_finish);          
    }
    else {
        // A.1. Help unprocessed subtrees
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // A.2. Create my sorted array
    if (!input_data->sorted_arrays[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_sorted_array_from_data_queue(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->array_lists, input_data->next_queue_data_pos, input_data->sorted_arrays);
    }

    // B. Processing my sorted array
    int checks = 0;                                                                     

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. execute slow path: while a candidate queue node with smaller distance exists, compute actual distances
    sorted_array_t *my_array = input_data->sorted_arrays[input_data->workernumber % NUM_PRIORITY_QUEUES];
    if (my_array && !input_data->queue_finished[input_data->workernumber]) {
        help_sorted_array(input_data, my_array, input_data->workernumber % NUM_PRIORITY_QUEUES, &checks, input_data->parallelism_in_subtree);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other sorted arrays to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (!input_data->sorted_arrays[i] && input_data->next_queue_data_pos[i]) {
                create_sorted_array_from_data_queue(i, input_data->array_lists, input_data->next_queue_data_pos, input_data->sorted_arrays);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // B.3. Help non-finished sorted arrays
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (input_data->queue_finished[i] || !input_data->sorted_arrays[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
                input_data->helper_queue_exist[i] = 1;
            }

            help_sorted_array(input_data, input_data->sorted_arrays[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }

    if (leaves_in_arrays_cnt > 0) {
        COUNT_LEAVES_ARRAY(leaves_in_arrays_cnt);
    }

    if (unique_leaves_in_arrays_cnt > 0) {
        COUNT_UNIQUE_LEAVES_ARRAY(unique_leaves_in_arrays_cnt);
    }
}

// ekosmas-lf version
void add_to_array_data_lf_opt(float *paa, isax_node *node, isax_index *index, float bsf, array_list_t *array_lists, int *tnumber, volatile unsigned long *next_queue_data_pos, const unsigned long query_id, volatile unsigned long *node_index, volatile unsigned long *node_index_fai, unsigned long *cur_index, unsigned long *next_index, volatile unsigned long *subtree_prune_helpers_exist, char *prunning_helpers_exist, char is_scanning, volatile unsigned long *stop, char fai_only_after_help)
{
    float distance = -1;

    if (node->processed == query_id) {
        return;
    }

    if (*next_index == 0) {
        if (*subtree_prune_helpers_exist < query_id && fai_only_after_help) {
            *next_index = (unsigned long) *node_index;
            *node_index = (volatile unsigned long) *next_index + 1;
        }
        else {
            if (!node_index_fai && fai_only_after_help) {
                CASULONG(&node_index_fai, (unsigned long) 0, (unsigned long) node_index);
            }
            *next_index = __sync_fetch_and_add(node_index_fai, 1);
        }
    }

    if ((node->is_pruned > -1*query_id || node->is_pruned < query_id) && *stop < query_id) {
        distance =  minidist_paa_to_isax(paa, node->isax_values,
                                        node->isax_cardinalities,
                                        index->settings->sax_bit_cardinality,
                                        index->settings->sax_alphabet_cardinality,
                                        index->settings->paa_segments,
                                        MINVAL, MAXVAL,
                                        index->settings->mindist_sqrt);
        if(distance < bsf && node->processed < query_id && (node->is_pruned > -1*query_id || node->is_pruned < query_id)) {
            node->is_pruned = -1 * query_id;                                            
        }
        else if (node->is_pruned > -1*query_id || node->is_pruned < query_id) {
            node->is_pruned = query_id;
        }
    }

    if ((*next_index == *cur_index || is_scanning) && node->is_leaf && node->is_pruned == -1 * query_id && *stop < query_id) {
        if (distance == -1) {
            distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);
        }

        unsigned long queue_data_pos = __sync_fetch_and_add(&(next_queue_data_pos[*tnumber]),1);
        array_element_t *array_elem = get_element_at(queue_data_pos, &array_lists[*tnumber]);
        array_elem->distance = distance;
        array_elem->node = node;

        leaves_in_arrays_cnt++;

        *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
    }

    if (*next_index == *cur_index && !is_scanning && *stop < query_id) {
        if (*subtree_prune_helpers_exist < query_id && fai_only_after_help) {
            *next_index = (unsigned long) *node_index;
            *node_index = (volatile unsigned long) *next_index + 1;
            // if (*next_index > (*cur_index) + 1) {
            //     *prunning_helpers_exist = 1;
            // }
        }
        else {
            if (!node_index_fai && fai_only_after_help) {
                CASULONG(&node_index_fai, (unsigned long) 0, (unsigned long) node_index);
            }
            *next_index = __sync_fetch_and_add(node_index_fai, 1);
        }
    }

    if (node->is_pruned == -1 * query_id && !node->is_leaf && *stop < query_id) {   
        if (node->left_child->isax_cardinalities != NULL && node->left_child->processed < query_id)                        
        {
            (*cur_index)++;
            add_to_array_data_lf_opt(paa, node->left_child, index,  bsf, array_lists, tnumber, next_queue_data_pos, query_id, node_index, node_index_fai, cur_index, next_index, subtree_prune_helpers_exist, prunning_helpers_exist, is_scanning, stop, fai_only_after_help);
        }
        if (node->right_child->isax_cardinalities != NULL && node->right_child->processed < query_id)                      
        {
            (*cur_index)++;
            add_to_array_data_lf_opt(paa, node->right_child, index,  bsf, array_lists, tnumber, next_queue_data_pos, query_id, node_index, node_index_fai, cur_index, next_index, subtree_prune_helpers_exist, prunning_helpers_exist, is_scanning, stop, fai_only_after_help);
        }
    }

    // mark node as processed
    if ((node->is_leaf || node->is_pruned == query_id || !*prunning_helpers_exist || is_scanning) && node->processed < query_id) {
        node->processed = query_id;         
        if (node->is_leaf && node->is_pruned == -1 * query_id)
            unique_leaves_in_arrays_cnt++;
    }
}

void add_to_array_data_lf(float *paa, isax_node *node, isax_index *index, float bsf, array_list_t *array_lists, int *tnumber, volatile unsigned long *next_queue_data_pos, const int query_id)
{   

    if (node->processed == query_id) {         // it can only be: node->processed <= query_id
        return;
    }


    //COUNT_CAL_TIME_START
    float distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);
    //COUNT_CAL_TIME_END
    if(distance < bsf && node->processed < query_id)
    {
        if (node->is_leaf) 
        {   
            unsigned long queue_data_pos = __sync_fetch_and_add(&(next_queue_data_pos[*tnumber]),1);
            array_element_t *array_elem = get_element_at(queue_data_pos, &array_lists[*tnumber]);
            array_elem->distance = distance;
            array_elem->node = node;
    
            leaves_in_arrays_cnt++;

            *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
        }
        else
        {   
            if (node->left_child->isax_cardinalities != NULL && node->left_child->processed < query_id)                        
            {
                add_to_array_data_lf(paa, node->left_child, index, bsf, array_lists, tnumber, next_queue_data_pos, query_id);
            }
            if (node->right_child->isax_cardinalities != NULL && node->right_child->processed < query_id)                      
            {
                add_to_array_data_lf(paa, node->right_child, index, bsf, array_lists, tnumber, next_queue_data_pos, query_id);
            }
        }
    }

    // mark node as processed
    if (node->processed < query_id) {
        node->processed = query_id;
        if (node->is_leaf && distance < bsf) {
            unique_leaves_in_arrays_cnt++;
        }
    }
}

