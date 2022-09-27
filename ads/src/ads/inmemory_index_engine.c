//
//  Updated by Eleftherios Kosmas on May 2020.
//



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
#include "ads/isax_query_engine.h"
#include "ads/inmemory_index_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/pqueue.h"
#include "ads/sax/sax.h"
#include "ads/isax_node_split.h"
#include <sched.h>
#include <malloc.h>


inline void backoff_delay_char(unsigned long backoff, volatile unsigned char *stop) {
    if (!backoff){
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}

inline void backoff_delay_lockfree_subtree_copy(unsigned long backoff, isax_node * volatile *stop) {
    if (!backoff){
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}

inline void backoff_delay_lockfree_subtree_parallel(unsigned long backoff, volatile unsigned char *stop) {
    if (!backoff){
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}

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

long int count_ts_in_nodes (isax_node *root_node, const char parallelism_in_subtree, const char recBuf_helpers_exist)
{
    long int my_subtree_nodes = 0;

    if (!root_node -> is_leaf) {
        my_subtree_nodes = count_ts_in_nodes(root_node->left_child, parallelism_in_subtree, recBuf_helpers_exist);       
        my_subtree_nodes += count_ts_in_nodes(root_node->right_child, parallelism_in_subtree, recBuf_helpers_exist);
        return my_subtree_nodes;
    }
    else {
        if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && recBuf_helpers_exist) ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && root_node->recBuf_leaf_helpers_exist)) {
            if (root_node->fai_leaf_size == 0) {
                return root_node->leaf_size;
            }
            else if (root_node->fai_leaf_size < root_node->leaf_size) {
                printf ("root_node->fai_leaf_size < root_node->leaf_size  !!!!\n");fflush(stdout);
            }
            return root_node->fai_leaf_size;
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW) {
            return root_node->buffer->partial_buffer_size;
        }
        else {
            return root_node->leaf_size;
        }
    }
}
inline void check_validity(isax_index *index, long int ts_num) {

    // print the number of threads helped each block (not required for validity)
    // unsigned long total_blocks = ts_num/read_block_length;
    // for (int i=0; i < total_blocks; i++) {
    //     if (block_helpers_num[i]) {
    //         printf("Block [%d] was helped by [%d] threads\n", i, block_helpers_num[i]);
    //     }
    // }

    // count the total number of time series stored into receive buffers
    ts_in_RecBufs_cnt = 0;
    for (int i=0; i < index->fbl->number_of_buffers; i++) {


        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer*)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized) {
            continue;
        }


        // int tmp_count = 0;
        for (int k = 0; k < maxquerythread; k++)
        {
            // tmp_count += current_fbl_node->buffer_size[k];
            ts_in_RecBufs_cnt += current_fbl_node->buffer_size[k];
         }

        // printf("RecBuf[%d] contains [%d] iSAX summarries\n", i, tmp_count);
    }

    // print difference with actual total time series in raw file
    // printf ("Total series in RecBufs = [%d] which are [%d] more than total series in raw file\n", ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - ts_num);

    // count the total number of time series stored into tree index
    ts_in_tree_cnt = 0;
    non_empty_subtrees_cnt = 0;
    min_ts_in_subtrees = ts_num;
    max_ts_in_subtrees = 0;
    int cnt_1_10 = 0;
    int cnt_10_100 = 0;
    int cnt_100_1000 = 0;
    int cnt_1000_10000 = 0;
    int cnt_10000_100000 = 0;
    int cnt_100000_1000000 = 0;
    int cnt_1000000_10000000 = 0;
    for (int i=0; i < index->fbl->number_of_buffers; i++) {

        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer*)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized) {
            continue;
        }

        non_empty_subtrees_cnt++;
        long int tmp_num = count_ts_in_nodes((isax_node *)current_fbl_node->node, NO_PARALLELISM_IN_SUBTREE, 0);
        ts_in_tree_cnt += tmp_num;
        if (tmp_num < min_ts_in_subtrees) {
            min_ts_in_subtrees = tmp_num;
        }
        else if (tmp_num > max_ts_in_subtrees) {
            max_ts_in_subtrees = tmp_num;
        }

        // printf("Subtree[%d] contains [%d] nodes\n", i, tmp_num);

        if (tmp_num < 10) {
            cnt_1_10 ++;
        }
        else if (tmp_num >= 10 && tmp_num < 100) {
            cnt_10_100 ++;
        }
        else if (tmp_num >= 100 && tmp_num < 1000) {
            cnt_100_1000 ++;
        }
        else if (tmp_num >= 1000 && tmp_num < 10000) {
            cnt_1000_10000 ++;
        }
        else if (tmp_num >= 10000 && tmp_num < 100000) {
            cnt_10000_100000 ++;
        }
        else if (tmp_num >= 100000 && tmp_num < 1000000) {
            cnt_100000_1000000 ++;
        }
        else if (tmp_num >= 1000000 && tmp_num < 10000000) {
            cnt_1000000_10000000 ++;
        }

        // if (recBuf_helpers_num[i]) {
        //     printf("RecBuf [%d] was helped by [%d] threads and contains [%d] nodes \n", i, recBuf_helpers_num[i], tmp_num);
        // }
    }

    // printf("\nThere exist [%d] subtrees with 1-9 nodes\n", cnt_1_10);
    // printf("There exist [%d] subtrees with 10-99 nodes\n", cnt_10_100);
    // printf("There exist [%d] subtrees with 100-999 nodes\n", cnt_100_1000);
    // printf("There exist [%d] subtrees with 1000-9999 nodes\n", cnt_1000_10000);
    // printf("There exist [%d] subtrees with 10000-99999 nodes\n", cnt_10000_100000);
    // printf("There exist [%d] subtrees with 100000-999999 nodes\n", cnt_100000_1000000);
    // printf("There exist [%d] subtrees with 1000000-9999999 nodes\n", cnt_1000000_10000000);

    // compare numbers of time series stored into receive buffers and tree index. They have to be the same!!!
    // printf ("Total series in Tree = [%d] which are [%d] more than total series in RecBufs\n", ts_in_tree_cnt, ts_in_tree_cnt - ts_in_RecBufs_cnt);
}
inline void check_validity_ekosmas(isax_index *index, long int ts_num) {

    // print the number of threads helped each block (not required for validity)
    // unsigned long total_blocks = ts_num/read_block_length;
    // for (int i=0; i < total_blocks; i++) {
    //     if (block_helpers_num[i]) {
    //         printf("Block [%d] was helped by [%d] threads\n", i, block_helpers_num[i]);
    //     }
    // }

    // count the total number of time series stored into receive buffers
    ts_in_RecBufs_cnt = 0;
    for (int i=0; i < index->fbl->number_of_buffers; i++) {


        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas*)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized) {
            continue;
        }


        // int tmp_count = 0;
        for (int k = 0; k < maxquerythread; k++)
        {
            // tmp_count += current_fbl_node->buffer_size[k];
            ts_in_RecBufs_cnt += current_fbl_node->buffer_size[k];
         }

        // printf("RecBuf[%d] contains [%d] iSAX summarries\n", i, tmp_count);
    }

    // print difference with actual total time series in raw file
    // printf ("Total series in RecBufs = [%d] which are [%d] more than total series in raw file\n", ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - ts_num);

    // count the total number of time series stored into tree index
    ts_in_tree_cnt = 0;
    non_empty_subtrees_cnt = 0;
    min_ts_in_subtrees = ts_num;
    max_ts_in_subtrees = 0;
    int cnt_1_10 = 0;
    int cnt_10_100 = 0;
    int cnt_100_1000 = 0;
    int cnt_1000_10000 = 0;
    int cnt_10000_100000 = 0;
    int cnt_100000_1000000 = 0;
    int cnt_1000000_10000000 = 0;
    for (int i=0; i < index->fbl->number_of_buffers; i++) {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas*)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized) {
            continue;
        }

        non_empty_subtrees_cnt++;
        long int tmp_num = count_ts_in_nodes((isax_node *)current_fbl_node->node, NO_PARALLELISM_IN_SUBTREE, 0); 
        ts_in_tree_cnt += tmp_num;
        if (tmp_num < min_ts_in_subtrees) {
            min_ts_in_subtrees = tmp_num;
        }
        else if (tmp_num > max_ts_in_subtrees) {
            max_ts_in_subtrees = tmp_num;
        }

        // printf("Subtree[%d] contains [%d] nodes\n", i, tmp_num);

        if (tmp_num < 10) {
            cnt_1_10 ++;
        }
        else if (tmp_num >= 10 && tmp_num < 100) {
            cnt_10_100 ++;
        }
        else if (tmp_num >= 100 && tmp_num < 1000) {
            cnt_100_1000 ++;
        }
        else if (tmp_num >= 1000 && tmp_num < 10000) {
            cnt_1000_10000 ++;
        }
        else if (tmp_num >= 10000 && tmp_num < 100000) {
            cnt_10000_100000 ++;
        }
        else if (tmp_num >= 100000 && tmp_num < 1000000) {
            cnt_100000_1000000 ++;
        }
        else if (tmp_num >= 1000000 && tmp_num < 10000000) {
            cnt_1000000_10000000 ++;
        }

        // if (recBuf_helpers_num[i]) {
        //     printf("RecBuf [%d] was helped by [%d] threads and contains [%d] nodes \n", i, recBuf_helpers_num[i], tmp_num);
        // }
    }

    // printf("\nThere exist [%d] subtrees with 1-9 nodes\n", cnt_1_10);
    // printf("There exist [%d] subtrees with 10-99 nodes\n", cnt_10_100);
    // printf("There exist [%d] subtrees with 100-999 nodes\n", cnt_100_1000);
    // printf("There exist [%d] subtrees with 1000-9999 nodes\n", cnt_1000_10000);
    // printf("There exist [%d] subtrees with 10000-99999 nodes\n", cnt_10000_100000);
    // printf("There exist [%d] subtrees with 100000-999999 nodes\n", cnt_100000_1000000);
    // printf("There exist [%d] subtrees with 1000000-9999999 nodes\n", cnt_1000000_10000000);

    // compare numbers of time series stored into receive buffers and tree index. They have to be the same!!!
    // printf ("Total series in Tree = [%d] which are [%d] more than total series in RecBufs\n", ts_in_tree_cnt, ts_in_tree_cnt - ts_in_RecBufs_cnt);
}
void check_validity_ekosmas_lf(isax_index *index, long int ts_num, const char parallelism_in_subtree) {

    // print the number of threads helped each block (not required for validity)
    // unsigned long total_blocks = ts_num/read_block_length;
    // for (int i=0; i < total_blocks; i++) {
    //     if (block_helpers_num[i]) {
    //         printf("Block [%d] was helped by [%d] threads\n", i, block_helpers_num[i]);
    //     }
    // }

    // count the total number of time series stored into receive buffers
    ts_in_RecBufs_cnt = 0;
    for (int i=0; i < index->fbl->number_of_buffers; i++) {


        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized) {
            continue;
        }


        // int tmp_count = 0;
        for (int k = 0; k < maxquerythread; k++)
        {
            // tmp_count += current_fbl_node->buffer_size[k];
            ts_in_RecBufs_cnt += current_fbl_node->buffer_size[k];
         }

        // printf("RecBuf[%d] contains [%d] iSAX summarries\n", i, tmp_count);
    }

    // print difference with actual total time series in raw file
    // printf ("Total series in RecBufs = [%d] which are [%d] more than total series in raw file\n", ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - ts_num);

    // count the total number of time series stored into tree index
    ts_in_tree_cnt = 0;
    non_empty_subtrees_cnt = 0;
    min_ts_in_subtrees = ts_num;
    max_ts_in_subtrees = 0;
    int cnt_1_10 = 0;
    int cnt_10_100 = 0;
    int cnt_100_1000 = 0;
    int cnt_1000_10000 = 0;
    int cnt_10000_100000 = 0;
    int cnt_100000_1000000 = 0;
    int cnt_1000000_10000000 = 0;
    for (int i=0; i < index->fbl->number_of_buffers; i++) {

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized) {
            continue;
        }

        non_empty_subtrees_cnt++;
        long int tmp_num = count_ts_in_nodes((isax_node *)current_fbl_node->node, parallelism_in_subtree, current_fbl_node->recBuf_helpers_exist); 
        ts_in_tree_cnt += tmp_num;
        if (tmp_num < min_ts_in_subtrees) {
            min_ts_in_subtrees = tmp_num;
        }
        else if (tmp_num > max_ts_in_subtrees) {
            max_ts_in_subtrees = tmp_num;
        }

        // printf("Subtree[%d] contains [%d] nodes\n", i, tmp_num);

        if (tmp_num < 10) {
            cnt_1_10 ++;
        }
        else if (tmp_num >= 10 && tmp_num < 100) {
            cnt_10_100 ++;
        }
        else if (tmp_num >= 100 && tmp_num < 1000) {
            cnt_100_1000 ++;
        }
        else if (tmp_num >= 1000 && tmp_num < 10000) {
            cnt_1000_10000 ++;
        }
        else if (tmp_num >= 10000 && tmp_num < 100000) {
            cnt_10000_100000 ++;
        }
        else if (tmp_num >= 100000 && tmp_num < 1000000) {
            cnt_100000_1000000 ++;
        }
        else if (tmp_num >= 1000000 && tmp_num < 10000000) {
            cnt_1000000_10000000 ++;
        }

        // if (recBuf_helpers_num[i]) {
        //     printf("RecBuf [%d] was helped by [%d] threads and contains [%d] nodes \n", i, recBuf_helpers_num[i], tmp_num);
        // }
    }

    // printf("\nThere exist [%d] subtrees with 1-9 nodes\n", cnt_1_10);
    // printf("There exist [%d] subtrees with 10-99 nodes\n", cnt_10_100);
    // printf("There exist [%d] subtrees with 100-999 nodes\n", cnt_100_1000);
    // printf("There exist [%d] subtrees with 1000-9999 nodes\n", cnt_1000_10000);
    // printf("There exist [%d] subtrees with 10000-99999 nodes\n", cnt_10000_100000);
    // printf("There exist [%d] subtrees with 100000-999999 nodes\n", cnt_100000_1000000);
    // printf("There exist [%d] subtrees with 1000000-9999999 nodes\n", cnt_1000000_10000000);

    // compare numbers of time series stored into receive buffers and tree index. They have to be the same!!!
    // printf ("Total series in Tree = [%d] which are [%d] more than total series in RecBufs\n", ts_in_tree_cnt, ts_in_tree_cnt - ts_in_RecBufs_cnt);

    // check that all data series have been processed!
    for (int ts_id=0; ts_id < ts_num; ts_id++) {
        if (!ts_processed[ts_id]) {
            printf("--- ERROR : Time series with id [%d] has not been processed!!!! ---\n", ts_id); fflush(stdout);
        }
    }

    if (parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE) {
        return;
    }

    // check that all iSAX summaries have been iserted into index tree
    unsigned long num_iSAX_processed_from_RecBufs = 0;
    for (int i=0; i < index->fbl->number_of_buffers; i++) {
        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[i];

        if (!current_fbl_node->initialized) {
            continue;
        }

        for (int j=0; j < maxquerythread; j++) {
            for (int k=0; k < current_fbl_node->buffer_size[j]; k++) {
                if(!current_fbl_node->iSAX_processed[j][k]) {
                    printf("--- ERROR : iSAX summary of [%d] recBuf in position ([%d],[%d]) has not been processed!!!! ---", i, j, k); fflush(stdout);
                }
            }
            num_iSAX_processed_from_RecBufs += current_fbl_node->buffer_size[j];
        }

    }

    // printf ("Processed [%d] iSAX summaries from RecBufs\n", num_iSAX_processed_from_RecBufs);
}
    
// reads data from file
// place data in memory
// create the buffers
// void index_creation_pRecBuf_new_botao
void index_creation_pRecBuf_new(const char *ifilename, long int ts_num, isax_index *index)
{
    // fprintf(stderr, ">>> Indexing: %s\n", ifilename);
    FILE * ifile;
    COUNT_INPUT_TIME_START
    ifile = fopen (ifilename,"rb");
    COUNT_INPUT_TIME_END

    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }
    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }
    index->sax_file=NULL;

    long int ts_loaded = 0;
    unsigned long shared_start_number=0;
    int i;
    int node_counter=0;
    pthread_t threadid[maxquerythread];
    buffer_data_inmemory *input_data=malloc(sizeof(buffer_data_inmemory)*(maxquerythread));
    rawfile=malloc(sizeof(ts_type) * index->settings->timeseries_size*ts_num);
    index->sax_cache= malloc(sizeof(sax_type) * index->settings->paa_segments*ts_num);
    pthread_barrier_t lock_barrier1,lock_barrier2;
    pthread_barrier_init(&lock_barrier1, NULL, maxquerythread+1);
    pthread_barrier_init(&lock_barrier2, NULL, maxquerythread+1);
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    COUNT_INPUT_TIME_START
    int read_number=fread(rawfile, sizeof(ts_type), index->settings->timeseries_size*ts_num, ifile);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    pthread_mutex_t lock_record=PTHREAD_MUTEX_INITIALIZER,lockfbl=PTHREAD_MUTEX_INITIALIZER,lock_index=PTHREAD_MUTEX_INITIALIZER,
                    lock_firstnode=PTHREAD_MUTEX_INITIALIZER,lock_disk=PTHREAD_MUTEX_INITIALIZER;

    destroy_fbl(index->fbl);
    index->fbl = (struct first_buffer_layer *)initialize_pRecBuf(index->settings->initial_fbl_buffer_size,
                                pow(2, index->settings->paa_segments),
                                index->settings->max_total_buffer_size+DISK_BUFFER_SIZE*(PROGRESS_CALCULATE_THREAD_NUMBER-1), index);
    // set the thread on decided cpu


    // COUNT_OUTPUT_TIME_START
    int nodeid[index->fbl->number_of_buffers];
    int nodesize[index->fbl->number_of_buffers];

    for ( i = 0; i < maxquerythread; i++)
    {
        input_data[i].index=index;
        input_data[i].lock_fbl=&lockfbl;
        input_data[i].lock_record=&lock_record;
        input_data[i].lock_firstnode =&lock_firstnode;
        input_data[i].lock_index=&lock_index;
        input_data[i].ts=rawfile;
        input_data[i].lock_disk=&lock_disk;
        input_data[i].workernumber=i;
        input_data[i].total_workernumber=maxquerythread;
        input_data[i].start_number=i*(ts_num/maxquerythread);
        input_data[i].shared_start_number=&shared_start_number;
        input_data[i].stop_number=ts_num;
        input_data[i].node_counter=&node_counter;
        input_data[i].lock_barrier1=&lock_barrier1;
        input_data[i].lock_barrier2=&lock_barrier2;
        input_data[i].nodeid=nodeid;
    }
    for (i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]),NULL,index_creation_pRecBuf_worker_new,(void*)&(input_data[i]));
    }

    pthread_barrier_wait(&lock_barrier1);

    //wait for the finish of other threads
    for (i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i],NULL);
    }
    __sync_fetch_and_add(&(index->total_records),ts_num);
    index->sax_cache_size=index->total_records;
    fclose(ifile);
    // fprintf(stderr, ">>> Finished indexing\n");
    free(input_data);
        //printf(" the sax point is %d\n",index->first_node->isax_cardinalities[0]);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END

    check_validity(index, ts_num);
}
void index_creation_pRecBuf_new_ekosmas_func(const char *ifilename, long int ts_num, isax_index *index, char embarrassingly_parallel, const char parallelism_in_subtree)
{
    // A. open input file and check its validity
    FILE * ifile;
    ifile = fopen (ifilename,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);                                                             
    file_position_type sz = (file_position_type) ftell(ifile);                              // sz = size in bytes
    file_position_type total_records = sz/index->settings->ts_byte_size;                    // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num) {                                                           // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);                                    
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size*ts_num);                                    
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size*ts_num, ifile);
    COUNT_INPUT_TIME_END


    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    index->fbl = (first_buffer_layer *) initialize_pRecBuf_ekosmas(
                                index->settings->initial_fbl_buffer_size,
                                pow(2, index->settings->paa_segments),
                                index->settings->max_total_buffer_size+DISK_BUFFER_SIZE*(PROGRESS_CALCULATE_THREAD_NUMBER-1), index);
    
    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                             // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas)*(maxquerythread));       // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    int node_counter = 0;                                                       // required for tree construction using fai

    volatile unsigned long *next_iSAX_group = calloc(index->fbl->max_total_size, sizeof(unsigned long));        

    pthread_barrier_t wait_summaries_to_compute;                                // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index                     = index;
        input_data[i].lock_firstnode            = &lock_firstnode;                  // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber              = i;
        input_data[i].shared_start_number       = &next_block_to_process;
        input_data[i].ts_num                    = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter              = &node_counter;                    // required for tree construction using fai
        input_data[i].parallelism_in_subtree    = parallelism_in_subtree;
        input_data[i].next_iSAX_group           = next_iSAX_group;                  
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {
        if (!embarrassingly_parallel){
            pthread_create(&(threadid[i]),NULL,index_creation_pRecBuf_worker_new_ekosmas,(void*)&(input_data[i]));
        }
        else {
            pthread_create(&(threadid[i]),NULL,index_creation_pRecBuf_worker_new_ekosmas_EP,(void*)&(input_data[i]));   
        }
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i],NULL);
    }

    free(input_data);
    fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END

    check_validity_ekosmas(index, ts_num);    
}
void index_creation_pRecBuf_new_ekosmas(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_func(ifilename, ts_num, index, 0, NO_PARALLELISM_IN_SUBTREE);
}
void index_creation_pRecBuf_new_ekosmas_MESSI_with_enhanced_blocking_parallelism(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_func(ifilename, ts_num, index, 0, BLOCKING_PARALLELISM_IN_SUBTREE);
}
void index_creation_pRecBuf_new_ekosmas_EP(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_func(ifilename, ts_num, index, 1, NO_PARALLELISM_IN_SUBTREE);
}


void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree)
{
    // A. open input file and check its validity
    // ------------------------------------------------------------------
    FILE * ifile;
    ifile = fopen (ifilename,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);                                                             
    file_position_type sz = (file_position_type) ftell(ifile);                              // sz = size in bytes
    file_position_type total_records = sz/index->settings->ts_byte_size;                    // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num) {                                                           // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }
    // ------------------------------------------------------------------

    // B. Read file in memory (into the "rawfile" array)
    // ------------------------------------------------------------------
    index->settings->raw_filename = malloc(256);                                    
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size*ts_num);                                    
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size*ts_num, ifile);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    index->fbl = (first_buffer_layer *) initialize_pRecBuf_ekosmas_lf(
                                index->settings->initial_fbl_buffer_size,
                                pow(2, index->settings->paa_segments),
                                index->settings->max_total_buffer_size+DISK_BUFFER_SIZE*(PROGRESS_CALCULATE_THREAD_NUMBER-1), index);
    
    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    // ------------------------------------------------------------------
    int nodeid[index->fbl->number_of_buffers];                                                      
    pthread_t threadid[maxquerythread];                                                             // thread's id array
    buffer_data_inmemory_ekosmas_lf *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas_lf)*(maxquerythread));       // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    int node_counter = 0;                                                       // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute;                                // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);     

    unsigned long total_blocks = ts_num/read_block_length;
    if (read_block_length*total_blocks < ts_num) {
        total_blocks++;
    }

    block_processed = malloc(sizeof(unsigned char)*total_blocks);
    group_processed = malloc(sizeof(unsigned char *)*total_blocks);
    // next_ts_group_read_in_block = malloc(sizeof(unsigned long)*total_blocks);
    next_ts_group_read_in_block = malloc(sizeof(next_ts_group)*total_blocks);
    next_ts_read_in_group = malloc(sizeof(next_ts_in_group *)*total_blocks);
    next_ts_read_in_group_fai = malloc(sizeof(next_ts_in_group *)*total_blocks);
    block_helper_exist = malloc(sizeof(unsigned char)*total_blocks);
    group_helpers_exist = malloc(sizeof(unsigned char *)*total_blocks);
    // block_helpers_num = malloc(sizeof(unsigned char)*total_blocks);
    for (int i=0; i< total_blocks; i++) {
        block_processed[i] = 0;
        group_processed[i] = calloc(ts_group_length, sizeof(unsigned char));
        next_ts_group_read_in_block[i].num = 0;
        next_ts_read_in_group[i] = calloc(ts_group_length, sizeof(next_ts_in_group));
        next_ts_read_in_group_fai[i] = calloc(ts_group_length, sizeof(next_ts_in_group));
        block_helper_exist[i] = 0;
        group_helpers_exist[i] = calloc(ts_group_length, sizeof(unsigned char));
        // block_helpers_num[i] = 0;
    }

    recBuf_helpers_num = malloc(sizeof(unsigned char)*index->fbl->number_of_buffers);
    for (int i=0; i< index->fbl->number_of_buffers; i++) {
        recBuf_helpers_num[i] = 0;
    }    

    ts_processed = malloc(sizeof(unsigned char)*ts_num);
    for (int i=0; i< ts_num; i++) {
        ts_processed[i] = 0;
    }
       
    all_blocks_processed = 0;
    all_RecBufs_processed = 0;

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index                     = index;
        input_data[i].workernumber              = i;
        input_data[i].shared_start_number       = &next_block_to_process;
        input_data[i].ts_num                    = ts_num;
        input_data[i].parallelism_in_subtree    = parallelism_in_subtree;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;       
        input_data[i].node_counter              = &node_counter;                    // required for tree construction using fai
        input_data[i].sax                       = (void *)memalign(CACHE_LINE_SIZE, index->settings->sax_byte_size);
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]),NULL,index_creation_pRecBuf_worker_new_ekosmas_lock_free_full_fai,(void*)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i],NULL);
    }
    // ------------------------------------------------------------------

    free(input_data);
    fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    
    check_validity_ekosmas_lf(index, ts_num, parallelism_in_subtree);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, NO_PARALLELISM_IN_SUBTREE);
}

void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_blocking_parallelism_in_subtree(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, BLOCKING_PARALLELISM_IN_SUBTREE);
}

void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_cow(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_COW);
}


void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree)
{
    // A. open input file and check its validity
    // ------------------------------------------------------------------
    FILE * ifile;
    ifile = fopen (ifilename,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);                                                             
    file_position_type sz = (file_position_type) ftell(ifile);                              // sz = size in bytes
    file_position_type total_records = sz/index->settings->ts_byte_size;                    // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num) {                                                           // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }
    // ------------------------------------------------------------------

    // B. Read file in memory (into the "rawfile" array)
    // ------------------------------------------------------------------
    index->settings->raw_filename = malloc(256);                                    
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size*ts_num);                                    
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size*ts_num, ifile);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    index->fbl = (first_buffer_layer *) initialize_pRecBuf_ekosmas_lf(
                                index->settings->initial_fbl_buffer_size,
                                pow(2, index->settings->paa_segments),
                                index->settings->max_total_buffer_size+DISK_BUFFER_SIZE*(PROGRESS_CALCULATE_THREAD_NUMBER-1), index);
    
    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    // ------------------------------------------------------------------
    int nodeid[index->fbl->number_of_buffers];                                                      // not used!
    pthread_t threadid[maxquerythread];                                                             // thread's id array
    buffer_data_inmemory_ekosmas_lf *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas_lf)*(maxquerythread));       // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    int node_counter = 0;                                                       // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute;                                // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    unsigned long total_blocks = ts_num/read_block_length;
    if (read_block_length*total_blocks < ts_num) {
        total_blocks++;
    }

    block_processed = malloc(sizeof(unsigned char)*total_blocks);
    group_processed = malloc(sizeof(unsigned char *)*total_blocks);
    // next_ts_group_read_in_block = malloc(sizeof(unsigned long)*total_blocks);
    next_ts_group_read_in_block = malloc(sizeof(next_ts_group)*total_blocks);
    next_ts_read_in_group = malloc(sizeof(next_ts_in_group *)*total_blocks);
    next_ts_read_in_group_fai = malloc(sizeof(next_ts_in_group *)*total_blocks);
    block_helper_exist = malloc(sizeof(unsigned char)*total_blocks);
    group_helpers_exist = malloc(sizeof(unsigned char *)*total_blocks);
    // block_helpers_num = malloc(sizeof(unsigned char)*total_blocks);
    for (int i=0; i< total_blocks; i++) {
        block_processed[i] = 0;
        group_processed[i] = calloc(ts_group_length, sizeof(unsigned char));
        next_ts_group_read_in_block[i].num = 0;
        next_ts_read_in_group[i] = calloc(ts_group_length, sizeof(next_ts_in_group));
        next_ts_read_in_group_fai[i] = calloc(ts_group_length, sizeof(next_ts_in_group));
        block_helper_exist[i] = 0;
        group_helpers_exist[i] = calloc(ts_group_length, sizeof(unsigned char));
        // block_helpers_num[i] = 0;
    }

    recBuf_helpers_num = malloc(sizeof(unsigned char)*index->fbl->number_of_buffers);
    for (int i=0; i< index->fbl->number_of_buffers; i++) {
        recBuf_helpers_num[i] = 0;
    }      

    ts_processed = malloc(sizeof(unsigned char)*ts_num);
    for (int i=0; i< ts_num; i++) {
        ts_processed[i] = 0;
    }
    
    all_blocks_processed = 0;
    all_RecBufs_processed = 0;

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index                     = index;      
        input_data[i].workernumber              = i;
        input_data[i].shared_start_number       = &next_block_to_process;
        input_data[i].ts_num                    = ts_num;                           
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].parallelism_in_subtree    = parallelism_in_subtree;
        input_data[i].node_counter              = &node_counter;                    // required for tree construction using fai
        input_data[i].sax                       = (void *)memalign(CACHE_LINE_SIZE, index->settings->sax_byte_size);
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]),NULL,index_creation_pRecBuf_worker_new_ekosmas_lock_free_fai_only_after_help,(void*)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i],NULL);
    }
    // ------------------------------------------------------------------

    free(input_data);
    fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END


    check_validity_ekosmas_lf(index, ts_num, parallelism_in_subtree);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, NO_PARALLELISM_IN_SUBTREE);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_blocking_parallelism_in_subtree(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, BLOCKING_PARALLELISM_IN_SUBTREE);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_cow(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_COW);
}




inline void scan_RecBuf_for_unprocessed_iSAX_summaries(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, volatile unsigned char *stop, unsigned long my_id, const char is_helper, const char lockfree_parallelism_in_subtree) 
{
    for (int k = 0; k < maxquerythread; k++) {
        for (unsigned long i = 0; i < current_fbl_node->buffer_size[k] && !(*stop) ; i++)
        {
            if (!current_fbl_node->iSAX_processed[k][i]) {

                r->sax = (sax_type *) &(((current_fbl_node->sax_records[k]))[i*index->settings->paa_segments]);
                r->position = (file_position_type *) &((file_position_type *)(current_fbl_node->pos_records[k]))[i];
                r->insertion_mode = NO_TMP | PARTIAL;

                if (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
                    lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP ||
                    lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF) {
                    add_record_to_node_inmemory_parallel_lockfree_announce(index, current_fbl_node, r, my_id, maxquerythread, is_helper, lockfree_parallelism_in_subtree);
                }
                else if (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW) {
                    add_record_to_node_inmemory_parallel_lockfree_cow(index, current_fbl_node, r, my_id);
                }
                else { 
                    add_record_to_node_inmemory_parallel_locks(index, current_fbl_node->node, r);
                }

                if (!current_fbl_node->iSAX_processed[k][i]) {
                    current_fbl_node->iSAX_processed[k][i] = 1;
                }  
            }
        }
    }
}
unsigned long populate_tree_lock_free_announce(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id, const char is_helper, const char lockfree_parallelism_in_subtree) 
{
    isax_node *root_node = NULL;

    // create and initialize a new fbl leaf node
    if (!current_fbl_node->node) {
        root_node = isax_root_node_init_lockfree_announce(current_fbl_node->mask, index->settings->initial_leaf_buffer_size, maxquerythread, lockfree_parallelism_in_subtree, current_fbl_node);
    }

    // try to establish root node
    if (root_node != NULL && (current_fbl_node->node || !CASPTR(&current_fbl_node->node, NULL, root_node))) {
        // free memory
        isax_tree_destroy_lockfree(root_node);
    }

    root_node = current_fbl_node->node;

    // populate tree
    unsigned long subtree_nodes = 0;
    unsigned long iSAX_group = 0;
    unsigned long prev_iSAX_group_id = 0;
    unsigned int process_recBuf_id = 0;
    unsigned int prev_recBuf_iSAX_num = 0;
    char helpers_exist = 0;

    unsigned long num_added = 0;

    while (!current_fbl_node->processed) {
        prev_iSAX_group_id = iSAX_group;

        if (lockfree_parallelism_in_subtree != LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE && !current_fbl_node->recBuf_helpers_exist) {
            iSAX_group = current_fbl_node->next_iSAX_group;
            current_fbl_node->next_iSAX_group = iSAX_group+1;               
        }
        else {
            iSAX_group = __sync_fetch_and_add(&(current_fbl_node->next_iSAX_group),1);
        }
        
        #ifdef DEBUG
        if (iSAX_group && iSAX_group <= prev_iSAX_group_id) {                                                   
            printf ("\nCAUTION: populate_tree_lock_free_announce: Counter went back!!\n\n"); fflush(stdout);
            getchar();
        }
        #endif

        while (process_recBuf_id < maxquerythread && iSAX_group >= current_fbl_node->buffer_size[process_recBuf_id] + prev_recBuf_iSAX_num) {
            prev_recBuf_iSAX_num += current_fbl_node->buffer_size[process_recBuf_id];
            process_recBuf_id++;
        }

        if (process_recBuf_id == maxquerythread) {
            break;
        }

        if (!helpers_exist && iSAX_group > prev_iSAX_group_id + 1) {                                            // performance enhancement
            helpers_exist = 1;                                                      
        }

        int k = process_recBuf_id;

        #ifdef DEBUG
        if (k<0) {                                                                                              
            printf("ERRROR!!! - k equals [%d] but it can not be negative!\n", k); fflush(stdout);
            getchar();
        }
        #endif

        int i = iSAX_group - prev_recBuf_iSAX_num;
        r->sax = (sax_type *) &(((current_fbl_node->sax_records[k]))[i*index->settings->paa_segments]);
        r->position = (file_position_type *) &((file_position_type *)(current_fbl_node->pos_records[k]))[i];
        r->insertion_mode = NO_TMP | PARTIAL;

        // Add record to index
        add_record_to_node_inmemory_parallel_lockfree_announce(index, current_fbl_node, r, my_id, maxquerythread, is_helper, lockfree_parallelism_in_subtree);

        if (!current_fbl_node->iSAX_processed[k][i]) {
            current_fbl_node->iSAX_processed[k][i] = 1;
        }

        subtree_nodes++;     
    }

    if ((is_helper || helpers_exist) && !current_fbl_node->processed) {                    // performance enhancement
        scan_RecBuf_for_unprocessed_iSAX_summaries (index, current_fbl_node, r, &current_fbl_node->processed, my_id, is_helper, lockfree_parallelism_in_subtree);
    }

    if (!current_fbl_node->processed) {
        current_fbl_node->processed = 1;
    }

    return subtree_nodes;
}

unsigned long populate_tree_lock_free_cow(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id, const char is_helper) 
{
    isax_node *root_node = NULL;

    // create and initialize a new fbl leaf node
    if (!current_fbl_node->node) {
        root_node = isax_root_node_init_lockfree_cow(current_fbl_node->mask, index->settings->initial_leaf_buffer_size);
    }

    // try to establish root node
    if (root_node != NULL && (current_fbl_node->node || !CASPTR(&current_fbl_node->node, NULL, root_node))) {
        // free memory
        isax_tree_destroy_lockfree(root_node);
    }

    root_node = current_fbl_node->node;



    // populate tree
    unsigned long subtree_nodes = 0;
    unsigned long iSAX_group;
    unsigned long prev_iSAX_group_id = 0;
    unsigned int process_recBuf_id = 0;
    unsigned int prev_recBuf_iSAX_num = 0;
    char helpers_exist = 0;

    unsigned long num_added = 0;

    while (!current_fbl_node->processed) {
        iSAX_group = __sync_fetch_and_add(&(current_fbl_node->next_iSAX_group),1);

        while (process_recBuf_id < maxquerythread && iSAX_group >= current_fbl_node->buffer_size[process_recBuf_id] + prev_recBuf_iSAX_num) {
            prev_recBuf_iSAX_num += current_fbl_node->buffer_size[process_recBuf_id];
            process_recBuf_id++;
        }

        if (iSAX_group > prev_iSAX_group_id + 1) {                                         // performance enhancement
            helpers_exist = 1;                                                      
        }

        if (process_recBuf_id == maxquerythread) {
            break;
        }

        int k = process_recBuf_id;
        if (k<0) {
            printf("ERRROR!!! - k equals [%d] but it can not be negative!\n", k);
            getchar();
        }
        int i = iSAX_group - prev_recBuf_iSAX_num;

        r->sax = (sax_type *) &(((current_fbl_node->sax_records[k]))[i*index->settings->paa_segments]);
        r->position = (file_position_type *) &((file_position_type *)(current_fbl_node->pos_records[k]))[i];
        r->insertion_mode = NO_TMP | PARTIAL;

        // Add record to index
        // printf("add record...\n"); fflush(stdout);
        add_record_to_node_inmemory_parallel_lockfree_cow(index, current_fbl_node, r, my_id);
        // printf("add record - DONE...\n"); fflush(stdout);

        if (!current_fbl_node->iSAX_processed[k][i]) {
            current_fbl_node->iSAX_processed[k][i] = 1;
        }

        subtree_nodes++;     
        prev_iSAX_group_id = iSAX_group;
    }

    if ((is_helper || helpers_exist) && !current_fbl_node->processed) {                    // performance enhancement
        scan_RecBuf_for_unprocessed_iSAX_summaries (index, current_fbl_node, r, &current_fbl_node->processed, my_id, is_helper, LOCKFREE_PARALLELISM_IN_SUBTREE_COW);
    }

    if (!current_fbl_node->processed) {
        current_fbl_node->processed = 1;
    }

    return subtree_nodes;
}
unsigned long populate_tree_with_locks(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id, const char is_helper) 
{
    isax_node *root_node = NULL;

    // create and initialize a new fbl leaf node
    if (!current_fbl_node->node) {
        root_node = isax_root_node_init(current_fbl_node->mask, index->settings->initial_leaf_buffer_size, NULL);
        root_node->is_leaf = 1;
        root_node->lock_node = malloc (sizeof(pthread_mutex_t));
        pthread_mutex_init (root_node->lock_node, NULL);
    }

    // try to establish root node
    if (root_node != NULL && (current_fbl_node->node || !CASPTR(&current_fbl_node->node, NULL, root_node))) {
        // printf("ERROR!!! NOTHING SHOULD BE FREE!!!\n");fflush(stdout);
        // getchar();
        // free memory
        pthread_mutex_destroy(root_node->lock_node);
        isax_tree_destroy_lockfree(root_node);
    }

    root_node = current_fbl_node->node;

    // populate tree
    unsigned long subtree_nodes = 0;
    unsigned long iSAX_group;
    unsigned long prev_iSAX_group_id = 0;
    unsigned int process_recBuf_id = 0;
    unsigned int prev_recBuf_iSAX_num = 0;
    char helpers_exist = 0;

    while (!current_fbl_node->processed) {
        iSAX_group = __sync_fetch_and_add(&(current_fbl_node->next_iSAX_group),1);

        while (process_recBuf_id < maxquerythread && iSAX_group >= current_fbl_node->buffer_size[process_recBuf_id] + prev_recBuf_iSAX_num) {
            prev_recBuf_iSAX_num += current_fbl_node->buffer_size[process_recBuf_id];
            process_recBuf_id++;
        }

        if (iSAX_group > prev_iSAX_group_id + 1) {                                         // performance enhancement
            helpers_exist = 1;                                                      
        }

        if (process_recBuf_id == maxquerythread) {
            break;
        }

        int k = process_recBuf_id;
        int i = iSAX_group - prev_recBuf_iSAX_num;

        r->sax = (sax_type *) &(((current_fbl_node->sax_records[k]))[i*index->settings->paa_segments]);
        r->position = (file_position_type *) &((file_position_type *)(current_fbl_node->pos_records[k]))[i];
        r->insertion_mode = NO_TMP | PARTIAL;

        // Add record to index
        add_record_to_node_inmemory_parallel_locks(index, root_node, r);

        if (!current_fbl_node->iSAX_processed[k][i]) {
            current_fbl_node->iSAX_processed[k][i] = 1;
        }

        subtree_nodes++;     
        prev_iSAX_group_id = iSAX_group;
    }

    if ((is_helper || helpers_exist) && !current_fbl_node->processed) {                    // performance enhancement
        scan_RecBuf_for_unprocessed_iSAX_summaries (index, current_fbl_node, r, &current_fbl_node->processed, my_id, is_helper, BLOCKING_PARALLELISM_IN_SUBTREE);
    }

    if (!current_fbl_node->processed) {
        current_fbl_node->processed = 1;
    }

    return subtree_nodes;
}
inline unsigned long populate_tree_copy_and_establish_lock_free(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id) 
{
    // create and initialize a new fbl leaf node
    isax_node *root_node = isax_root_node_init(current_fbl_node->mask, index->settings->initial_leaf_buffer_size, current_fbl_node);
    root_node->is_leaf = 1;

    // populate tree
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !current_fbl_node->node; k++)
    {
        for (int i=0; i<current_fbl_node->buffer_size[k] && !current_fbl_node->node; i++)
        {
            r->sax = (sax_type *) &(((current_fbl_node->sax_records[k]))[i*index->settings->paa_segments]);
            r->position = (file_position_type *) &((file_position_type *)(current_fbl_node->pos_records[k]))[i];
            r->insertion_mode = NO_TMP | PARTIAL;
            // Add record to index
            add_record_to_node_inmemory(index, root_node, r, 1);
        }

        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    // try to establish tree copy
    if (current_fbl_node->node || !CASPTR(&current_fbl_node->node, NULL, root_node)) {
        // free memory
        isax_tree_destroy_lockfree(root_node);
    }

    return subtree_nodes;
}
inline unsigned long count_nodes_in_RecBuf_for_subtree_copy(parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node * volatile *stop) 
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !(*stop); k++) {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}
inline unsigned long count_nodes_in_RecBuf_for_subtree_parallel(parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, volatile unsigned char *stop) 
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !(*stop); k++) {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}
static inline void scan_for_unprocessed_RecBufs(isax_index *index, isax_node_record *r, unsigned long my_id, const char parallelism_in_subtree) 
{
    unsigned long backoff_time = backoff_multiplier;

    if (my_num_subtree_construction) {
        backoff_time *= (unsigned long) BACKOFF_SUBTREE_DELAY_PER_NODE;
    }
    else {
        backoff_time = 0;
    }

    for (int i=0; i < index->fbl->number_of_buffers && !all_RecBufs_processed; i++) {

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[i];

        if (!current_fbl_node->initialized || 
            (parallelism_in_subtree != NO_PARALLELISM_IN_SUBTREE && current_fbl_node->processed) || 
            (parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE && current_fbl_node->node)) {
            continue;
        }

        unsigned long num_nodes; 

        // UNCOMMENT THE FOLLOWING TO DISABLE BACKOFF
        if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF ||
            parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP ||
            parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {       //  do not backoff in this case
            ;   
        }
        else if (parallelism_in_subtree != NO_PARALLELISM_IN_SUBTREE) {
            num_nodes = count_nodes_in_RecBuf_for_subtree_parallel(current_fbl_node, &current_fbl_node->processed);
            backoff_delay_lockfree_subtree_parallel(backoff_time*num_nodes, &current_fbl_node->processed);
        }
        // EKOSMAS: COMMENT OUT THE FOLLOWING TO DISABLE BACKOFF
        // else {
        //     num_nodes = count_nodes_in_RecBuf_for_subtree_copy(current_fbl_node, &current_fbl_node->node);
        //     backoff_delay_lockfree_subtree_copy(backoff_time*num_nodes, &current_fbl_node->node);
        // }

        if ((parallelism_in_subtree != NO_PARALLELISM_IN_SUBTREE && current_fbl_node->processed) || 
            (parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE && current_fbl_node->node)) {
            recBufs_helping_avoided_cnt++;
            continue;
        }

        recBufs_helped_cnt++;

        if (parallelism_in_subtree == BLOCKING_PARALLELISM_IN_SUBTREE){
            populate_tree_with_locks (index, current_fbl_node, r, my_id, 1);
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {
            populate_tree_lock_free_announce (index, current_fbl_node, r, my_id, 1, parallelism_in_subtree);
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP ||
                 parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF){
            // EKOSMAS: This should be like that, but current_fbl_node->recBuf_helpers_exist is also used to acquire
            // iSAX_groups by executing FAI when helpers arrive in the receive buffer. So, for LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP
            // this flag is also used to follow expeditive/standard mode during insertion in the subtree.
            // if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && !current_fbl_node->recBuf_helpers_exist) {
            if (!current_fbl_node->recBuf_helpers_exist) {
                current_fbl_node->recBuf_helpers_exist = 1;
            }

            populate_tree_lock_free_announce (index, current_fbl_node, r, my_id, 1, parallelism_in_subtree);
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW){
            populate_tree_lock_free_cow (index, current_fbl_node, r, my_id, 1);
        }
        else{ // parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE 
            populate_tree_copy_and_establish_lock_free (index, current_fbl_node, r, my_id);
        }
    }

    if (!all_RecBufs_processed) {
        all_RecBufs_processed = 1;
    }

    if (recBufs_helping_avoided_cnt) {
        COUNT_SUBTREE_HELP_AVOIDED(recBufs_helping_avoided_cnt)
    }    
    if (recBufs_helped_cnt) {
        COUNT_SUBTREES_HELPED(recBufs_helped_cnt)
    }    
}
static inline void tree_index_creation_from_pRecBuf_fai_lock_free(void *transferdata, const char parallelism_in_subtree)
{
    buffer_data_inmemory_ekosmas_lf *input_data = (buffer_data_inmemory_ekosmas_lf*) transferdata;
    isax_index *index = input_data->index;
    int j;
    
    isax_node_record *r = malloc(sizeof(isax_node_record));

    while(!all_RecBufs_processed)
    {
        // acquire the next receive buffer
        j = __sync_fetch_and_add(input_data->node_counter,1);
        if( j >= index->fbl->number_of_buffers)
        {
            break;
        }
        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[j];
        
        // check if the acquired receive buffer contains elements
        if (!current_fbl_node->initialized) {
            continue;
        }

        COUNT_MY_TIME_START
        if (parallelism_in_subtree == BLOCKING_PARALLELISM_IN_SUBTREE){
            my_num_subtree_nodes += populate_tree_with_locks (index, current_fbl_node, r, input_data->workernumber, 0);
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
                 parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP ||
                 parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF) {
            my_num_subtree_nodes += populate_tree_lock_free_announce (index, current_fbl_node, r, input_data->workernumber, 0, parallelism_in_subtree);
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW){
            my_num_subtree_nodes += populate_tree_lock_free_cow (index, current_fbl_node, r, input_data->workernumber, 0);
        }
        else{ // parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE
            my_num_subtree_nodes += populate_tree_copy_and_establish_lock_free (index, current_fbl_node, r, input_data->workernumber);
        }
        COUNT_MY_TIME_FOR_SUBTREE_END
        my_num_subtree_construction++;
    }

    if (!DO_NOT_HELP) {     // there is no need for a barrier here, since query answering threads will be spawn only after all index worker threads finish
        scan_for_unprocessed_RecBufs(index, r, input_data->workernumber, parallelism_in_subtree);
    }
    
}

inline void tree_index_creation_from_pRecBuf_fai_blocking(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas*) transferdata;
    isax_index *index = input_data->index;
    int j;
    bool has_record;
    isax_node_record *r = malloc(sizeof(isax_node_record));

    while(1)
    {
    
        j = __sync_fetch_and_add(input_data->node_counter,1);
        if( j >= index->fbl->number_of_buffers)
        {
            break;
        }

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas*)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }
    
        for (int k = 0; k < maxquerythread; k++)
        {

            for (int i=0; i<current_fbl_node->buffer_size[k]; i++)
            {
                r->sax = (sax_type *) &(((current_fbl_node->sax_records[k]))[i*index->settings->paa_segments]);
                r->position = (file_position_type *) &((file_position_type *)(current_fbl_node->pos_records[k]))[i];
                r->insertion_mode = NO_TMP | PARTIAL;
                // Add record to index
                add_record_to_node_inmemory(index, (isax_node *) current_fbl_node->node, r, 1);
            }
        }   
    }

    free(r);
}
inline unsigned long populate_tree_with_locks_blocking_with_parallelism(buffer_data_inmemory_ekosmas *input_data, int j, parallel_fbl_soft_buffer_ekosmas *current_fbl_node, isax_node_record *r, unsigned long my_id) 
{
    isax_index *index = input_data->index;

    isax_node *root_node = current_fbl_node->node;
    pthread_mutex_t *tmp_lock_node = NULL;


    // create and initialize a new lock_node
    if (!root_node->lock_node) {
        tmp_lock_node = malloc (sizeof(pthread_mutex_t));
        pthread_mutex_init (tmp_lock_node, NULL);
    }

    // try to establish new lock_node
    if (tmp_lock_node != NULL && (root_node->lock_node || !CASPTR(&root_node->lock_node, NULL, tmp_lock_node))) {
        // free memory
        pthread_mutex_destroy(tmp_lock_node);
    }



    // populate tree
    unsigned long subtree_nodes = 0;
    unsigned long iSAX_group;
    unsigned int process_recBuf_id = 0;
    unsigned int prev_recBuf_iSAX_num = 0;

    while (!current_fbl_node->finished) {
        iSAX_group = __sync_fetch_and_add(&(input_data->next_iSAX_group[j]),1);

        while (process_recBuf_id < maxquerythread && iSAX_group >= current_fbl_node->buffer_size[process_recBuf_id] + prev_recBuf_iSAX_num) {
            prev_recBuf_iSAX_num += current_fbl_node->buffer_size[process_recBuf_id];
            process_recBuf_id++;
        }

        if (process_recBuf_id == maxquerythread) {
            break;
        }

        int k = process_recBuf_id;
        int i = iSAX_group - prev_recBuf_iSAX_num;

        r->sax = (sax_type *) &(((current_fbl_node->sax_records[k]))[i*index->settings->paa_segments]);
        r->position = (file_position_type *) &((file_position_type *)(current_fbl_node->pos_records[k]))[i];
        r->insertion_mode = NO_TMP | PARTIAL;

        // Add record to index
        add_record_to_node_inmemory_parallel_locks(index, root_node, r);

        subtree_nodes++;     
    }

    if (!current_fbl_node->finished) {
        current_fbl_node->finished = 1;
    }

    return subtree_nodes;
}
inline void backoff_delay_lockfree_subtree_parallel_blocking_with_parallelism(unsigned long backoff, volatile int *stop) {
    if (!backoff){
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}
inline unsigned long count_nodes_in_RecBuf_for_subtree_parallel_blocking_with_parallelism(parallel_fbl_soft_buffer_ekosmas *current_fbl_node, volatile int *stop) 
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !(*stop); k++) {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}
static inline void scan_for_unprocessed_RecBufs_blocking_with_parallelism(buffer_data_inmemory_ekosmas *input_data, isax_node_record *r, unsigned long my_id) 
{

    isax_index *index = input_data->index;

    unsigned long backoff_time = backoff_multiplier;

    if (my_num_subtree_construction) {
        backoff_time *= (unsigned long) BACKOFF_SUBTREE_DELAY_PER_NODE;
    }
    else {
        backoff_time = 0;
    }

    for (int i=0; i < index->fbl->number_of_buffers && !all_RecBufs_processed; i++) {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas*)(index->fbl))->soft_buffers[i];

        if (!current_fbl_node->initialized || current_fbl_node->finished) {
            continue;
        }

        unsigned long num_nodes = count_nodes_in_RecBuf_for_subtree_parallel_blocking_with_parallelism(current_fbl_node, &current_fbl_node->finished);
        backoff_delay_lockfree_subtree_parallel_blocking_with_parallelism(backoff_time*num_nodes, &current_fbl_node->finished);

        if (current_fbl_node->finished) {
            recBufs_helping_avoided_cnt++;
            continue;
        }

        recBufs_helped_cnt++;

        populate_tree_with_locks_blocking_with_parallelism(input_data, i, current_fbl_node, r, my_id);
    }

    if (!all_RecBufs_processed) {
        all_RecBufs_processed = 1;
    }

    if (recBufs_helping_avoided_cnt) {
        COUNT_SUBTREE_HELP_AVOIDED(recBufs_helping_avoided_cnt)
    }    

    if (recBufs_helped_cnt) {
        COUNT_SUBTREES_HELPED(recBufs_helped_cnt)
    }    
}
static inline void tree_index_creation_from_pRecBuf_fai_blocking_with_parallelism(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas*) transferdata;
    isax_index *index = input_data->index;
    int j;
    bool has_record;
    isax_node_record *r = malloc(sizeof(isax_node_record));

    while(1)
    {
        j = __sync_fetch_and_add(input_data->node_counter,1);
        if( j >= index->fbl->number_of_buffers) {
            break;
        }

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas*)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }
    
        COUNT_MY_TIME_START
        populate_tree_with_locks_blocking_with_parallelism(input_data, j, current_fbl_node, r, input_data->workernumber);
        COUNT_MY_TIME_FOR_SUBTREE_END
        my_num_subtree_construction++;
    }

    if (!DO_NOT_HELP) {     // there is no need for a barrier here, since query answering threads will be spawn only after all index worker threads finish
        scan_for_unprocessed_RecBufs_blocking_with_parallelism(input_data, r, input_data->workernumber);    
    }

    free(r);
}

// void* index_creation_pRecBuf_worker_new_botao
void* index_creation_pRecBuf_worker_new(void *transferdata)
{

    buffer_data_inmemory *input_data = (buffer_data_inmemory*) transferdata;
    threadPin(input_data->workernumber, maxquerythread);

    sax_type * sax = malloc(sizeof(sax_type) * ((buffer_data_inmemory*)transferdata)->index->settings->paa_segments);
    //struct timeval workertimestart;
    //struct timeval writetiemstart;
    //struct timeval workercurenttime;
    //struct timeval writecurenttime;
    //double worker_total_time,tee,tss;
    //gettimeofday(&workertimestart, NULL);
    unsigned long roundfinishednumber;

    unsigned long start_number;
    unsigned long stop_number=((buffer_data_inmemory*)transferdata)->stop_number;
    file_position_type *pos = malloc(sizeof(file_position_type));
    isax_index *index= ((buffer_data_inmemory*)transferdata)->index;
    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    int paa_segments=((buffer_data_inmemory*)transferdata)->index->settings->paa_segments;

    unsigned long i=0;
    float *raw_file=((buffer_data_inmemory*)transferdata)->ts;
    while(1)
    {
        start_number=__sync_fetch_and_add(((buffer_data_inmemory*)transferdata)->shared_start_number,read_block_length);
        if(start_number>stop_number)
        {
            break;
        }
        else if(start_number>stop_number-read_block_length)
        {
            roundfinishednumber=stop_number;
        }
        else
        {
            roundfinishednumber=start_number+read_block_length;
        }
         for (i=start_number;i<roundfinishednumber;i++)
         {
            memcpy(ts,&(raw_file[i*index->settings->timeseries_size]), sizeof(float)* index->settings->timeseries_size);
            if(sax_from_ts(ts, sax, index->settings->ts_values_per_paa_segment,
                       index->settings->paa_segments, index->settings->sax_alphabet_cardinality,
                       index->settings->sax_bit_cardinality) == SUCCESS)
            {
                *pos = (file_position_type)(i*index->settings->timeseries_size);
                memcpy(&(index->sax_cache[i*index->settings->paa_segments]),sax, sizeof(sax_type)* index->settings->paa_segments);

                isax_pRecBuf_index_insert_inmemory(index, sax, pos, ((buffer_data_inmemory*)transferdata)->lock_firstnode,((buffer_data_inmemory*)transferdata)->workernumber,((buffer_data_inmemory*)transferdata)->total_workernumber);

            }
            else
            {
                fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
            }
        }

    }

    free(pos);
    free(sax);
    free(ts);

    pthread_barrier_wait(((buffer_data_inmemory*)transferdata)->lock_barrier1);
    if (input_data->workernumber == 0) {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
    }
    
    bool have_record=false;
    int j;
    isax_node_record *r = malloc(sizeof(isax_node_record));
    
    while(1)
    {
        j=__sync_fetch_and_add(((buffer_data_inmemory*)transferdata)->node_counter,1);
    
        if(j>=index->fbl->number_of_buffers)
        {
            break;
        }
        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer*)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }
    
        int i;
        have_record=false;
        for (int k = 0; k < ((buffer_data_inmemory*)transferdata)->total_workernumber; k++)
        {
            if (current_fbl_node->buffer_size[k] > 0)
            have_record=true;
            for (i=0; i<current_fbl_node->buffer_size[k]; i++)
            {
                r->sax = (sax_type *) &(((current_fbl_node->sax_records[k]))[i*index->settings->paa_segments]);
                r->position = (file_position_type *) &((file_position_type *)(current_fbl_node->pos_records[k]))[i];
                r->insertion_mode = NO_TMP | PARTIAL;
                add_record_to_node_inmemory(index, (isax_node *)current_fbl_node->node, r, 1);      
            }
        }
        if (have_record)
        {
            flush_subtree_leaf_buffers_inmemory(index, (isax_node *)current_fbl_node->node);
        }
    
    }
    free(r);
}

void* index_creation_pRecBuf_worker_new_ekosmas(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas*) transferdata;

    threadPin(input_data->workernumber, maxquerythread);

    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num/read_block_length;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    file_position_type pos;
    sax_type *sax = malloc(sax_byte_size);                                              

   
    unsigned long i, block_num, my_ts_start, my_ts_end;
    while(1)
    {
        block_num = __sync_fetch_and_add(next_block_to_process, 1);
        if(block_num > total_blocks)
        {
            break;
        }

        my_ts_start = block_num*read_block_length;
        if (block_num == total_blocks)  {                                                  // there may still remain some more data series (i.e. less than #read_block_length)
            my_ts_end = ts_num; 
        }
        else {
            my_ts_end = (block_num+1)*read_block_length;
        }
        
        for ( i = my_ts_start; i < my_ts_end ; i++)
        {
            if(sax_from_ts( 
                        (ts_type *)&rawfile[i*index->settings->timeseries_size],                        
                        sax, 
                        index->settings->ts_values_per_paa_segment,
                        paa_segments, 
                        index->settings->sax_alphabet_cardinality,
                        index->settings->sax_bit_cardinality) 
                == SUCCESS)
            {
                pos = (file_position_type)(i * index->settings->timeseries_size);

                isax_pRecBuf_index_insert_inmemory_ekosmas(
                            index, 
                            sax, 
                            &pos,                                                                       
                            input_data->lock_firstnode,                                                 
                            input_data->workernumber,                                                   
                            maxquerythread);
            }
            else
            {
                fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
            }
        }

    }

    free(sax);

    pthread_barrier_wait(input_data->wait_summaries_to_compute);


    if (input_data->workernumber == 0) {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    if (input_data->parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE)
        tree_index_creation_from_pRecBuf_fai_blocking(transferdata);
    else //transferdata->parallelism_in_subtree == BLOCKING_PARALLELISM_IN_SUBTREE
        tree_index_creation_from_pRecBuf_fai_blocking_with_parallelism(transferdata);
}

// Embarrassingly Parallel
void* index_creation_pRecBuf_worker_new_ekosmas_EP(void *transferdata)      
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas*) transferdata;

    threadPin(input_data->workernumber, maxquerythread);

    unsigned long ts_num = input_data->ts_num;
    // unsigned long total_blocks = ts_num/read_block_length;

    isax_index *index= input_data->index;
    // unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    file_position_type pos;
    sax_type *sax = malloc(sax_byte_size);        

    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;

    if (input_data->workernumber == maxquerythread-1) {                             // there may still remain some more data series (i.e. less than #maxquerythread)
        my_ts_end += ts_num - ts_per_thread * maxquerythread;
    }
    
    for (unsigned long i = my_ts_start; i < my_ts_end; i++)
    {
        if(sax_from_ts( 
                    (ts_type *)&rawfile[i*index->settings->timeseries_size],                        
                    sax, 
                    index->settings->ts_values_per_paa_segment,
                    paa_segments, 
                    index->settings->sax_alphabet_cardinality,
                    index->settings->sax_bit_cardinality) 
            == SUCCESS)
        {
            pos = (file_position_type)(i * index->settings->timeseries_size);

            isax_pRecBuf_index_insert_inmemory_ekosmas(
                        index,
                        sax,
                        &pos,
                        input_data->lock_firstnode,
                        input_data->workernumber,
                        maxquerythread);
        }
        else
        {
            fprintf(stderr, "error: cannot insert record in index, since sax representation\
                failed to be created");
        }
    }

    free(sax);

    pthread_barrier_wait(input_data->wait_summaries_to_compute);

    if (input_data->workernumber == 0) {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    tree_index_creation_from_pRecBuf_fai_blocking(transferdata);
}

inline void store_isax_in_pRecBuf(buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, unsigned long ts_id) 
{
    file_position_type pos;
    int sax_byte_size = index->settings->sax_byte_size;
    int paa_segments = index->settings->paa_segments;
    
    sax_type *sax = malloc(sax_byte_size);          

    if(sax_from_ts( 
                (ts_type *)&rawfile[ts_id*index->settings->timeseries_size],
                input_data->sax, 
                // sax, 
                index->settings->ts_values_per_paa_segment,
                paa_segments, 
                index->settings->sax_alphabet_cardinality,
                index->settings->sax_bit_cardinality) 
        == SUCCESS)
    {
        pos = (file_position_type)(ts_id * index->settings->timeseries_size);

        // Create mask for the first bit of the sax representation
        root_mask_type first_bit_mask = 0x00;
        CREATE_MASK(first_bit_mask, index, input_data->sax);

        insert_to_pRecBuf_lock_free(
                            (parallel_first_buffer_layer_ekosmas_lf*)(index->fbl), 
                            input_data->sax, 
                            // sax, 
                            &pos,
                            first_bit_mask, 
                            index,
                            input_data->workernumber,
                            maxquerythread,
                            input_data->parallelism_in_subtree);


    }
    else
    {
        fprintf(stderr, "error: cannot insert record in index, since sax representation failed to be created");
    }
}
static inline void scan_for_unprocessed_ts(buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, unsigned long ts_start, unsigned long ts_end, volatile unsigned char *stop) 
{
    for (unsigned long ts_id = ts_start; ts_id < ts_end && !(*stop) ; ts_id++)
    {
        if (!ts_processed[ts_id]) {
            store_isax_in_pRecBuf(input_data, index, ts_id);
            if (!ts_processed[ts_id]) {
                ts_processed[ts_id] = 1;
            }  
        }
    }
}

static void process_group(unsigned long ts_group, unsigned long block_num, unsigned long total_groups_in_block, unsigned long ts_group_start, unsigned long ts_group_end, buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, char is_helper, char fai_only_after_help) {
    unsigned long ts_id, tmp;
    while (!group_processed[block_num][ts_group]) {
        if (fai_only_after_help && !group_helpers_exist[block_num][ts_group]) {
            tmp = next_ts_read_in_group[block_num][ts_group].num;
            next_ts_read_in_group[block_num][ts_group].num = tmp+1;
        }
        else {
            if (!next_ts_read_in_group_fai[block_num][ts_group].num && next_ts_read_in_group[block_num][ts_group].num) {
                CASULONG(&next_ts_read_in_group_fai[block_num][ts_group].num, 0, next_ts_read_in_group[block_num][ts_group].num);
            }
            tmp = __sync_fetch_and_add(&next_ts_read_in_group_fai[block_num][ts_group].num, 1);
        }
        ts_id = ts_group_start + tmp;

        if (ts_id >= ts_group_end)
            break;

        if (!ts_processed[ts_id]) {
            store_isax_in_pRecBuf(input_data, index, ts_id);
            if (!ts_processed[ts_id]) {
                ts_processed[ts_id] = 1;
            }
        }
    }

    if ((is_helper || group_helpers_exist[block_num][ts_group]) && !group_processed[block_num][ts_group]) {                    // performance enhancement
        scan_for_unprocessed_ts(input_data, index, ts_group_start, ts_group_end, &group_processed[block_num][ts_group]);
    }
    else {
        COUNT_MY_TIME_FOR_BLOCKS_END
        my_num_blocks_processed++;        
    }

    if (!group_processed[block_num][ts_group]) {
        group_processed[block_num][ts_group] = 1;
    }

}

static inline void scan_for_unprocessed_groups(unsigned long block_num, buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, unsigned long total_groups_in_block, unsigned long my_ts_start, unsigned long my_ts_end, volatile unsigned char *stop, char fai_only_after_help)
{
    for (unsigned long ts_group = 0; ts_group < total_groups_in_block && !*stop; ts_group++) {
        unsigned long ts_group_start = my_ts_start + ts_group*ts_group_length;
        unsigned long ts_group_end;

        if (ts_group == total_groups_in_block-1)  {
            ts_group_end = my_ts_end; 
        }
        else {
            ts_group_end = ts_group_start + ts_group_length;
        }

        if (!group_helpers_exist[block_num][ts_group]) {
            group_helpers_exist[block_num][ts_group] = 1;
        }

        process_group(ts_group, block_num, total_groups_in_block, ts_group_start, ts_group_end, input_data, index, 1, fai_only_after_help);
    }

    if (!block_processed[block_num]) {
        block_processed[block_num] = 1;
    }

}


static void process_block(unsigned long block_num, unsigned long total_blocks, unsigned long total_ts_num, buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, char is_helper, char fai_only_after_help)
{
    unsigned long my_ts_start, my_ts_end, ts_id;

    my_ts_start = block_num*read_block_length;
    if (block_num == total_blocks-1)  {                                                   // there may still remain some more data series (i.e. less than #read_block_length)
        my_ts_end = total_ts_num; 
    }
    else {
        my_ts_end = my_ts_start + read_block_length;
    }

    unsigned long total_groups_in_block = (my_ts_end-my_ts_start) / ts_group_length;
    if (total_groups_in_block*ts_group_length < my_ts_end-my_ts_start) {
        total_groups_in_block++;
    }

    unsigned long prev_group_id = 0;
    char helpers_exist = 0;


    if (!is_helper) {
        COUNT_MY_TIME_START
    }

    while (!block_processed[block_num]) {
        unsigned long ts_group;
        
        if (fai_only_after_help && !block_helper_exist[block_num]) {
            ts_group = next_ts_group_read_in_block[block_num].num;
            next_ts_group_read_in_block[block_num].num = ts_group+1;               
        }
        else {
            ts_group = __sync_fetch_and_add(&next_ts_group_read_in_block[block_num].num, 1);
        }

        if (ts_group > prev_group_id + 1) {                                         // performance enhancement
            helpers_exist = 1;                                                      
        }

        unsigned long ts_group_start = my_ts_start + ts_group*ts_group_length;
        unsigned long ts_group_end;
        
        if (ts_group >= total_groups_in_block) {
            break;
        }
        
        if (ts_group == total_groups_in_block-1)  {
            ts_group_end = my_ts_end; 
        }
        else {
            ts_group_end = ts_group_start + ts_group_length;
        }

        #ifdef DEBUG
        if (ts_group && ts_group <= prev_group_id ) {
            printf ("\nCAUTION: process_block: Counter went back!!\n\n"); fflush(stdout);
            getchar();
        }
        #endif

        if (is_helper && !group_helpers_exist[block_num][ts_group]) {
            group_helpers_exist[block_num][ts_group] = 1;
        }

        process_group(ts_group, block_num, total_groups_in_block, ts_group_start, ts_group_end, input_data, index, is_helper, fai_only_after_help);

        prev_group_id = ts_group;                                                         // performance enhancement
    }

    if ((is_helper || helpers_exist) && !block_processed[block_num]) {                    // performance enhancement
        scan_for_unprocessed_groups(block_num, input_data, index, total_groups_in_block, my_ts_start, my_ts_end, &block_processed[block_num], fai_only_after_help);
    }
    else {
        COUNT_MY_TIME_FOR_BLOCKS_END
        my_num_blocks_processed++;        
    }

    if (!block_processed[block_num]) {
        block_processed[block_num] = 1;
    }
}

static inline void scan_for_unprocessed_blocks(buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, unsigned long total_blocks, char fai_only_after_help) 
{
    unsigned long total_ts_num = input_data->ts_num;
    unsigned long my_id = input_data->workernumber;

    unsigned long start_block_num = (my_id + 1) % total_blocks;
    unsigned long block_num = start_block_num;

    unsigned long backoff_time = backoff_multiplier;

    if (my_num_blocks_processed) {
        backoff_time *= (unsigned long) BACKOFF_BLOCK_DELAY_VALUE;
    }
    else {
        backoff_time = 0;
    }

    do
    {
        if (all_blocks_processed) {
            break;
        }
        else if (block_processed[block_num]) {
            block_num = (block_num+1)%total_blocks;
            continue;
        }

        backoff_delay_char(backoff_time, &block_processed[block_num]);

        if (block_processed[block_num]) {
            blocks_helping_avoided_cnt++;
            block_num = (block_num+1)%total_blocks;
            continue;
        }

        blocks_helped_cnt++;
        // __sync_fetch_and_add(&block_helpers_num[block_num], 1);

        if (fai_only_after_help && !block_helper_exist[block_num]) {
            block_helper_exist[block_num] = 1;
        }

        process_block(block_num, total_blocks, total_ts_num, input_data, index, 1, fai_only_after_help);

        block_num = (block_num+1)%total_blocks;
    } while (block_num != start_block_num);

    if (!all_blocks_processed)
        all_blocks_processed = 1;

    if (blocks_helping_avoided_cnt) {
        COUNT_BLOCK_HELP_AVOIDED(blocks_helping_avoided_cnt)
    }

    if (blocks_helped_cnt) {
        COUNT_BLOCKS_HELPED(blocks_helped_cnt)
    }
}

// Lock-Free Full FAI (9992, 99929, 9994, 99949, 9996, 99969, 9998, 99989)
void* index_creation_pRecBuf_worker_new_ekosmas_lock_free_full_fai(void *transferdata)
{
    buffer_data_inmemory_ekosmas_lf *input_data = (buffer_data_inmemory_ekosmas_lf*) transferdata;

    threadPin(input_data->workernumber, maxquerythread);

    unsigned long total_ts_num = input_data->ts_num;
    unsigned long total_blocks = total_ts_num/read_block_length;
    if (read_block_length*total_blocks < total_ts_num) {
        total_blocks++;
    }   

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;
    unsigned long block_num;
    while(!all_blocks_processed)
    {
        block_num = __sync_fetch_and_add(next_block_to_process, 1);
        if(block_num >= total_blocks)
        {
            break;
        }

        process_block(block_num, total_blocks, total_ts_num, input_data, index, 0, 0);

    }

    if (DO_NOT_HELP) {                               
        pthread_barrier_wait(input_data->wait_summaries_to_compute);          
    }
    else {
        scan_for_unprocessed_blocks(input_data, index, total_blocks, 0);
    }

    if (input_data->workernumber == 0) {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    tree_index_creation_from_pRecBuf_fai_lock_free(transferdata, input_data->parallelism_in_subtree);
}

// Lock-Free FAI (per ts of a block) only after a helper exists (9993, 99939, 9995, 9959, 9997, 99979, 9999, 99999)
void* index_creation_pRecBuf_worker_new_ekosmas_lock_free_fai_only_after_help(void *transferdata)
{
    buffer_data_inmemory_ekosmas_lf *input_data = (buffer_data_inmemory_ekosmas_lf*) transferdata;

    threadPin(input_data->workernumber, maxquerythread);

    unsigned long total_ts_num = input_data->ts_num;
    unsigned long total_blocks = total_ts_num/read_block_length;
    if (read_block_length*total_blocks < total_ts_num) {
        total_blocks++;
    }   

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;
    unsigned long block_num;
    while(!all_blocks_processed)
    {
        block_num = __sync_fetch_and_add(next_block_to_process, 1);
        if(block_num >= total_blocks)
        {
            break;
        }

        process_block(block_num, total_blocks, total_ts_num, input_data, index, 0, 1);
    }

    if (DO_NOT_HELP) {                                  
        pthread_barrier_wait(input_data->wait_summaries_to_compute);          
    }
    else {
        scan_for_unprocessed_blocks(input_data, index, total_blocks, 1);
    }

    if (input_data->workernumber == 0) {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    tree_index_creation_from_pRecBuf_fai_lock_free(transferdata, input_data->parallelism_in_subtree);     
}


root_mask_type isax_pRecBuf_index_insert_inmemory(isax_index *index,
                                    sax_type * sax,
                                    file_position_type * pos, pthread_mutex_t *lock_firstnode, int workernumber, int total_workernumber)
{
    int i,t;
    int totalsize = index->settings->max_total_buffer_size;

    // Create mask for the first bit of the sax representation

    // Step 1: Check if there is a root node that represents the
    //         current node's sax representation

    // TODO: Create INSERTION SHORT AND BINARY SEARCH METHODS.

    root_mask_type first_bit_mask = 0x00;

    CREATE_MASK(first_bit_mask, index, sax);

    insert_to_pRecBuf(
                        (parallel_first_buffer_layer*)(index->fbl), 
                        sax, 
                        pos,
                        first_bit_mask, 
                        index,
                        lock_firstnode,
                        workernumber,
                        total_workernumber);

    return first_bit_mask;
}
root_mask_type isax_pRecBuf_index_insert_inmemory_ekosmas(isax_index *index,
                                    sax_type * sax,
                                    file_position_type * pos, pthread_mutex_t *lock_firstnode, int workernumber, int total_workernumber)
{
    int i,t;
    int totalsize = index->settings->max_total_buffer_size;

    // Create mask for the first bit of the sax representation

    // Step 1: Check if there is a root node that represents the
    //         current node's sax representation

    // TODO: Create INSERTION SHORT AND BINARY SEARCH METHODS.

    root_mask_type first_bit_mask = 0x00;

    CREATE_MASK(first_bit_mask, index, sax);

    insert_to_pRecBuf_ekosmas(
                        (parallel_first_buffer_layer_ekosmas*)(index->fbl), 
                        sax, 
                        pos,
                        first_bit_mask, 
                        index,
                        lock_firstnode,
                        workernumber,
                        total_workernumber);

    return first_bit_mask;
}

enum response flush_subtree_leaf_buffers_inmemory (isax_index *index, isax_node *node)
{

    if (node->is_leaf && node->filename != NULL) {
        // Set that unloaded data exist in disk
        if (node->buffer->partial_buffer_size > 0
            || node->buffer->tmp_partial_buffer_size > 0) {
            node->has_partial_data_file = 1;
        }
        // Set that the node has flushed full data in the disk
        if (node->buffer->full_buffer_size > 0
            || node->buffer->tmp_full_buffer_size > 0) {
            node->has_full_data_file = 1;
        }

        if(node->has_full_data_file) {
            int prev_rec_count = node->leaf_size - (node->buffer->full_buffer_size + node->buffer->tmp_full_buffer_size);

            int previous_page_size =  ceil((float) (prev_rec_count * index->settings->full_record_size) / (float) PAGE_SIZE);
            int current_page_size =   ceil((float) (node->leaf_size * index->settings->full_record_size) / (float) PAGE_SIZE);
            __sync_fetch_and_add(&(index->memory_info.disk_data_full),(current_page_size - previous_page_size));
            //index->memory_info.disk_data_full += (current_page_size - previous_page_size);
        }
        if(node->has_partial_data_file) {
            int prev_rec_count = node->leaf_size - (node->buffer->partial_buffer_size + node->buffer->tmp_partial_buffer_size);

            int previous_page_size =  ceil((float) (prev_rec_count * index->settings->partial_record_size) / (float) PAGE_SIZE);
            int current_page_size =   ceil((float) (node->leaf_size * index->settings->partial_record_size) / (float) PAGE_SIZE);

            //index->memory_info.disk_data_partial += (current_page_size - previous_page_size);
            __sync_fetch_and_add(&(index->memory_info.disk_data_partial),(current_page_size - previous_page_size));
        }
        if(node->has_full_data_file && node->has_partial_data_file) {
             printf("WARNING: (Mem size counting) this leaf has both partial and full data.\n");
        }
        //index->memory_info.disk_data_full += (node->buffer->full_buffer_size +
                                              //node->buffer->tmp_full_buffer_size);
        __sync_fetch_and_add(&(index->memory_info.disk_data_full),(node->buffer->full_buffer_size + node->buffer->tmp_full_buffer_size));
        //index->memory_info.disk_data_partial += (node->buffer->partial_buffer_size +
                                                 //node->buffer->tmp_partial_buffer_size);
         __sync_fetch_and_add(&(index->memory_info.disk_data_partial),(node->buffer->partial_buffer_size + node->buffer->tmp_partial_buffer_size));
        //flush_node_buffer(node->buffer, index->settings->paa_segments,
                          //index->settings->timeseries_size,
                          //node->filename);
    }
    else if (!node->is_leaf)
    {
        flush_subtree_leaf_buffers_inmemory(index, node->left_child);
        flush_subtree_leaf_buffers_inmemory(index, node->right_child);
    }

    return SUCCESS;
}
