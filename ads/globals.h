//
//  defines.h
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/19/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
//  Updated by Eleftherios Kosmas on May 2020.
//
#include "config.h"

#ifndef isax_globals_h
  #define isax_globals_h

  #define STORE_ANSWER

  #define ULONG_MAX 0xFFFFFFFFUL


  #pragma GCC diagnostic ignored "-Wunused-result" 
  #pragma GCC diagnostic ignored "-Wunused-variable" 
   

  #define PAGE_SIZE 4096
  #define PROGRESS_CALCULATE_THREAD_NUMBER 12
  #define PROGRESS_FLUSH_THREAD_NUMBER 12
  #define QUERIES_THREAD_NUMBER 4
  #define DISK_BUFFER_SIZE 8192
  #define LOCK_SIZE 65536 
  ///// TYPES /////
  typedef unsigned char sax_type;
  typedef float ts_type;
  typedef unsigned long long file_position_type;
  typedef unsigned long long root_mask_type;

  enum response {OUT_OF_MEMORY_FAILURE, FAILURE, SUCCESS};
  enum insertion_mode {PARTIAL = 1, 
                       TMP = 2, 
                       FULL = 4,
                       NO_TMP = 8};

  enum buffer_cleaning_mode {FULL_CLEAN, TMP_ONLY_CLEAN, TMP_AND_TS_CLEAN};
  enum node_cleaning_mode {DO_NOT_INCLUDE_CHILDREN = 0,
                           INCLUDE_CHILDREN = 1};

  ///// DEFINITIONS /////
  #define MINVAL -2000000
  #define MAXVAL 2000000
  #define DELIMITER ' '
  #define TRUE 1
  #define FALSE 0
  #define BUFFER_REALLOCATION_RATE  2 

  ///// GLOBAL VARIABLES /////
  int FLUSHES;
  unsigned long BYTES_ACCESSED;
  float APPROXIMATE;

  #define INCREASE_BYTES_ACCESSED(new_bytes) \
  	    BYTES_ACCESSED += (unsigned long) new_bytes;
  #define RESET_BYTES_ACCESSED \
  		BYTES_ACCESSED = 0;
  #define SET_APPROXIMATE(approximate)\
  		APPROXIMATE = approximate;

  ///// MACROS /////
  #define CREATE_MASK(mask, index, sax_array)\
  	int mask__i; \
  	for (mask__i=0; mask__i < index->settings->paa_segments; mask__i++) \
  		if(index->settings->bit_masks[index->settings->sax_bit_cardinality - 1] & sax_array[mask__i]) \
  			mask |= index->settings->bit_masks[index->settings->paa_segments - mask__i - 1];  

  ///// EKOSMAS DEVELOPMENT OUTPUT /////
  #ifdef EKOSMAS_DEV_OUTPUT
      #define EKOSMAS_PRINT(message) {printf(message); fflush(stdout);}
  #else
      #define EKOSMAS_PRINT(message) ;
  #endif

  ///// EKOSMAS CACHE ALIGNMENT /////
  #ifndef CACHE_LINE_SIZE
  // #define CACHE_LINE_SIZE   64
  // #define CACHE_LINE_SIZE   128
  // #define CACHE_LINE_SIZE   256
  #define CACHE_LINE_SIZE   512   
  #endif
  #define CACHE_ALIGN       __attribute__ ((aligned (CACHE_LINE_SIZE)))
  #define PAD_CACHE(A)      ((CACHE_LINE_SIZE - (A % CACHE_LINE_SIZE))/sizeof(char))

  ///// EKOSMAS ATOMIC PRIMITIVES /////
  #define CASPTR(A, B, C)     __sync_bool_compare_and_swap((long *)A, (long)B, (long)C)
  // #define CASFLOAT(A, B, C)   __sync_bool_compare_and_swap_4((float *)A, (float)B, (float)C)     // NOT WORKING
  #define CASULONG(A, B, C)   __sync_bool_compare_and_swap((unsigned long *)A, (unsigned long)B, (unsigned long)C)

  ///// EKOSMAS ENUM /////
  enum {NO_PARALLELISM_IN_SUBTREE, 
        BLOCKING_PARALLELISM_IN_SUBTREE, 
        LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE, 
        LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP, 
        LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF,
        LOCKFREE_PARALLELISM_IN_SUBTREE_COW};


  ///// BENCHMARKING /////
  #ifdef BENCHMARK
		#include <time.h>
		#include <sys/time.h>
	   
        double tS;
        double tE;
        int added_tree_node;
        // --------------------------------------
        struct timeval total_time_start;
        struct timeval queue_time_start;
        struct timeval parse_time_start;
        struct timeval input_time_start;
        struct timeval input2_time_start;
        struct timeval cal_time_start;
        struct timeval output_time_start;
        struct timeval output2_time_start;
        struct timeval load_node_start;
        struct timeval current_time;
        struct timeval fetch_start;
        struct timeval fetch_check_start;
        struct timeval fill_rec_bufs_time_start;
        struct timeval create_tree_index_time_start;
        struct timeval query_answering_time_start;
        struct timeval queue_fill_time_start;
        struct timeval queue_fill_help_time_start;
        struct timeval queue_process_time_start;
        struct timeval queue_process_help_time_start;
        struct timeval initialize_index_tree_time_start;
        // --------------------------------------
        // struct timespec total_time_start;
        // struct timespec queue_time_start;
        // struct timespec parse_time_start;
        // struct timespec input_time_start;
        // struct timespec input2_time_start;
        // struct timespec cal_time_start;
        // struct timespec output_time_start;
        // struct timespec output2_time_start;
        // struct timespec load_node_start;
        // struct timespec current_time;
        // struct timespec fetch_start;
        // struct timespec fetch_check_start;
        // struct timespec fill_rec_bufs_time_start;
        // struct timespec create_tree_index_time_start;
        // struct timespec query_answering_time_start;
        // struct timespec queue_fill_time_start;
        // struct timespec queue_fill_help_time_start;
        // struct timespec queue_process_time_start;
        // struct timespec queue_process_help_time_start;
        // struct timespec initialize_index_tree_time_start;
        // --------------------------------------
        double total_input_time;
        double total_queue_time;
        double total_input2_time;
        double total_cal_time;
        double load_node_time;
        double total_output_time;
        double total_output2_time;
        double total_parse_time;
        double total_time;
        int total_tree_nodes;
        int loaded_nodes;
        int checked_nodes;
        file_position_type loaded_records;
        unsigned long int LBDcalculationnumber;
        unsigned long int RDcalculationnumber;
        unsigned long blocks_helped;
        unsigned long block_help_avoided;
        unsigned long subtrees_helped;
        unsigned long subtree_help_avoided;
        
        double fill_rec_bufs_time;
        double create_tree_index_time;
        double query_answering_time;
        double queue_fill_time;
        double queue_fill_help_time;
        double queue_process_time;
        double queue_process_help_time;
        double initialize_index_tree_time;
        long int ts_in_RecBufs_cnt;
        long int ts_in_tree_cnt;
        long int non_empty_subtrees_cnt;
        long int min_ts_in_subtrees;
        long int max_ts_in_subtrees;
        unsigned long leaves_in_arrays;
        unsigned long unique_leaves_in_arrays;
        unsigned long leaves_in_arrays_total;
        unsigned long unique_leaves_in_arrays_total;
        char DO_NOT_HELP;
        #define INIT_STATS() total_input_time = 0;\
                             total_output_time = 0;\
                             total_time = 0;\
                             total_parse_time = 0;\
                             total_tree_nodes = 0;\
                             loaded_nodes = 0;\
                             checked_nodes = 0;\
                             load_node_time=0;\
                             loaded_records = 0; \
                             APPROXIMATE=0;\
                             BYTES_ACCESSED=0;\
                             blocks_helped=0;\
                             block_help_avoided=0;\
                             subtrees_helped=0;\
                             subtree_help_avoided=0;\
                             fill_rec_bufs_time=0;\
                             create_tree_index_time=0;\
                             query_answering_time=0;\
                             queue_fill_time=0;\
                             queue_fill_help_time=0;\
                             queue_process_time=0;\
                             queue_process_help_time=0;\
                             initialize_index_tree_time=0;\
                             leaves_in_arrays_total=0;\
                             unique_leaves_in_arrays_total=0;
        // printf("input\t output\t nodes\t checked_nodes\t bytes_accessed\t loaded_nodes\t loaded_records\t approximate_distance\t distance\t total\n");
        #define PRINT_STATS(result_distance) printf("%lf\t %lf\t %d\t %d\t %ld\t %d\t %lld\t %lf\t %lf\t %lf\t %d\t %lf\t %lf\n", \
        total_input_time, total_output_time, \
        total_tree_nodes, checked_nodes, \
        BYTES_ACCESSED, loaded_nodes, \
        loaded_records, APPROXIMATE,\
        result_distance, total_time,\
        block_help_avoided, fill_rec_bufs_time, create_tree_index_time);
        //#define PRINT_STATS(result_distance) printf("%d\t",loaded_nodes);

        #define PRINT_QUERY_STATS(result_distance) printf("%d\t %d\t %ld\t %d\t %lld\t %lf\t %lf\t %u\t %u\t %u\t\n", \
        total_tree_nodes, checked_nodes, \
        BYTES_ACCESSED, loaded_nodes, loaded_records, \
        APPROXIMATE, result_distance,\
        unique_leaves_in_arrays, leaves_in_arrays, leaves_in_arrays-unique_leaves_in_arrays);


        #define MY_PRINT_STATS(result_distance) printf("%lf\t %lf\t %lf\t %lf\t %lf\t %lf\t  %lu\t %lu\t %lu\t %lu\t %ld\t %ld\t %ld\t %ld\t %ld\t %ld\t %lf\t %lf\t %lf\t %lf\t %lf\t %lf\t %lf\t %u\t %u\t %u\t\n", \
        total_input_time, total_output_time, \
        total_time, fill_rec_bufs_time, create_tree_index_time, query_answering_time, \
        blocks_helped, block_help_avoided, subtrees_helped, subtree_help_avoided, \
        ts_in_RecBufs_cnt, ts_in_RecBufs_cnt-dataset_size, ts_in_tree_cnt-ts_in_RecBufs_cnt, \
        non_empty_subtrees_cnt, min_ts_in_subtrees, max_ts_in_subtrees, (double)ts_in_tree_cnt/non_empty_subtrees_cnt, \
        queue_fill_time, queue_fill_help_time, queue_process_time, queue_process_help_time, initialize_index_tree_time, \
        queue_fill_time + queue_process_time + initialize_index_tree_time,\
        unique_leaves_in_arrays_total, leaves_in_arrays_total, leaves_in_arrays_total-unique_leaves_in_arrays_total);

        #define PRINT_STATS_TO_FILE(result_distance, fp) fprintf(fp, "%lf\t %lf\t %lf\t %lf\t %lf\t %lf\t %lu\t %lu\t %lu\t %lu\t %ld\t %ld\t %ld\t %ld\t %ld\t %ld\t %lf\t %lf\t %lf\t %lf\t %lf\t %lf\t %lf\t %u\t %u\t %u\t\n", \
        total_input_time, total_output_time, \
        total_time, fill_rec_bufs_time, create_tree_index_time, query_answering_time, \
        blocks_helped, block_help_avoided, subtrees_helped, subtree_help_avoided, \
        ts_in_RecBufs_cnt, ts_in_RecBufs_cnt-dataset_size, ts_in_tree_cnt-ts_in_RecBufs_cnt, \
        non_empty_subtrees_cnt, min_ts_in_subtrees, max_ts_in_subtrees, (double)ts_in_tree_cnt/non_empty_subtrees_cnt, \
        queue_fill_time, queue_fill_help_time, queue_process_time, queue_process_help_time, initialize_index_tree_time, \
        queue_fill_time + queue_process_time + initialize_index_tree_time,\
        unique_leaves_in_arrays_total, leaves_in_arrays_total, leaves_in_arrays_total-unique_leaves_in_arrays_total);

        #define min(x,y)  ( x<y?x:y )
        #define COUNT_NEW_NODE() __sync_fetch_and_add(&total_tree_nodes,1);
        #define COUNT_BLOCKS_HELPED(num) __sync_fetch_and_add(&blocks_helped, num);
        #define COUNT_BLOCK_HELP_AVOIDED(num) __sync_fetch_and_add(&block_help_avoided, num);
        #define COUNT_SUBTREES_HELPED(num) __sync_fetch_and_add(&subtrees_helped, num);
        #define COUNT_SUBTREE_HELP_AVOIDED(num) __sync_fetch_and_add(&subtree_help_avoided, num);
        #define COUNT_LEAVES_ARRAY(num) __sync_fetch_and_add(&leaves_in_arrays, num);
        #define COUNT_UNIQUE_LEAVES_ARRAY(num) __sync_fetch_and_add(&unique_leaves_in_arrays, num);
        #define COUNT_LEAVES_ARRAY_TOTAL(num) leaves_in_arrays_total += num;
        #define COUNT_UNIQUE_LEAVES_ARRAY_TOTAL(num) unique_leaves_in_arrays_total += num;
        #define COUNT_LOADED_NODE() loaded_nodes++;
        #define COUNT_CHECKED_NODE() checked_nodes++;
        #define COUNT_LOADED_RECORD() loaded_records++;
        
        #define COUNT_INPUT_TIME_START gettimeofday(&input_time_start, NULL);
        #define COUNT_FILL_REC_BUF_TIME_START gettimeofday(&fill_rec_bufs_time_start, NULL);
        #define COUNT_CREATE_TREE_INDEX_TIME_START gettimeofday(&create_tree_index_time_start, NULL);
        #define COUNT_QUERY_ANSWERING_TIME_START gettimeofday(&query_answering_time_start, NULL);
        #define COUNT_QUEUE_FILL_TIME_START gettimeofday(&queue_fill_time_start, NULL);
        #define COUNT_QUEUE_FILL_HELP_TIME_START gettimeofday(&queue_fill_help_time_start, NULL);
        #define COUNT_QUEUE_PROCESS_TIME_START gettimeofday(&queue_process_time_start, NULL);
        #define COUNT_QUEUE_PROCESS_HELP_TIME_START gettimeofday(&queue_process_help_time_start, NULL);
        #define COUNT_INITIALIZE_INDEX_TREE_TIME_START gettimeofday(&initialize_index_tree_time_start, NULL);
        #define COUNT_QUEUE_TIME_START gettimeofday(&queue_time_start, NULL);
        #define COUNT_CAL_TIME_START gettimeofday(&cal_time_start, NULL); 
        #define COUNT_INPUT2_TIME_START gettimeofday(&input2_time_start, NULL); 
        #define COUNT_OUTPUT_TIME_START gettimeofday(&output_time_start, NULL); 
        #define COUNT_OUTPUT2_TIME_START gettimeofday(&output2_time_start, NULL);
        #define COUNT_TOTAL_TIME_START gettimeofday(&total_time_start, NULL);   
        #define COUNT_PARSE_TIME_START gettimeofday(&parse_time_start, NULL);   
        #define COUNT_LOAD_NODE_START gettimeofday(&load_node_start, NULL);
        #define COUNT_INPUT_TIME_END  gettimeofday(&current_time, NULL); \
                                      tS = input_time_start.tv_sec*1000000 + (input_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000 + (current_time.tv_usec); \
                                      total_input_time += (tE - tS); 
        #define COUNT_INPUT2_TIME_END  gettimeofday(&current_time, NULL); \
                                      tS = input2_time_start.tv_sec*1000000 + (input2_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000 + (current_time.tv_usec); \
                                      total_input2_time += (tE - tS); 
        #define COUNT_CAL_TIME_END  gettimeofday(&current_time, NULL); \
                                      tS = cal_time_start.tv_sec*1000000 + (cal_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000 + (current_time.tv_usec); \
                                      total_cal_time += (tE - tS); 
        #define COUNT_OUTPUT_TIME_END gettimeofday(&current_time, NULL); \
                                      tS = output_time_start.tv_sec*1000000 + (output_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      total_output_time += (tE - tS); 
        #define COUNT_OUTPUT2_TIME_END gettimeofday(&current_time, NULL); \
                                      tS = output2_time_start.tv_sec*1000000 + (output2_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      total_output2_time += (tE - tS); 
        #define COUNT_TOTAL_TIME_END  gettimeofday(&current_time, NULL); \
                                      tS = total_time_start.tv_sec*1000000 + (total_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      total_time += (tE - tS); 
        #define COUNT_PARSE_TIME_END  gettimeofday(&current_time, NULL);  \
                                      tS = parse_time_start.tv_sec*1000000 + (parse_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      total_parse_time += (tE - tS); 
        #define COUNT_LOAD_NODE_END   gettimeofday(&current_time, NULL);  \
                                      tS = load_node_start.tv_sec*1000000 + (load_node_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      load_node_time += (tE - tS); 
        #define COUNT_QUEUE_TIME_END  gettimeofday(&current_time, NULL);  \
                                      tS = queue_time_start.tv_sec*1000000 + (queue_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      total_queue_time += (tE - tS); 
        #define COUNT_FILL_REC_BUF_TIME_END  gettimeofday(&current_time, NULL);  \
                                      tS = fill_rec_bufs_time_start.tv_sec*1000000 + (fill_rec_bufs_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      fill_rec_bufs_time += (tE - tS); 
        #define COUNT_CREATE_TREE_INDEX_TIME_END  gettimeofday(&current_time, NULL);  \
                                      tS = create_tree_index_time_start.tv_sec*1000000 + (create_tree_index_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      create_tree_index_time += (tE - tS); 
        #define COUNT_QUERY_ANSWERING_TIME_END  gettimeofday(&current_time, NULL);  \
                                      tS = query_answering_time_start.tv_sec*1000000 + (query_answering_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      query_answering_time += (tE - tS);             
        #define COUNT_QUEUE_FILL_TIME_END  gettimeofday(&current_time, NULL);  \
                                      tS = queue_fill_time_start.tv_sec*1000000 + (queue_fill_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      queue_fill_time += (tE - tS);                              
        #define COUNT_QUEUE_FILL_HELP_TIME_END  gettimeofday(&current_time, NULL);  \
                                      tS = queue_fill_help_time_start.tv_sec*1000000 + (queue_fill_help_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      queue_fill_help_time += (tE - tS);                              
        #define COUNT_QUEUE_PROCESS_TIME_END  gettimeofday(&current_time, NULL);  \
                                      tS = queue_process_time_start.tv_sec*1000000 + (queue_process_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      queue_process_time += (tE - tS);                              
        #define COUNT_QUEUE_PROCESS_HELP_TIME_END  gettimeofday(&current_time, NULL);  \
                                      tS = queue_process_help_time_start.tv_sec*1000000 + (queue_process_help_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      queue_process_help_time += (tE - tS);      
        #define COUNT_INITIALIZE_INDEX_TREE_TIME_END gettimeofday(&current_time, NULL);  \
                                      tS = initialize_index_tree_time_start.tv_sec*1000000 + (initialize_index_tree_time_start.tv_usec); \
                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
                                      initialize_index_tree_time += (tE - tS); 

        // -------------------

        // #define COUNT_INPUT_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &input_time_start);
        // #define COUNT_FILL_REC_BUF_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &fill_rec_bufs_time_start);
        // #define COUNT_CREATE_TREE_INDEX_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &create_tree_index_time_start);
        // #define COUNT_QUERY_ANSWERING_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &query_answering_time_start);
        // #define COUNT_QUEUE_FILL_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &queue_fill_time_start);
        // #define COUNT_QUEUE_FILL_HELP_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &queue_fill_help_time_start);
        // #define COUNT_QUEUE_PROCESS_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &queue_process_time_start);
        // #define COUNT_QUEUE_PROCESS_HELP_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &queue_process_help_time_start);
        // #define COUNT_INITIALIZE_INDEX_TREE_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &initialize_index_tree_time_start);
        // #define COUNT_QUEUE_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &queue_time_start);
        // #define COUNT_CAL_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cal_time_start); 
        // #define COUNT_INPUT2_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &input2_time_start); 
        // #define COUNT_OUTPUT_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &output_time_start); 
        // #define COUNT_OUTPUT2_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &output2_time_start);
        // #define COUNT_TOTAL_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &total_time_start);   
        // #define COUNT_PARSE_TIME_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &parse_time_start);   
        // #define COUNT_LOAD_NODE_START clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &load_node_start);
        // #define COUNT_INPUT_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time); \
        //                               tS = input_time_start.tv_sec*1000000000 + (input_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000 + (current_time.tv_nsec); \
        //                               total_input_time += (tE - tS); 
        // #define COUNT_INPUT2_TIME_END clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time); \
        //                               tS = input2_time_start.tv_sec*1000000000 + (input2_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000 + (current_time.tv_nsec); \
        //                               total_input2_time += (tE - tS); 
        // #define COUNT_CAL_TIME_END    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time); \
        //                               tS = cal_time_start.tv_sec*1000000000 + (cal_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000 + (current_time.tv_nsec); \
        //                               total_cal_time += (tE - tS); 
        // #define COUNT_OUTPUT_TIME_END clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time); \
        //                               tS = output_time_start.tv_sec*1000000000 + (output_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               total_output_time += (tE - tS); 
        // #define COUNT_OUTPUT2_TIME_END clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time); \
        //                               tS = output2_time_start.tv_sec*1000000000 + (output2_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               total_output2_time += (tE - tS); 
        // #define COUNT_TOTAL_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time); \
        //                               tS = total_time_start.tv_sec*1000000000 + (total_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               total_time += (tE - tS); 
        // #define COUNT_PARSE_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = parse_time_start.tv_sec*1000000000 + (parse_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               total_parse_time += (tE - tS); 
        // #define COUNT_LOAD_NODE_END   clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = load_node_start.tv_sec*1000000000 + (load_node_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               load_node_time += (tE - tS); 
        // #define COUNT_QUEUE_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = queue_time_start.tv_sec*1000000000 + (queue_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               total_queue_time += (tE - tS); 
        // #define COUNT_FILL_REC_BUF_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = fill_rec_bufs_time_start.tv_sec*1000000000 + (fill_rec_bufs_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               fill_rec_bufs_time += (tE - tS); 
        // #define COUNT_CREATE_TREE_INDEX_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = create_tree_index_time_start.tv_sec*1000000000 + (create_tree_index_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               create_tree_index_time += (tE - tS); 
        // #define COUNT_QUERY_ANSWERING_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = query_answering_time_start.tv_sec*1000000000 + (query_answering_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               query_answering_time += (tE - tS);             
        // #define COUNT_QUEUE_FILL_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = queue_fill_time_start.tv_sec*1000000000 + (queue_fill_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               queue_fill_time += (tE - tS);                              
        // #define COUNT_QUEUE_FILL_HELP_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = queue_fill_help_time_start.tv_sec*1000000000 + (queue_fill_help_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               queue_fill_help_time += (tE - tS);                              
        // #define COUNT_QUEUE_PROCESS_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = queue_process_time_start.tv_sec*1000000000 + (queue_process_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               queue_process_time += (tE - tS);                              
        // #define COUNT_QUEUE_PROCESS_HELP_TIME_END  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = queue_process_help_time_start.tv_sec*1000000000 + (queue_process_help_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               queue_process_help_time += (tE - tS);      
        // #define COUNT_INITIALIZE_INDEX_TREE_TIME_END clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &current_time);  \
        //                               tS = initialize_index_tree_time_start.tv_sec*1000000000 + (initialize_index_tree_time_start.tv_nsec); \
        //                               tE = current_time.tv_sec*1000000000  + (current_time.tv_nsec); \
        //                               initialize_index_tree_time += (tE - tS); 
    #else
        #define INIT_STATS() ;
        #define PRINT_STATS() ;
        #define PRINT_QUERY_STATS() ;
        #define MY_PRINT_STATS() ;
        #define PRINT_STATS_TO_FILE(result_distance, fp) ;
        #define COUNT_NEW_NODE() ;
        #define COUNT_BLOCK_HELP_AVOIDED(num) ;
        #define COUNT_SUBTREE_HELP_AVOIDED(num) ;
        #define COUNT_CHECKED_NODE();
        #define COUNT_LOADED_NODE() ;
        #define COUNT_LOADED_RECORD() ;
        #define COUNT_INPUT_TIME_START ;
        #define COUNT_INPUT_TIME_END ;
        #define COUNT_OUTPUT_TIME_START ;
        #define COUNT_OUTPUT_TIME_END ;
        #define COUNT_TOTAL_TIME_START ;
        #define COUNT_TOTAL_TIME_END ;
        #define COUNT_PARSE_TIME_START ;
        #define COUNT_PARSE_TIME_END ;
        #define COUNT_LOAD_NODE_END ;
        #define COUNT_QUEUE_TIME_END ;
        #define COUNT_FILL_REC_BUF_TIME_START ;
        #define COUNT_FILL_REC_BUF_TIME_END ;
        #define COUNT_CREATE_TREE_INDEX_TIME_START ;
        #define COUNT_CREATE_TREE_INDEX_TIME_END ;                                      
        #define COUNT_QUERY_ANSWERING_TIME_START ;
        #define COUNT_QUERY_ANSWERING_TIME_END ;
    #endif
#endif