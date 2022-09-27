//
//  main.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/12/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
//  Updated by Eleftherios Kosmas on May 2020.
//
#define _GNU_SOURCE

#define PRODUCT "----------------------------------------------\
\nThis is the Adaptive Leaf iSAX index.\n\
Copyright (C) 2011-2014 University of Trento.\n\
----------------------------------------------\n\n"
#ifdef VALUES
#include <values.h>
#endif

#include "../../config.h"
#include "../../globals.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <math.h>
#include <getopt.h>
#include <time.h>
#include <float.h>
#include <sched.h>

#include "ads/sax/sax.h"
#include "ads/sax/ts.h"
#include "ads/isax_visualize_index.h"
#include "ads/isax_file_loaders.h"
#include "ads/isax_visualize_index.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/isax_query_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_inmemory_query_engine.h"
#include "ads/inmemory_index_engine.h"
#include "ads/parallel_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/inmemory_topk_engine.h"
//#define PROGRESS_CALCULATE_THREAD_NUMBER 4
//#define PROGRESS_FLUSH_THREAD_NUMBER 4
//#define QUERIES_THREAD_NUMBER 4
//#define DISK_BUFFER_SIZE 32

int main (int argc, char **argv)
{
    static char * dataset = "/home/ekosmas/datasets/dataset10GB.bin";
    static char * queries = "/home/botao/document/";
    static char * dataset_output = NULL;
    static char * index_path = "/home/botao/document/myexperiment/";
    static char * labelset="/home/botao/document/myexperiment/";
    static long int dataset_size = 10485760;//testbench
    static int queries_size = 10;
    static int time_series_size = 256;
    static int paa_segments = 16;
    static int sax_cardinality = 8;
    static int leaf_size = 2000;
    static int min_leaf_size = 2000;
    static int initial_lbl_size = 2000;
    static int flush_limit = 1000000;
    static int initial_fbl_size = 100;
    static char use_index = 0;
    static int complete_type = 0;
    static int total_loaded_leaves = 1;
    static int tight_bound = 0;
    static int aggressive_check = 0;
    static float minimum_distance = FLT_MAX;
    static int serial_scan = 0;
    static char knnlabel = 0;
    static int min_checked_leaves = -1;
    static int cpu_control_type = 80;
    static char inmemory_flag=1;
    int calculate_thread=8;
    int  function_type = 9994;
    maxreadthread=5;
    read_block_length=20000;
    ts_group_length = 1;
    // backoff_time = 1 << 10;
    backoff_multiplier = 1;
    int k_size=0;
    long int labelsize=1;
    int topk=0;
    while (1)
    {
        static struct option long_options[] =  {
            {"use-index", no_argument, 0, 'a'},
            {"initial-lbl-size", required_argument, 0, 'b'},
            {"complete-type", required_argument, 0, 'c'},
            {"dataset", required_argument, 0, 'd'},
            {"total-loaded-leaves", required_argument, 0, 'e'},
            {"flush-limit", required_argument, 0, 'f'},
            {"aggressive-check", no_argument, 0, 'g'},
            {"help", no_argument, 0, 'h'},
            {"initial-fbl-size", required_argument, 0, 'i'},
            {"serial", no_argument, 0, 'j'},
            {"queries-size", required_argument, 0, 'k'},
            {"leaf-size", required_argument, 0, 'l'},
            {"min-leaf-size", required_argument, 0, 'm'},
            {"tight-bound", no_argument, 0, 'n'},
            {"read-thread", required_argument, 0, 'o'},
            {"index-path", required_argument, 0, 'p'},
            {"queries", required_argument, 0, 'q'},
            {"read-block", required_argument, 0, 'r'},
            {"minimum-distance", required_argument, 0, 's'},
            {"timeseries-size", required_argument, 0, 't'},
            {"min-checked-leaves", required_argument, 0, 'u'},
            {"in-memory", no_argument, 0, 'v'},
            {"cpu-type", required_argument, 0, 'w'},
            {"sax-cardinality", required_argument, 0, 'x'},
            {"function-type", required_argument, 0, 'y'},
            {"dataset-size", required_argument, 0, 'z'},
            {"k-size", required_argument, 0, '0'},
            {"knn-label-set", required_argument, 0, '1'},
            {"knn-label-size", required_argument, 0, '2'},
            {"knn", no_argument, 0, '3'},
            {"topk", no_argument, 0, '4'},
            {"ts-group-length", required_argument, 0, '5'},
            {"backoff-power", required_argument, 0, '6'},
            {"dataset-output",required_argument,0,'7'},
            {NULL, 0, NULL, 0}
        };

        /* getopt_long stores the option index here. */
        int option_index = 0;
        int c = getopt_long (argc, argv, "",
                             long_options, &option_index);
        if (c == -1)
            break;
        switch (c)
        {
        	case 'j':
        		serial_scan = 1;
        		break;
            case 'g':
                aggressive_check = 1;
                break;

            case 's':
                minimum_distance = atof(optarg);
                break;

            case 'n':
                tight_bound = 1;
                break;

            case 'e':
                total_loaded_leaves = atoi(optarg);
                break;

            case 'c':
                complete_type = atoi(optarg);
                break;

            case 'q':
                queries = optarg;
                break;

            case 'k':
                queries_size = atoi(optarg);
                break;

            case 'd':
                dataset = optarg;
                break;

            case 'p':
                index_path = optarg;
                break;

            case 'z':
                dataset_size = atoi(optarg);
                break;

            case 't':
                time_series_size = atoi(optarg);
                break;

            case 'x':
                sax_cardinality = atoi(optarg);
                break;

            case 'l':
                leaf_size = atoi(optarg);
                break;

            case 'm':
                min_leaf_size = atoi(optarg);
                break;

            case 'b':
                initial_lbl_size = atoi(optarg);
                break;

            case 'f':
                flush_limit = atoi(optarg);
                break;

            case 'u':
            	min_checked_leaves = atoi(optarg);
            	break;
            case 'w':
                cpu_control_type = atoi(optarg);
                break;

            case 'y':
                function_type = atoi(optarg);
                break;
            case 'i':
                initial_fbl_size = atoi(optarg);
                break;
            case 'o':
                maxreadthread = atoi(optarg);
                break;
            case 'r':
                read_block_length = atoi(optarg);
                break;
            case '0':
                k_size = atoi(optarg); 
                
                break;
            case '1':
                labelset = optarg;
                break;
            case '2':
                labelsize =  atoi(optarg);
            case '3':
               knnlabel=1;
            case '4':
               topk=1;
                break;
            case '5':
                ts_group_length = atoi(optarg);
                break;
            case '6':
                if (atoi(optarg) == -1){
                    // backoff_time = 0;
                    backoff_multiplier = 0;
                    // printf ("backoff_time = [%d] --> [2^%d]\n", backoff_time, atoi(optarg));
                }
                else {
                    // backoff_time = 1 << atoi(optarg);
                    backoff_multiplier = atoi(optarg);
                    // printf ("backoff_time = [%d] --> [2^%d]\n", backoff_time, atoi(optarg));
                }
                break;
            case '7':
                dataset_output = optarg;
                break;
            case 'h':
                printf("Usage:\n\
                \t--dataset XX \t\t\tThe path to the dataset file\n\
                \t--queries XX \t\t\tThe path to the queries file\n\
                \t--dataset-size XX \t\tThe number of time series to load\n\
                \t--queries-size XX \t\tThe number of queries to do\n\
                \t--minimum-distance XX\t\tThe minimum distance we search (MAX if not set)\n\
                \t--use-index  \t\t\tSpecifies that an input index will be used\n\
                \t--index-path XX \t\tThe path of the output folder\n\
                \t--timeseries-size XX\t\tThe size of each time series\n\
                \t--sax-cardinality XX\t\tThe maximum sax cardinality in number of bits (power of two).\n\
                \t--leaf-size XX\t\t\tThe maximum size of each leaf\n\
                \t--min-leaf-size XX\t\tThe minimum size of each leaf\n\
                \t--initial-lbl-size XX\t\tThe initial lbl buffer size for each buffer.\n\
                \t--flush-limit XX\t\tThe limit of time series in memory at the same time\n\
                \t--initial-fbl-size XX\t\tThe initial fbl buffer size for each buffer.\n\
                \t--complete-type XX\t\t0 for no complete, 1 for serial, 2 for leaf\n\
                \t--total-loaded-leaves XX\tNumber of leaves to load at each fetch\n\
                \t--min-checked-leaves XX\t\tNumber of leaves to check at minimum\n\
                \t--tight-bound XX\tSet for tight bounds.\n\
                \t--aggressive-check XX\t\tSet for aggressive check\n\
                \t--serial\t\t\tSet for serial scan\n\
                \t--in-memory\t\t\tSet for in-memory search\n\
                \t--function-type\t\t\tSet for query answering type on disk\n\
                                \t\t\tADS+: 0\n\
                \t\t\tParIS+: 1\n\
                \t\t\tnb-ParIS+: 2\n\n\
                \t\t\tin memory  traditional exact search: 0\n\
                \t\t\tADS+: 1\n\
                \t\t\tParIS-TS: 2\n\
                \t\t\tParIS: 4\n\
                \t\t\tParIS+: 6\n\
                \t\t\t\\MESSI-Hq: 7\n\
                \t\t\t\\MESSI-Sq: 8\n\
                \t--cpu-type\t\t\tSet for how many cores you want to used and in 1 or 2 cpu\n\
                \t--help\n\n\
                \tCPU type code:\t\t\t21 : 2 core in 1 CPU\n\
                \t\t\t\t\t22 : 2 core in 2 CPUs\n\
                \t\t\t\t\t41 : 4 core in 1 CPU\n\
                \t\t\t\t\t42 : 4 core in 2 CPUs\n\
                \t\t\t\t\t61 : 6 core in 1 CPU\n\
                \t\t\t\t\t62 : 6 core in 2 CPUs\n\
                \t\t\t\t\t81 : 8 core in 1 CPU\n\
                \t\t\t\t\t82 : 8 core in 2 CPUs\n\
                \t\t\t\t\t101: 10 core in 1 CPU\n\
                \t\t\t\t\t102: 10 core in 2 CPUs\n\
                \t\t\t\t\t121: 12 core in 1 CPU\n\
                \t\t\t\t\t122: 12 core in 2 CPUs\n\
                \t\t\t\t\t181: 18 core in 1 CPU\n\
                \t\t\t\t\t182: 18 core in 2 CPUs\n\
                \t\t\t\t\t242: 24 core in 2 CPUs\n\
                \t\t\t\t\tOther: 1 core in 1 CPU\n\
                \t--topk\t\t\tSet for topk search\n\
                \t--knn\t\t\tSet for knn search\n");
                return 0;
                break;
            case 'a':
                use_index = 1;
                break;
            case 'v':
                inmemory_flag = 1;
                break;
            default:
                exit(-1);
                break;
        }
    }
    INIT_STATS();

    maxquerythread = cpu_control_type;         

    char rm_command[256];

	isax_index_settings * index_settings = isax_index_settings_init(    index_path,         // INDEX DIRECTORY
                                                                        time_series_size,   // TIME SERIES SIZE
	                                                                    paa_segments,       // PAA SEGMENTS
	                                                                    sax_cardinality,    // SAX CARDINALITY IN BITS
	                                                                    leaf_size,          // LEAF SIZE
	                                                                    min_leaf_size,      // MIN LEAF SIZE
	                                                                    initial_lbl_size,   // INITIAL LEAF BUFFER SIZE
	                                                                    flush_limit,        // FLUSH LIMIT
	                                                                    initial_fbl_size,   // INITIAL FBL BUFFER SIZE
	                                                                    total_loaded_leaves,// Leaves to load at each fetch
																		tight_bound,		// Tightness of leaf bounds
																		aggressive_check,	// aggressive check
																		1,                  
                                                                        inmemory_flag);		// new index
	
    
    isax_index *idx;

    
    COUNT_TOTAL_TIME_START
    if (function_type == 0) {   // Botao's version of MESSI
        idx = isax_index_init_inmemory(index_settings);
        // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
        index_creation_pRecBuf_new(dataset, dataset_size, idx);                     // MESSI: paralllel in memory index creation

        // PHASE 3: Query Answering
        // isax_knn_query_binary_file_traditional(queries,labelset, queries_size, idx, minimum_distance, min_checked_leaves, k_size, 2000, &exact_topk_MESSImq_inmemory);
        isax_query_binary_file_traditional(queries, queries_size, idx, minimum_distance, min_checked_leaves, &exact_search_ParISnew_inmemory_hybrid);
    }
    else {  // All ekosmas versions
        idx = isax_index_init_inmemory_ekosmas(index_settings);

        // ------------------------------------------
        // ekosmas's version according to botao's
        if (function_type == 9990){                                     
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            isax_query_binary_file_traditional_ekosmas(queries, queries_size, idx, minimum_distance, &exact_search_ParISnew_inmemory_hybrid_ekosmas);
        }
        
        // ------------------------------------------
        // ekosmas's version according to botao's + fine-grained blocking parallelism when creating subtrees
        else if (function_type == 99904){                               
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_MESSI_with_enhanced_blocking_parallelism(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            isax_query_binary_file_traditional_ekosmas(queries, queries_size, idx, minimum_distance, &exact_search_ParISnew_inmemory_hybrid_ekosmas);
        }

        // ------------------------------------------
        // ekosmas's embarrassingly parallel version
        else if (function_type == 9991){                                
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_EP(dataset, dataset_size, idx);

            // PHASE 3: Query Answering            
            isax_query_binary_file_traditional_ekosmas_EP(queries, queries_size, idx, minimum_distance, &exact_search_ParISnew_inmemory_hybrid_ekosmas_EP);
        }

        // ------------------------------------------
        // ekosmas's lock-free full fai version, with tree copy (i.e. no parallelism) on subtrees
        else if (function_type == 9992 || function_type == 99929){      
            DO_NOT_HELP = 0;
            if (function_type == 99929) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_full_fai(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            // isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, NO_PARALLELISM_IN_SUBTREE, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
        // ekosmas's lock-free fai only after help version, with tree copy (i.e. no parallelism) on subtrees
        else if (function_type == 9993 || function_type == 99939){      
            DO_NOT_HELP = 0;
            if (function_type == 99939) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            // isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, NO_PARALLELISM_IN_SUBTREE, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }

        // ------------------------------------------
        // ekosmas's lock-free full fai version with blocking parallelism in subtree
        else if (function_type == 9994 || function_type == 99949){      
            DO_NOT_HELP = 0;
            if (function_type == 99949) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_blocking_parallelism_in_subtree(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            // isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, BLOCKING_PARALLELISM_IN_SUBTREE, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
        // ekosmas's lock-free fai only after help version with blocking parallelism in subtree
        else if (function_type == 9995 || function_type == 99959){      
            DO_NOT_HELP = 0;
            if (function_type == 99959) {
                DO_NOT_HELP = 1;
            }            
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_blocking_parallelism_in_subtree(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, BLOCKING_PARALLELISM_IN_SUBTREE, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }

        // ------------------------------------------
        // ekosmas's lock-free full fai version with lock-free parallelism in subtree and announce
        else if (function_type == 9996 || function_type == 99969){      
            DO_NOT_HELP = 0;
            if (function_type == 99969) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            // isax_query_binary_file_traditional_ekosmas_lf(queries, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
        // ekosmas's lock-free fai only after help version with lock-free parallelism in subtree and announce
        else if (function_type == 9997 || function_type == 99979){      
            DO_NOT_HELP = 0;
            if (function_type == 99979) {
                DO_NOT_HELP = 1;
            }            
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            // isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }

        // ------------------------------------------
        // ekosmas's lock-free full fai version with lock-free parallelism in subtree and announce, only after helping and per subtree
        else if (function_type == 99966 || function_type == 999669){      
            DO_NOT_HELP = 0;
            if (function_type == 999669) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            // isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
        // ekosmas's lock-free fai only after help version with lock-free parallelism in subtree and announce, only after helping and per subtree
        else if (function_type == 99977 || function_type == 999779){      
            DO_NOT_HELP = 0;
            if (function_type == 999779) {
                DO_NOT_HELP = 1;
            }            
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            // isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }

        // ------------------------------------------
        // ekosmas's lock-free full fai version with lock-free parallelism in subtree and announce, only after helping and per leaf
        else if (function_type == 99666 || function_type == 996669){      
            DO_NOT_HELP = 0;
            if (function_type == 996669) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            // isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
        // ekosmas's lock-free fai only after help version with lock-free parallelism in subtree and announce, only after helping and per leaf
        else if (function_type == 99777 || function_type == 997779){      
            DO_NOT_HELP = 0;
            if (function_type == 997779) {
                DO_NOT_HELP = 1;
            }            
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            // isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }

        // ------------------------------------------
        // ekosmas's lock-free full fai version with lock-free parallelism in subtree and announce, only after helping and per leaf
        else if (function_type == 99966601 || function_type == 99966602 || function_type == 99966603 || 
                 function_type == 99966604 || function_type == 99966605 || function_type == 99966606 || 
                 function_type == 99966607 || function_type == 99966608 || 
                 function_type == 99966691 || function_type == 99966692 || function_type == 99966693 || 
                 function_type == 99966694 || function_type == 99966695 || function_type == 99966696 || 
                 function_type == 99966697 || function_type == 99966698){      
            DO_NOT_HELP = 0;
            if ((function_type%100)/10 == 9) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF, function_type%10, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
        // ekosmas's lock-free full fai version with lock-free parallelism in subtree and announce, only after helping and per leaf
        else if (function_type == 999666010 || function_type == 999666011 || function_type == 999666012 || function_type == 999666013 ||
                 function_type == 999666910 || function_type == 999666911 || function_type == 999666912 || function_type == 999666913) {      
            DO_NOT_HELP = 0;
            if ((function_type%1000)/100 == 9) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF, function_type%100, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
        // ekosmas's lock-free fai only after help version with lock-free parallelism in subtree and announce, only after helping and per leaf
        else if (function_type == 99977701 || function_type == 99977702 || function_type == 99977703 || 
                 function_type == 99977704 || function_type == 99977705 || function_type == 99977706 || 
                 function_type == 99977707 || function_type == 99977708 || 
                 function_type == 99977791 || function_type == 99977792 || function_type == 99977793 || 
                 function_type == 99977794 || function_type == 99977795 || function_type == 99977796 || 
                 function_type == 99977797 || function_type == 99977798){      
            DO_NOT_HELP = 0;
            if ((function_type%100)/10 == 9) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF, function_type%10, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
        // ekosmas's lock-free full fai version with lock-free parallelism in subtree and announce, only after helping and per leaf
        else if (function_type == 999777010 || function_type == 999777011 || function_type == 999777012 || function_type == 999777013 ||
                 function_type == 999777910 || function_type == 999777911 || function_type == 999777912 || function_type == 999777913) {      
            DO_NOT_HELP = 0;
            if ((function_type%1000)/100 == 9) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF, function_type%100, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }

        // ------------------------------------------
        // ekosmas's lock-free full fai version with lock-free parallelism in subtree and cow
        else if (function_type == 9998 || function_type == 99989){      
            DO_NOT_HELP = 0;
            if (function_type == 99989) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_cow(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_COW, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
        // ekosmas's lock-free fai only after help version with lock-free parallelism in subtree and cow
        else if (function_type == 9999 || function_type == 99999){      
            DO_NOT_HELP = 0;
            if (function_type == 99999) {
                DO_NOT_HELP = 1;
            }            
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_cow(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_COW, 0, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
        else {  // Something went wrong!
            printf("ERROR: Version [%d] of MESSI does not exist!\n", function_type);
        }
    }
    
    COUNT_TOTAL_TIME_END
    MY_PRINT_STATS(0.00f)
    
    char filename[100];
    sprintf(filename, "results/results_[%d].txt", dataset_size);
    FILE * fp;
    fp = fopen (filename, "a");
    PRINT_STATS_TO_FILE(0.00f, fp)
    fclose(fp);

    return 0;
}