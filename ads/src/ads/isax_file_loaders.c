//
//  isax_file_loaders.c
//  isax
//
//  Created by Kostas Zoumpatianos on 4/7/12.
//  Copyright 2012 University of Trento. All rights reserved.
//


#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <float.h>
#include <unistd.h>
#include <math.h>

#include "ads/isax_node.h"
#include "ads/isax_index.h"
#include "ads/isax_query_engine.h"
#include "ads/isax_node_record.h"
#include "ads/isax_file_loaders.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/inmemory_index_engine.h"


// Botao's version
void isax_query_binary_file_traditional(const char *ifilename, int q_num, isax_index *index,
                            float minimum_distance, int min_checked_leaves,
                            query_result (*search_function)(ts_type*, ts_type*, isax_index*,node_list*, float, int)) 
{
    COUNT_INPUT_TIME_START
    fprintf(stderr, ">>> Performing queries in file: %s\n", ifilename);
    FILE * ifile;
    ifile = fopen (ifilename,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < q_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    int q_loaded = 0; 
    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_QUERY_ANSWERING_TIME_START
    ts_type * paa = malloc(sizeof(ts_type) * index->settings->paa_segments);
    //sax_type * sax = malloc(sizeof(sax_type) * index->settings->paa_segments);

    node_list nodelist;
    nodelist.nlist=malloc(sizeof(isax_node*)*pow(2, index->settings->paa_segments));
    nodelist.node_amount=0;
    isax_node *current_root_node = index->first_node;

    while(1)
    {
        if (current_root_node!=NULL)
        {
            nodelist.nlist[nodelist.node_amount]=current_root_node;
            current_root_node=current_root_node->next;
            nodelist.node_amount++;
        }
        else
        {
            break;
        }
                    
    }
    // printf("the node node_amount is %d\n",nodelist.node_amount ); fflush(stdout);
                
    while (q_loaded < q_num)
    {
        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END

        COUNT_INPUT_TIME_START
        fread(ts, sizeof(ts_type),index->settings->timeseries_size,ifile);
        COUNT_INPUT_TIME_END
        //printf("Querying for: %d\n", index->settings->ts_byte_size * q_loaded);
        

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START
        
        // Parse ts and make PAA representation
        paa_from_ts(ts, paa, index->settings->paa_segments,
                    index->settings->ts_values_per_paa_segment);
        query_result result = search_function(ts, paa, index, &nodelist, minimum_distance, min_checked_leaves);

        fflush(stdout);
    #if VERBOSE_LEVEL >= 1
        printf("[%p]: Distance: %lf\n", result.node, result.distance);
    #endif
        //sax_from_paa(paa, sax, index->settings->paa_segments, index->settings->sax_alphabet_cardinality, index->settings->sax_bit_cardinality);
        //if (index->settings->timeseries_size * sizeof(ts_type) * q_loaded == 1024) {
        //    sax_print(sax, index->settings->paa_segments, index->settings->sax_bit_cardinality);
        //}

        q_loaded++;

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END
        
        PRINT_QUERY_STATS(result.distance)

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START        
    }

    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END
 
    COUNT_INPUT_TIME_START
    free(ts);
    fclose(ifile);
    fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END
}

// ekosmas version
void isax_query_binary_file_traditional_ekosmas(const char *ifilename, int q_num, isax_index *index,
                            float minimum_distance,
                            query_result (*search_function)(ts_type*, ts_type*, isax_index*,node_list*, float)) 
{
    COUNT_INPUT_TIME_START
    fprintf(stderr, ">>> Performing queries in file: %s\n", ifilename);
    FILE * ifile;
    ifile = fopen (ifilename,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < q_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    int q_loaded = 0; 
    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_QUERY_ANSWERING_TIME_START
    ts_type * paa = malloc(sizeof(ts_type) * index->settings->paa_segments);

    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node*)*pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;

    // maintain root nodes into an array, so that it is possible to execute FAI
    for (int j = 0; j < index->fbl->number_of_buffers; j++ ) {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas*)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }

        nodelist.nlist[nodelist.node_amount] = current_fbl_node->node;
        if (!current_fbl_node->node) {
            printf ("Error: node is NULL!!!!\t"); fflush(stdout);
            getchar();
        }
        nodelist.node_amount++;
                    
    }

    while (q_loaded < q_num)
    {
        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END

        COUNT_INPUT_TIME_START
        fread(ts, sizeof(ts_type),index->settings->timeseries_size,ifile);
        COUNT_INPUT_TIME_END

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START

        // Parse ts and make PAA representation
        paa_from_ts(ts, paa, index->settings->paa_segments,
                    index->settings->ts_values_per_paa_segment);
        query_result result = search_function(ts, paa, index, &nodelist, minimum_distance);
        q_loaded++;

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END
        PRINT_QUERY_STATS(result.distance);
        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START
    }
    
    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END

    COUNT_INPUT_TIME_START
    free(ts);
    fclose(ifile);
    fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END
}

// ekosmas Embarrasingly Parallel (EP) version
void isax_query_binary_file_traditional_ekosmas_EP(const char *ifilename, int q_num, isax_index *index,
                            float minimum_distance,
                            query_result (*search_function)(ts_type*, ts_type*, isax_index*,node_list*, float)) 
{
    COUNT_INPUT_TIME_START
    fprintf(stderr, ">>> Performing queries in file: %s\n", ifilename);
    FILE * ifile;
    ifile = fopen (ifilename,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < q_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    int q_loaded = 0; 
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_QUERY_ANSWERING_TIME_START
    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    ts_type * paa = malloc(sizeof(ts_type) * index->settings->paa_segments);

    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node*)*pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;

    // maintain root nodes into an array, so that it is possible to execute FAI
    for (int j = 0; j < index->fbl->number_of_buffers; j++ ) {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas*)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }

        nodelist.nlist[nodelist.node_amount] = current_fbl_node->node;
        nodelist.node_amount++;
                    
    }

    // printf("the node node_amount is %d\n",nodelist.node_amount ); fflush(stdout);

    while (q_loaded < q_num)
    {
        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END

        COUNT_INPUT_TIME_START
        fread(ts, sizeof(ts_type),index->settings->timeseries_size,ifile);
        COUNT_INPUT_TIME_END

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START

        // Parse ts and make PAA representation
        paa_from_ts(ts, paa, index->settings->paa_segments,
                    index->settings->ts_values_per_paa_segment);
        query_result result = search_function(ts, paa, index, &nodelist, minimum_distance);
        q_loaded++;

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END
        PRINT_QUERY_STATS(result.distance);
        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START
    }
    
    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END

    COUNT_INPUT_TIME_START
    free(ts);
    fclose(ifile);
    fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END
}

void reinitialize_index_subtree(isax_node *node)
{
    if (!node) {
        return;
    }

    reinitialize_index_subtree(node->left_child);
    reinitialize_index_subtree(node->right_child);

    node->processed = 0;
}
void reinitialize_index_tree(isax_index *index)
{
    for (int j = 0; j < index->fbl->number_of_buffers; j++ ) {

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }

        reinitialize_index_subtree(current_fbl_node->node);                    
    }
}

// GEOPAT
inline int compare_float(float f1, float f2)
 {
  float precision = 0.0000001;
  if (((f1 - precision) < f2) && 
      ((f1 + precision) > f2))
   {
    return 1;
   }
  else
   {
    return 0;
   }
 }

inline void validate_bsf(const char* compare_file,float bsf,int query_num){

    char buffer[60];
    char* pend;

    if (compare_file == NULL)
        return;
    
    FILE * ifile;
    ifile = fopen (compare_file,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", compare_file);
        exit(-1);
    }
    int current_line = 0;
    int keep_reading = 1;

    do{
        fgets(buffer,60,ifile);

        if(feof(ifile)){
            keep_reading = 0;
            printf("File %d lines\n",current_line);
            printf("Couldn't find line %d\n",query_num);
            exit(-1);
        }
        else if(current_line == query_num){
            keep_reading = false;
            float line = strtof(buffer,&pend);
            if(compare_float(line,bsf)){
                printf("\033[0;31m"); 
                printf("ERROR at line %d, %f != %f\n",query_num,line,bsf);
                printf("\033[0m");
            }
        }
        current_line++;
    }while(keep_reading);

    printf("Success : Query Num = %d\n",query_num);
}

// ekosmas Lock Free version
void isax_query_binary_file_traditional_ekosmas_lf(const char *ifilename, const char *output_file, int q_num, isax_index *index,
                            float minimum_distance, const char parallelism_in_subtree, const int third_phase,
                            query_result (*search_function)(ts_type*, ts_type*, isax_index*,node_list*, float, const char, const int, const int query_id)) 
{
    COUNT_INPUT_TIME_START
    fprintf(stderr, ">>> Performing queries in file: %s\n", ifilename);
    FILE * ifile;
    ifile = fopen (ifilename,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < q_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    unsigned long q_loaded = 1; 
    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_QUERY_ANSWERING_TIME_START
    ts_type * paa = malloc(sizeof(ts_type) * index->settings->paa_segments);

    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node *) * pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;

    // maintain root nodes into an array, so that it is possible to execute FAI
    for (int j = 0; j < index->fbl->number_of_buffers; j++ ) {

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }

        nodelist.nlist[nodelist.node_amount] = current_fbl_node->node;
        nodelist.node_amount++;
                    
    }

    while (q_loaded <= q_num)
    {
        // re-initialized index tree
        // if (q_loaded > 0) { 
        //     COUNT_INITIALIZE_INDEX_TREE_TIME_START
        //     reinitialize_index_tree(index);
        //     COUNT_INITIALIZE_INDEX_TREE_TIME_END
        // }

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END

        COUNT_INPUT_TIME_START
        fread(ts, sizeof(ts_type),index->settings->timeseries_size,ifile);
        COUNT_INPUT_TIME_END

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START

        // Parse ts and make PAA representation
        paa_from_ts(ts, paa, index->settings->paa_segments,
                    index->settings->ts_values_per_paa_segment);
        unique_leaves_in_arrays = 0;
        leaves_in_arrays = 0;
        query_result result = search_function(ts, paa, index, &nodelist, minimum_distance, parallelism_in_subtree, third_phase, q_loaded);
        q_loaded++;

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END

        COUNT_UNIQUE_LEAVES_ARRAY_TOTAL(unique_leaves_in_arrays)
        COUNT_LEAVES_ARRAY_TOTAL(leaves_in_arrays)
        PRINT_QUERY_STATS(result.distance);
        validate_bsf(output_file,result.distance,q_loaded-2);

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START
    }
    
    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END

    COUNT_INPUT_TIME_START
    free(ts);
    fclose(ifile);
    fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END
}