//
//  isax_node_buffer.c
//  aisax
//
//  Created by Kostas Zoumpatianos on 4/6/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "ads/isax_node_buffer.h"
#include "ads/isax_node.h"
#include "ads/isax_node_record.h"

void destroy_node_buffer(isax_node_buffer *node_buffer) {
    if (node_buffer->full_position_buffer != NULL) {
        free(node_buffer->full_position_buffer);
        node_buffer->full_position_buffer = NULL;
    }
    if (node_buffer->full_sax_buffer != NULL) {
        free(node_buffer->full_sax_buffer);
        node_buffer->full_sax_buffer = NULL;
    }
    if (node_buffer->full_ts_buffer != NULL) {
        free(node_buffer->full_ts_buffer);
        node_buffer->full_ts_buffer = NULL;
    }
    if (node_buffer->partial_position_buffer != NULL) {
        // !!! DON'T FREE THAT IT REMOVES THE DATA!!!!
        free(node_buffer->partial_position_buffer);
        node_buffer->partial_position_buffer = NULL;
    }
    if (node_buffer->partial_sax_buffer != NULL) {
        free(node_buffer->partial_sax_buffer);
        node_buffer->partial_sax_buffer = NULL;
    }
    if (node_buffer->tmp_full_position_buffer != NULL) {
        free(node_buffer->tmp_full_position_buffer);
        node_buffer->tmp_full_position_buffer = NULL;
    }
    if (node_buffer->tmp_full_sax_buffer != NULL) {
        free(node_buffer->tmp_full_sax_buffer);
        node_buffer->tmp_full_sax_buffer = NULL;
    }
    if (node_buffer->tmp_full_ts_buffer != NULL) {
        free(node_buffer->tmp_full_ts_buffer);
        node_buffer->tmp_full_ts_buffer = NULL;
    }
    if (node_buffer->tmp_partial_position_buffer != NULL) {
        free(node_buffer->tmp_partial_position_buffer);
        node_buffer->tmp_partial_position_buffer = NULL;
    }
    if (node_buffer->tmp_partial_sax_buffer != NULL) {
        free(node_buffer->tmp_partial_sax_buffer);
        node_buffer->tmp_partial_sax_buffer = NULL;
    }
    free(node_buffer);
}


isax_node_buffer * init_node_buffer(int initial_buffer_size) {
    isax_node_buffer * node_buffer = malloc(sizeof(isax_node_buffer));
    node_buffer->initial_buffer_size = initial_buffer_size;
    
    node_buffer->max_full_buffer_size = 0;
    node_buffer->max_partial_buffer_size = 0;
    node_buffer->max_tmp_full_buffer_size = 0;
    node_buffer->max_tmp_partial_buffer_size = 0;
    node_buffer->full_buffer_size = 0;
    node_buffer->partial_buffer_size = 0;
    node_buffer->tmp_full_buffer_size = 0;
    node_buffer->tmp_partial_buffer_size = 0;
    
    (node_buffer->full_position_buffer) = NULL;
    (node_buffer->full_sax_buffer) = NULL;
    (node_buffer->full_ts_buffer) = NULL;
    (node_buffer->partial_position_buffer) = NULL;
    (node_buffer->partial_sax_buffer) = NULL;
    (node_buffer->tmp_full_position_buffer) = NULL;
    (node_buffer->tmp_full_sax_buffer) = NULL;
    (node_buffer->tmp_full_ts_buffer) = NULL;
    (node_buffer->tmp_partial_position_buffer) = NULL;
    (node_buffer->tmp_partial_sax_buffer = NULL);   
    
    return node_buffer;
}

isax_node_buffer *clone_buffer_and_add_record_lockfree_cow(isax_node_buffer *buffer, isax_node_record *record) {   

    // create new buffer
    isax_node_buffer *new_buffer = init_node_buffer(buffer->initial_buffer_size);                   


    // allocate memory
    int buffer_size = buffer->partial_buffer_size;
    new_buffer->partial_position_buffer = malloc((buffer_size+1) * sizeof(file_position_type*));
    new_buffer->partial_sax_buffer = malloc((buffer_size+1) * sizeof(sax_type*));

    // clone existing buffer
    for (int i=0; i<buffer_size; i++) {
        new_buffer->partial_position_buffer[i] = buffer->partial_position_buffer[i];
        new_buffer->partial_sax_buffer[i] = buffer->partial_sax_buffer[i];
    }

    // add new record
    add_to_node_buffer_lockfree(new_buffer, record, buffer_size);

    // Increment buffer size
    new_buffer->partial_buffer_size = buffer_size+1;

    return new_buffer;
}

enum response add_to_node_buffer(isax_node_buffer *node_buffer, 
                                 isax_node_record *record, 
                                 int sax_segments, int ts_segments)
{
    if (record->insertion_mode & TMP) 
    {
        if (record->insertion_mode & FULL) {
            if (node_buffer->max_tmp_full_buffer_size == 0) {
                node_buffer->max_tmp_full_buffer_size = node_buffer->initial_buffer_size;
                node_buffer->tmp_full_position_buffer = malloc(sizeof(file_position_type*) * 
                                                               node_buffer->max_tmp_full_buffer_size);
                node_buffer->tmp_full_sax_buffer = malloc(sizeof(sax_type*) * 
                                                          node_buffer->max_tmp_full_buffer_size);
                node_buffer->tmp_full_ts_buffer = malloc(sizeof(ts_type*) * 
                                                         node_buffer->max_tmp_full_buffer_size);
            }
            else if (node_buffer->max_tmp_full_buffer_size <= node_buffer->tmp_full_buffer_size) {
                node_buffer->max_tmp_full_buffer_size *= BUFFER_REALLOCATION_RATE;
                node_buffer->tmp_full_position_buffer = realloc(node_buffer->tmp_full_position_buffer,
                                                                sizeof(file_position_type*) * 
                                                                node_buffer->max_tmp_full_buffer_size);
                node_buffer->tmp_full_sax_buffer = realloc(node_buffer->tmp_full_sax_buffer,
                                                           sizeof(sax_type*) * 
                                                           node_buffer->max_tmp_full_buffer_size);
                node_buffer->tmp_full_ts_buffer = realloc(node_buffer->tmp_full_ts_buffer,
                                                          sizeof(ts_type*) * 
                                                          node_buffer->max_tmp_full_buffer_size);
            }
            node_buffer->tmp_full_position_buffer[node_buffer->tmp_full_buffer_size] = record->position;
            node_buffer->tmp_full_sax_buffer[node_buffer->tmp_full_buffer_size] = record->sax;
            node_buffer->tmp_full_ts_buffer[node_buffer->tmp_full_buffer_size] = record->ts;
            node_buffer->tmp_full_buffer_size++;
        }
        if (record->insertion_mode & PARTIAL) {
            if (node_buffer->max_tmp_partial_buffer_size == 0) {
                node_buffer->max_tmp_partial_buffer_size = node_buffer->initial_buffer_size;
                node_buffer->tmp_partial_position_buffer = malloc(sizeof(file_position_type*) * 
                                                               node_buffer->max_tmp_partial_buffer_size);
                node_buffer->tmp_partial_sax_buffer = malloc(sizeof(sax_type*) * 
                                                          node_buffer->max_tmp_partial_buffer_size);
            }
            else if (node_buffer->max_tmp_partial_buffer_size <= node_buffer->tmp_partial_buffer_size) {
                node_buffer->max_tmp_partial_buffer_size *= BUFFER_REALLOCATION_RATE;
                node_buffer->tmp_partial_position_buffer = realloc(node_buffer->tmp_full_position_buffer,
                                                                sizeof(file_position_type*) * 
                                                                node_buffer->max_tmp_partial_buffer_size);
                node_buffer->tmp_partial_sax_buffer = realloc(node_buffer->tmp_full_sax_buffer,
                                                           sizeof(sax_type*) * 
                                                           node_buffer->max_tmp_partial_buffer_size);
            }
            node_buffer->tmp_partial_position_buffer[node_buffer->tmp_partial_buffer_size] = record->position;
            node_buffer->tmp_partial_sax_buffer[node_buffer->tmp_partial_buffer_size] = record->sax;
            node_buffer->tmp_partial_buffer_size++;
        }
    }
    else if (record->insertion_mode & NO_TMP)
    {
        if (record->insertion_mode & FULL) {
            if (node_buffer->max_full_buffer_size == 0) {
                node_buffer->max_full_buffer_size = node_buffer->initial_buffer_size;
                node_buffer->full_position_buffer = malloc(sizeof(file_position_type*) * 
                                                           node_buffer->max_full_buffer_size);
                node_buffer->full_sax_buffer = malloc(sizeof(sax_type*) * 
                                                      node_buffer->max_full_buffer_size);
                node_buffer->full_ts_buffer = malloc(sizeof(ts_type*) * 
                                                     node_buffer->max_full_buffer_size);
            }
            else if (node_buffer->max_full_buffer_size <= node_buffer->full_buffer_size) {
                node_buffer->max_full_buffer_size *= BUFFER_REALLOCATION_RATE;
                node_buffer->full_position_buffer = realloc(node_buffer->full_position_buffer,
                                                            sizeof(file_position_type*) * 
                                                            node_buffer->max_full_buffer_size);
                node_buffer->full_sax_buffer = realloc(node_buffer->full_sax_buffer,
                                                       sizeof(sax_type*) * 
                                                       node_buffer->max_full_buffer_size);
                node_buffer->full_ts_buffer = realloc(node_buffer->full_ts_buffer,
                                                      sizeof(ts_type*) * 
                                                      node_buffer->max_full_buffer_size);
            }
            node_buffer->full_position_buffer[node_buffer->full_buffer_size] = record->position;
            node_buffer->full_sax_buffer[node_buffer->full_buffer_size] = record->sax;
            node_buffer->full_ts_buffer[node_buffer->full_buffer_size] = record->ts;
            node_buffer->full_buffer_size++;
        }
        if (record->insertion_mode & PARTIAL) {
            if (node_buffer->max_partial_buffer_size == 0) {
                node_buffer->max_partial_buffer_size = node_buffer->initial_buffer_size;
                node_buffer->partial_position_buffer = malloc(sizeof(file_position_type*) * 
                                                              node_buffer->max_partial_buffer_size);
                node_buffer->partial_sax_buffer = malloc(sizeof(sax_type*) * 
                                                         node_buffer->max_partial_buffer_size);
            }
            else if (node_buffer->max_partial_buffer_size <= node_buffer->partial_buffer_size) {
                node_buffer->max_partial_buffer_size *= BUFFER_REALLOCATION_RATE;
                node_buffer->partial_position_buffer = realloc(node_buffer->partial_position_buffer,
                                                               sizeof(file_position_type*) * 
                                                               node_buffer->max_partial_buffer_size);
                node_buffer->partial_sax_buffer = realloc(node_buffer->partial_sax_buffer,
                                                          sizeof(sax_type*) * 
                                                          node_buffer->max_partial_buffer_size);
            }
            node_buffer->partial_position_buffer[node_buffer->partial_buffer_size] = record->position;
            node_buffer->partial_sax_buffer[node_buffer->partial_buffer_size] = record->sax;
            node_buffer->partial_buffer_size++;
        } 
    }
    
    return SUCCESS;
}
enum response add_to_node_buffer_lockfree(isax_node_buffer *node_buffer, 
                                 isax_node_record *record, 
                                 unsigned long next_buf_pos)
{
    node_buffer->partial_position_buffer[next_buf_pos] = record->position;
    node_buffer->partial_sax_buffer[next_buf_pos] = record->sax;
    
    return SUCCESS;
}
