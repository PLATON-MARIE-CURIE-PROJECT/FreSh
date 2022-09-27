//
//  isax_node.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/10/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>

#include "ads/isax_node.h"

/**
 This function initializes an isax root node.
 */
isax_node * isax_root_node_init(root_mask_type mask, int initial_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, current_fbl_node);
    node->mask = mask;
    return node;
}

isax_node * isax_root_node_init_lockfree_announce(root_mask_type mask, int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init_lockfree_announce(initial_buffer_size, total_workers_num, lockfree_parallelism_in_subtree, current_fbl_node);
    node->mask = mask;
    return node;
}

isax_node * isax_root_node_init_lockfree_announce_copy(isax_node *old_node, root_mask_type mask, int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init_lockfree_announce_copy(old_node, initial_buffer_size, total_workers_num, lockfree_parallelism_in_subtree, current_fbl_node);
    node->mask = mask;
    return node;
}

isax_node * isax_root_node_init_lockfree_cow(root_mask_type mask, int initial_buffer_size) 
{
    isax_node *node = isax_leaf_node_init_lockfree_cow(initial_buffer_size);
    node->mask = mask;
    return node;
}

isax_node * isax_root_node_init_lockfree_cow_copy(isax_node *old_node, root_mask_type mask, int initial_buffer_size) 
{
    isax_node *node = isax_leaf_node_init_lockfree_cow_copy(old_node, initial_buffer_size);
    node->mask = mask;
    return node;
}


/**
 This function initalizes an isax leaf node.
 */
isax_node * isax_leaf_node_init(int initial_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    // COUNT_NEW_NODE()                                                       
    isax_node *node = malloc(sizeof(isax_node));
    if(node == NULL) {
        fprintf(stderr,"error: could not allocate memory for new node.\n");
        return NULL;
    }
    node->has_partial_data_file = 0;
    node->has_full_data_file = 0;
    node->right_child = NULL;
    node->left_child = NULL;
    node->parent = NULL;
    node->next = NULL;
    node->leaf_size = 0;
    node->filename = NULL;
    node->isax_values = NULL;
    node->isax_cardinalities = NULL;
    node->previous = NULL;
    node->split_data = NULL;
    node->buffer = init_node_buffer(initial_buffer_size);
    node->fai_leaf_size = 0;                                                    
    node->mask = 0;
    node->wedges = NULL;
    node->is_leaf = 0;
    node->lightweight_path = 0;
    node->announce_array = NULL;
    node->recBuf_leaf_helpers_exist = 0;
    node->fbl_node = current_fbl_node;
    node->processed = 0;
    node->is_pruned = 0;
    node->lock_node = NULL;                                                     

    return node;
}

isax_node * isax_leaf_node_init_lockfree_announce(int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init_lockfree_announce_copy(NULL, initial_buffer_size, total_workers_num, lockfree_parallelism_in_subtree, current_fbl_node);

    node->buffer->partial_position_buffer = calloc(node->buffer->initial_buffer_size, sizeof(file_position_type*));
    node->buffer->partial_sax_buffer = calloc(node->buffer->initial_buffer_size, sizeof(sax_type*));
    node->is_leaf = 1;

    return node;
}

isax_node * isax_leaf_node_init_lockfree_announce_copy(isax_node *old_node, int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, current_fbl_node);

    if (old_node && old_node->isax_values) {
        node->isax_values = old_node->isax_values;
        node->isax_cardinalities= old_node->isax_cardinalities;
    }

    if (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {
        node->announce_array = calloc(total_workers_num, sizeof(announce_rec *));
    }

    return node;
}

announce_rec *create_new_announce_rec(isax_node_record *record) {
    announce_rec *new_rec = malloc (sizeof(announce_rec));
    new_rec->record.sax = record->sax;
    new_rec->record.position = record->position;
    new_rec->buf_pos = ULONG_MAX;
    return (new_rec);
}

isax_node * isax_leaf_node_init_lockfree_cow(int initial_buffer_size) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, NULL);
    node->is_leaf = 1;

    return node;
}

isax_node * isax_leaf_node_init_lockfree_cow_copy(isax_node *old_node, int initial_buffer_size) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, NULL);

    if (old_node && old_node->isax_values) {
        node->isax_values = old_node->isax_values;
        node->isax_cardinalities= old_node->isax_cardinalities;
    }

    return node;
}

isax_node * isax_leaf_node_init_lockfree_cow_split(int initial_buffer_size) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, NULL);
    node->is_leaf = 1;

    node->buffer->partial_position_buffer = malloc(initial_buffer_size * sizeof(file_position_type*));
    node->buffer->partial_sax_buffer = malloc(initial_buffer_size * sizeof(sax_type*));

    return node;
}