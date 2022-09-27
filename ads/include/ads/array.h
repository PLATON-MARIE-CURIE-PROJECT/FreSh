//  Created by Eleftherios Kosmas on 04/08/22.


/**
 * @file  array.h
 * @brief Array declarations
 */
#include "ads/isax_query_engine.h"

#ifndef FRESH_ARRAY_H
#define FRESH_ARRAY_H

typedef struct array_element {
    float distance;						// EKOSMAS 15 SEPTEMBER 2020: During lock-free versions, this changes to negative (-1) to inform that this queue node has been processed
    // int dummy;
    isax_node *node;
} array_element_t;

typedef struct array_list_node {
	array_element_t *data;
	int num_node;
	// int dummy;
	struct array_list_node *next;
} array_list_node_t;

typedef struct array_list {
	array_list_node_t *Top;
	int element_size;
} array_list_t;

typedef struct sorted_array {
	array_element_t *data;
	int num_elements;
	// int dummy;
} sorted_array_t;

void initArrayList(array_list_t *List, int size);
array_list_node_t *add_array(array_list_node_t *last, array_list_t *List);
array_element_t *get_element_at(int position, array_list_t *List);

#endif /* FRESH_ARRAY_H */
