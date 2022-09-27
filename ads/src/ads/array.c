#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <float.h>
#include "ads/array.h"

void initArrayList(array_list_t *List, int size) 
{
	List->Top = malloc(sizeof(array_list_node_t));
	List->Top->data = calloc(size, sizeof(array_element_t));
	List->Top->num_node = 0;
	List->Top->next = NULL;
	List->element_size = size;
}


array_list_node_t *add_array(array_list_node_t *last, array_list_t *List) 
{
	array_list_node_t *tmp = malloc (sizeof(array_list_node_t));
	tmp->data = calloc(List->element_size, sizeof(array_element_t));
	tmp->num_node = last->num_node+1;
	tmp->next = last;
	
	if (!CASPTR(&List->Top, last, tmp)) {
		free (tmp);
		tmp = List->Top;
		while (tmp->num_node > last->num_node+1) {
			tmp = tmp->next;
		}
	}

	return tmp;
}

array_element_t *get_element_at(int position, array_list_t *List) {
	array_list_node_t *array_node;
	int array_num_node = position/List->element_size;
	array_list_node_t *last = List->Top; 

	// it can only be array_num_node <= last->num_node + 1
	if (array_num_node == last->num_node) { 
		array_node = last;	
	}
	else if (array_num_node > last->num_node) {		
		array_node = add_array(last, List);
	}
	else {
        array_node= List->Top;
		while (array_node->num_node > array_num_node) {
			array_node = array_node->next;
		} 
	}

	return &(array_node->data[position%List->element_size]);
}