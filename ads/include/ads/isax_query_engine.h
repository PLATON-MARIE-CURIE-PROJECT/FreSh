//
//  isax_query_engine.h
//  al_isax
//
//  Created by Kostas Zoumpatianos on 4/13/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#ifndef al_isax_isax_query_engine_h
#define al_isax_isax_query_engine_h
#include "../../config.h"
#include "../../globals.h"
#include "isax_index.h"
#include "isax_node.h"
#include "pqueue.h"

typedef struct query_result {
    float distance;						// EKOSMAS 15 SEPTEMBER 2020: During lock-free versions, this changes to negative (-1) to inform that this queue node has been processed
    isax_node *node;
    size_t pqueue_position;
} query_result;

static int
cmp_pri(double next, double curr)
{
	return (next > curr);
}


static double
get_pri(void *a)
{
	return (double) ((query_result *) a)->distance;
}


static void
set_pri(void *a, double pri)
{
	((query_result *) a)->distance = (float)pri;
}


static size_t
get_pos(void *a)
{
	return ((query_result *) a)->pqueue_position;
}


static void
set_pos(void *a, size_t pos)
{
	((query_result *) a)->pqueue_position = pos;
}

#endif
