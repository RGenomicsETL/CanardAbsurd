#define _POSIX_C_SOURCE 200809L

#include "canard_thread.h"

#include <stdlib.h>
#include <time.h>

typedef struct {
	canard_thread_function function;
	void *argument;
} canard_thread_start_data;

bool canard_mutex_init(canard_mutex *mutex) {
	return pthread_mutex_init(mutex, NULL) == 0;
}

void canard_mutex_destroy(canard_mutex *mutex) {
	(void)pthread_mutex_destroy(mutex);
}

void canard_mutex_lock(canard_mutex *mutex) {
	(void)pthread_mutex_lock(mutex);
}

void canard_mutex_unlock(canard_mutex *mutex) {
	(void)pthread_mutex_unlock(mutex);
}

bool canard_condition_init(canard_condition *condition) {
	return pthread_cond_init(condition, NULL) == 0;
}

void canard_condition_destroy(canard_condition *condition) {
	(void)pthread_cond_destroy(condition);
}

void canard_condition_broadcast(canard_condition *condition) {
	(void)pthread_cond_broadcast(condition);
}

void canard_condition_wait(canard_condition *condition, canard_mutex *mutex) {
	(void)pthread_cond_wait(condition, mutex);
}

void canard_condition_wait_milliseconds(canard_condition *condition, canard_mutex *mutex, uint64_t milliseconds) {
	struct timespec deadline;
	(void)clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += (time_t)(milliseconds / 1000);
	deadline.tv_nsec += (long)((milliseconds % 1000) * 1000000);
	if (deadline.tv_nsec >= 1000000000L) {
		deadline.tv_sec += 1;
		deadline.tv_nsec -= 1000000000L;
	}
	(void)pthread_cond_timedwait(condition, mutex, &deadline);
}

static void *canard_thread_entry(void *argument) {
	canard_thread_start_data *start = (canard_thread_start_data *)argument;
	canard_thread_function function = start->function;
	void *function_argument = start->argument;
	free(start);
	function(function_argument);
	return NULL;
}

bool canard_thread_start(canard_thread *thread, canard_thread_function function, void *argument) {
	canard_thread_start_data *start = (canard_thread_start_data *)malloc(sizeof(canard_thread_start_data));
	if (!start) {
		return false;
	}
	start->function = function;
	start->argument = argument;
	if (pthread_create(thread, NULL, canard_thread_entry, start) != 0) {
		free(start);
		return false;
	}
	return true;
}

void canard_thread_join(canard_thread thread) {
	(void)pthread_join(thread, NULL);
}
