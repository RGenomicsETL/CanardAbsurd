#include "canard_thread.h"

#include <stdlib.h>

#ifndef _WIN64
#error "The Canard coordinator supports 64-bit Windows builds"
#endif

typedef struct {
	canard_thread_function function;
	void *argument;
} canard_thread_start_data;

bool canard_mutex_init(canard_mutex *mutex) {
	InitializeCriticalSection(mutex);
	return true;
}

void canard_mutex_destroy(canard_mutex *mutex) {
	DeleteCriticalSection(mutex);
}

void canard_mutex_lock(canard_mutex *mutex) {
	EnterCriticalSection(mutex);
}

void canard_mutex_unlock(canard_mutex *mutex) {
	LeaveCriticalSection(mutex);
}

bool canard_condition_init(canard_condition *condition) {
	InitializeConditionVariable(condition);
	return true;
}

void canard_condition_destroy(canard_condition *condition) {
	(void)condition;
}

void canard_condition_broadcast(canard_condition *condition) {
	WakeAllConditionVariable(condition);
}

void canard_condition_wait(canard_condition *condition, canard_mutex *mutex) {
	(void)SleepConditionVariableCS(condition, mutex, INFINITE);
}

void canard_condition_wait_milliseconds(canard_condition *condition, canard_mutex *mutex, uint64_t milliseconds) {
	DWORD timeout = milliseconds > UINT32_MAX ? UINT32_MAX : (DWORD)milliseconds;
	(void)SleepConditionVariableCS(condition, mutex, timeout);
}

static DWORD canard_thread_entry(LPVOID argument) {
	canard_thread_start_data *start = (canard_thread_start_data *)argument;
	canard_thread_function function = start->function;
	void *function_argument = start->argument;
	free(start);
	function(function_argument);
	return 0;
}

bool canard_thread_start(canard_thread *thread, canard_thread_function function, void *argument) {
	canard_thread_start_data *start = (canard_thread_start_data *)malloc(sizeof(canard_thread_start_data));
	if (!start) {
		return false;
	}
	start->function = function;
	start->argument = argument;
	*thread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)canard_thread_entry, start, 0, NULL);
	if (!*thread) {
		free(start);
		return false;
	}
	return true;
}

void canard_thread_join(canard_thread thread) {
	(void)WaitForSingleObject(thread, INFINITE);
	CloseHandle(thread);
}
