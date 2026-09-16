#ifndef CANARD_THREAD_H
#define CANARD_THREAD_H

#include <stdbool.h>
#include <stdint.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>

typedef CRITICAL_SECTION canard_mutex;
typedef CONDITION_VARIABLE canard_condition;
typedef HANDLE canard_thread;
#else
#include <pthread.h>

typedef pthread_mutex_t canard_mutex;
typedef pthread_cond_t canard_condition;
typedef pthread_t canard_thread;
#endif

typedef void (*canard_thread_function)(void *);

bool canard_mutex_init(canard_mutex *mutex);
void canard_mutex_destroy(canard_mutex *mutex);
void canard_mutex_lock(canard_mutex *mutex);
void canard_mutex_unlock(canard_mutex *mutex);

bool canard_condition_init(canard_condition *condition);
void canard_condition_destroy(canard_condition *condition);
void canard_condition_broadcast(canard_condition *condition);
void canard_condition_wait(canard_condition *condition, canard_mutex *mutex);
void canard_condition_wait_milliseconds(canard_condition *condition, canard_mutex *mutex, uint64_t milliseconds);

bool canard_thread_start(canard_thread *thread, canard_thread_function function, void *argument);
void canard_thread_join(canard_thread thread);

#endif
