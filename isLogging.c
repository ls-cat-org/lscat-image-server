/** @file isLogging.c
 *  @brief Logs messages from the other system components
 *  @date 2018
 *  @copyright 2018 by Northwestern University. All rights reserved
 *  @author Keith Brister
 *
 *  Since we are going to be logging stuff from all kinds of different
 *  threads we'll set up a queue locked with a mutex.  When something
 *  has been added to the queue we'll know cause isLogging_cond will
 *  trigger us to read the queue and log the message with syslog.
 *
 */
#include "is.h"

static pthread_t        isLogging_thread;       //!< our thread
static pthread_mutex_t  isLogging_mutex;        //!< all important thread safety traffic cop
static phread_cond_t    isLogging_cond;         //!< Allows us to wait patiently

//! Maximum length of a single message
//! Aptly, this name is pretty long
#define IS_LOGGING_MSG_MAX_LENGTH       2048

//! Length of queue
#define IS_LOGGING_QUEUE_LENGTH         1024

//! Our queue structure
typedef struct isLogging_queue_struct {
  struct timespect ltime;       //!< time queued
  char lmsg[IS_LOGGING_MSG_MAX_LENGTH];
} isLogging_queue_t;

//! And the queue itself
static isLogging_queue_t isLogging_queue[IS_LOGGING_QUEUE_LENGTH];

//! number of items added to the queue
//! wrapping is OK since we only check for equality
static unsigned int isLogging_on  = 0;
static unsigned int isLogging_off = 0;

//! Initialize our objects
void isLogging_init() {
  pthread_mutex_init(&isLogging_mutex, NULL);
  pthread_cond_init( &isLogging_cond,  NULL);

  openlog("is", LOG_PID | LOG_PERROR, LOG_USER);
};
/*

       LOG_EMERG      system is unusable

       LOG_ALERT      action must be taken immediately

       LOG_CRIT       critical conditions

       LOG_ERR        error conditions

       LOG_WARNING    warning conditions

       LOG_NOTICE     normal, but significant, condition

       LOG_INFO       informational message

       LOG_DEBUG      debug-level message
*/

void isLogging_debug(char *fmt, ...) {
  va_list *arg_ptr;

  va_start(arg_ptr, *fmt);
  vsyslog(LOG_DEBUG, fmt, arg_ptr);
  va_end(arg_ptr);
}

void isLogging_info(char *fmt, ...) {
  va_list *arg_ptr;

  va_start(arg_ptr, *fmt);
  vsyslog(LOG_INFO, fmt, arg_ptr);
  va_end(arg_ptr);
}

void isLogging_notice(char *fmt, ...) {
  va_list *arg_ptr;

  va_start(arg_ptr, *fmt);
  vsyslog(LOG_NOTICE, fmt, arg_ptr);
  va_end(arg_ptr);
}

void isLogging_warning(char *fmt, ...) {
  va_list *arg_ptr;

  va_start(arg_ptr, *fmt);
  vsyslog(LOG_WARNING, fmt, arg_ptr);
  va_end(arg_ptr);
}

void isLogging_err(char *fmt, ...) {
  va_list *arg_ptr;

  va_start(arg_ptr, *fmt);
  vsyslog(LOG_ERR, fmt, arg_ptr);
  va_end(arg_ptr);
}

void isLogging_crit(char *fmt, ...) {
  va_list *arg_ptr;

  va_start(arg_ptr, *fmt);
  vsyslog(LOG_CRIT, fmt, arg_ptr);
  va_end(arg_ptr);
}

void isLogging_alert(char *fmt, ...) {
  va_list *arg_ptr;

  va_start(arg_ptr, *fmt);
  vsyslog(LOG_ALERT, fmt, arg_ptr);
  va_end(arg_ptr);
}

void isLogging_emerg(char *fmt, ...) {
  va_list *arg_ptr;

  va_start(arg_ptr, *fmt);
  vsyslog(LOG_EMERG, fmt, arg_ptr);
  va_end(arg_ptr);
}
