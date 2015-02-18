/*
  Copyright (c) 2012 Red Hat, Inc. <http://www.redhat.com>
  This file is part of GlusterFS.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#include <sys/poll.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "logging.h"
#include "event.h"
#include "mem-pool.h"
#include "common-utils.h"

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif


#ifdef HAVE_SYS_EPOLL_H
#include <sys/epoll.h>


static int
__event_getindex (struct event_pool *event_pool, int fd, int idx)
{
        int  ret = -1;
        int  i = 0;

        GF_VALIDATE_OR_GOTO ("event", event_pool, out);

        if (idx > -1 && idx < event_pool->used) {
                if (event_pool->reg[idx].fd == fd)
                        ret = idx;
        }

        for (i=0; ret == -1 && i<event_pool->used; i++) {
                if (event_pool->reg[i].fd == fd) {
                        ret = i;
                        break;
                }
        }

out:
        return ret;
}


static struct event_pool *
event_pool_new_epoll (int count)
{
        struct event_pool *event_pool = NULL;
        int                epfd = -1;

        event_pool = GF_CALLOC (1, sizeof (*event_pool),
                                gf_common_mt_event_pool);

        if (!event_pool)
                goto out;

        event_pool->count = count;
        event_pool->reg = GF_CALLOC (event_pool->count,
                                     sizeof (*event_pool->reg),
                                     gf_common_mt_reg);

        if (!event_pool->reg) {
                GF_FREE (event_pool);
                event_pool = NULL;
                goto out;
        }

        epfd = epoll_create (count);

        if (epfd == -1) {
                gf_log ("epoll", GF_LOG_ERROR, "epoll fd creation failed (%s)",
                        strerror (errno));
                GF_FREE (event_pool->reg);
                GF_FREE (event_pool);
                event_pool = NULL;
                goto out;
        }

        event_pool->fd = epfd;

        event_pool->count = count;

        pthread_mutex_init (&event_pool->mutex, NULL);
        pthread_cond_init (&event_pool->cond, NULL);

out:
        return event_pool;
}


int
event_register_epoll (struct event_pool *event_pool, int fd,
                      event_handler_t handler,
                      void *data, int poll_in, int poll_out)
{
        int                 idx = -1;
        int                 ret = -1;
        int             destroy = 0;
        struct epoll_event  epoll_event = {0, };
        struct event_data  *ev_data = (void *)&epoll_event.data;
        struct event_slot_epoll *slot = NULL;


        GF_VALIDATE_OR_GOTO ("event", event_pool, out);

        /* TODO: Even with the below check, there is a possiblity of race,
         * What if the destroy mode is set after the check is done.
         * Not sure of the best way to prevent this race, ref counting
         * is one possibility.
         * There is no harm in registering and unregistering the fd
         * even after destroy mode is set, just that such fds will remain
         * open until unregister is called, also the events on that fd will be
         * notified, until one of the poller thread is alive.
         */
        pthread_mutex_lock (&event_pool->mutex);
        {
                destroy = event_pool->destroy;
        }
        pthread_mutex_unlock (&event_pool->mutex);

        if (destroy == 1)
               goto out;

        idx = event_slot_alloc (event_pool, fd);
        if (idx == -1) {
                gf_log ("epoll", GF_LOG_ERROR,
                        "could not find slot for fd=%d", fd);
                return -1;
        }

        slot = event_slot_get (event_pool, idx);

        assert (slot->fd == fd);

        LOCK (&slot->lock);
        {
                /* make epoll 'singleshot', which
                   means we need to re-add the fd with
                   epoll_ctl(EPOLL_CTL_MOD) after delivery of every
                   single event. This assures us that while a poller
                   thread has picked up and is processing an event,
                   another poller will not try to pick this at the same
                   time as well.
                */

                slot->events = EPOLLPRI | EPOLLONESHOT;
                slot->handler = handler;
                slot->data = data;

                __slot_update_events (slot, poll_in, poll_out);

                epoll_event.events = slot->events;
                ev_data->idx = idx;
                ev_data->gen = slot->gen;

                ret = epoll_ctl (event_pool->fd, EPOLL_CTL_ADD, fd,
                                 &epoll_event);
                /* check ret after UNLOCK() to avoid deadlock in
                   event_slot_unref()
                */
        }
        UNLOCK (&slot->lock);

        if (ret == -1) {
                gf_log ("epoll", GF_LOG_ERROR,
                        "failed to add fd(=%d) to epoll fd(=%d) (%s)",
                        fd, event_pool->fd, strerror (errno));

                event_slot_unref (event_pool, slot, idx);
                idx = -1;
        }

        /* keep slot->ref (do not event_slot_unref) if successful */
out:
        return idx;
}


static int
event_unregister_epoll (struct event_pool *event_pool, int fd, int idx_hint)
{
        int  idx = -1;
        int  ret = -1;

        struct epoll_event epoll_event = {0, };
        struct event_data *ev_data = (void *)&epoll_event.data;
        int                lastidx = -1;

        GF_VALIDATE_OR_GOTO ("event", event_pool, out);

        pthread_mutex_lock (&event_pool->mutex);
        {
                idx = __event_getindex (event_pool, fd, idx_hint);

                if (idx == -1) {
                        gf_log ("epoll", GF_LOG_ERROR,
                                "index not found for fd=%d (idx_hint=%d)",
                                fd, idx_hint);
                        errno = ENOENT;
                        goto unlock;
                }

                ret = epoll_ctl (event_pool->fd, EPOLL_CTL_DEL, fd, NULL);

                /* if ret is -1, this array member should never be accessed */
                /* if it is 0, the array member might be used by idx_cache
                 * in which case the member should not be accessed till
                 * it is reallocated
                 */

                event_pool->reg[idx].fd = -1;

                if (ret == -1) {
                        gf_log ("epoll", GF_LOG_ERROR,
                                "fail to del fd(=%d) from epoll fd(=%d) (%s)",
                                fd, event_pool->fd, strerror (errno));
                        goto unlock;
                }

                lastidx = event_pool->used - 1;
                if (lastidx == idx) {
                        event_pool->used--;
                        goto unlock;
                }

                epoll_event.events = event_pool->reg[lastidx].events;
                ev_data->fd = event_pool->reg[lastidx].fd;
                ev_data->idx = idx;

                ret = epoll_ctl (event_pool->fd, EPOLL_CTL_MOD, ev_data->fd,
                                 &epoll_event);
                if (ret == -1) {
                        gf_log ("epoll", GF_LOG_ERROR,
                                "fail to modify fd(=%d) index %d to %d (%s)",
                                ev_data->fd, event_pool->used, idx,
                                strerror (errno));
                        goto unlock;
                }

                /* just replace the unregistered idx by last one */
                event_pool->reg[idx] = event_pool->reg[lastidx];
                event_pool->used--;
        }
unlock:
        pthread_mutex_unlock (&event_pool->mutex);

out:
        return ret;
}


static int
event_select_on_epoll (struct event_pool *event_pool, int fd, int idx_hint,
                       int poll_in, int poll_out)
{
        int idx = -1;
        int ret = -1;

        struct epoll_event epoll_event = {0, };
        struct event_data *ev_data = (void *)&epoll_event.data;


        GF_VALIDATE_OR_GOTO ("event", event_pool, out);

        pthread_mutex_lock (&event_pool->mutex);
        {
                idx = __event_getindex (event_pool, fd, idx_hint);

                if (idx == -1) {
                        gf_log ("epoll", GF_LOG_ERROR,
                                "index not found for fd=%d (idx_hint=%d)",
                                fd, idx_hint);
                        errno = ENOENT;
                        goto unlock;
                }

                switch (poll_in) {
                case 1:
                        event_pool->reg[idx].events |= EPOLLIN;
                        break;
                case 0:
                        event_pool->reg[idx].events &= ~EPOLLIN;
                        break;
                case -1:
                        /* do nothing */
                        break;
                default:
                        gf_log ("epoll", GF_LOG_ERROR,
                                "invalid poll_in value %d", poll_in);
                        break;
                }

                switch (poll_out) {
                case 1:
                        event_pool->reg[idx].events |= EPOLLOUT;
                        break;
                case 0:
                        event_pool->reg[idx].events &= ~EPOLLOUT;
                        break;
                case -1:
                        /* do nothing */
                        break;
                default:
                        gf_log ("epoll", GF_LOG_ERROR,
                                "invalid poll_out value %d", poll_out);
                        break;
                }

                epoll_event.events = event_pool->reg[idx].events;
                ev_data->fd = fd;
                ev_data->idx = idx;

                ret = epoll_ctl (event_pool->fd, EPOLL_CTL_MOD, fd,
                                 &epoll_event);
                if (ret == -1) {
                        gf_log ("epoll", GF_LOG_ERROR,
                                "failed to modify fd(=%d) events to %d",
                                fd, epoll_event.events);
                }
        }
unlock:
        pthread_mutex_unlock (&event_pool->mutex);

out:
        return ret;
}


static int
event_dispatch_epoll_handler (struct event_pool *event_pool,
                              struct epoll_event *events, int i)
{
        struct event_data  *event_data = NULL;
        event_handler_t     handler = NULL;
        void               *data = NULL;
        int                 idx = -1;
        int                 ret = -1;


        event_data = (void *)&events[i].data;
        handler = NULL;
        data = NULL;

        pthread_mutex_lock (&event_pool->mutex);
        {
                idx = __event_getindex (event_pool, event_data->fd,
                                        event_data->idx);

                if (idx == -1) {
                        gf_log ("epoll", GF_LOG_ERROR,
                                "index not found for fd(=%d) (idx_hint=%d)",
                                event_data->fd, event_data->idx);
                        goto unlock;
                }

                handler = event_pool->reg[idx].handler;
                data = event_pool->reg[idx].data;
        }
unlock:
        pthread_mutex_unlock (&event_pool->mutex);

        if (handler)
                ret = handler (event_data->fd, event_data->idx, data,
                               (events[i].events & (EPOLLIN|EPOLLPRI)),
                               (events[i].events & (EPOLLOUT)),
                               (events[i].events & (EPOLLERR|EPOLLHUP)));
        return ret;
}


/* Attempts to start the # of configured pollers, ensuring at least the first
 * is started in a joinable state */
static int
event_dispatch_epoll (struct event_pool *event_pool)
{
        int                       i = 0;
        pthread_t                 t_id;
        int                       pollercount = 0;
        int                       ret = -1;
        struct event_thread_data *ev_data = NULL;

        /* Start the configured number of pollers */
        pthread_mutex_lock (&event_pool->mutex);
        {
                pollercount = event_pool->eventthreadcount;

                /* Set to MAX if greater */
                if (pollercount > EVENT_MAX_THREADS)
                        pollercount = EVENT_MAX_THREADS;

                /* Default pollers to 1 in case this is incorrectly set */
                if (pollercount <= 0)
                        pollercount = 1;

                event_pool->activethreadcount++;

                for (i = 0; i < pollercount; i++) {
                        ev_data = GF_CALLOC (1, sizeof (*ev_data),
                                     gf_common_mt_event_pool);
                        if (!ev_data) {
                                gf_log ("epoll", GF_LOG_WARNING,
                                        "Allocation failure for index %d", i);
                                if (i == 0) {
                                        /* Need to suceed creating 0'th
                                         * thread, to joinable and wait */
                                        break;
                                } else {
                                        /* Inability to create other threads
                                         * are a lesser evil, and ignored */
                                        continue;
                                }
                        }

                        ev_data->event_pool = event_pool;
                        ev_data->event_index = i + 1;

                        ret = pthread_create (&t_id, NULL,
                                              event_dispatch_epoll_worker,
                                              ev_data);
                        if (!ret) {
                                event_pool->pollers[i] = t_id;

                                /* mark all threads other than one in index 0
                                 * as detachable. Errors can be ignored, they
                                 * spend their time as zombies if not detched
                                 * and the thread counts are decreased */
                                if (i != 0)
                                        pthread_detach (event_pool->pollers[i]);
                        } else {
                                gf_log ("epoll", GF_LOG_WARNING,
                                        "Failed to start thread for index %d",
                                        i);
                                if (i == 0) {
                                        GF_FREE (ev_data);
                                        break;
                                } else {
                                        GF_FREE (ev_data);
                                        continue;
                                }
                        }
                }
        }
        pthread_mutex_unlock (&event_pool->mutex);

        /* Just wait for the first thread, that is created in a joinable state
         * and will never die, ensuring this function never returns */
        if (event_pool->pollers[0] != 0)
                pthread_join (event_pool->pollers[0], NULL);

        pthread_mutex_lock (&event_pool->mutex);
        {
                event_pool->activethreadcount--;
        }
        pthread_mutex_unlock (&event_pool->mutex);

        return ret;
}

int
event_reconfigure_threads_epoll (struct event_pool *event_pool, int value)
{
        int                              i;
        int                              ret = 0;
        pthread_t                        t_id;
        int                              oldthreadcount;
        struct event_thread_data        *ev_data = NULL;

        pthread_mutex_lock (&event_pool->mutex);
        {
                /* Reconfigure to 0 threads is allowed only in destroy mode */
                if (event_pool->destroy == 1) {
                        value = 0;
                } else {
                        /* Set to MAX if greater */
                        if (value > EVENT_MAX_THREADS)
                                value = EVENT_MAX_THREADS;

                        /* Default pollers to 1 in case this is set incorrectly */
                        if (value <= 0)
                                value = 1;
                }

                oldthreadcount = event_pool->eventthreadcount;

                if (oldthreadcount < value) {
                        /* create more poll threads */
                        for (i = oldthreadcount; i < value; i++) {
                                /* Start a thread if the index at this location
                                 * is a 0, so that the older thread is confirmed
                                 * as dead */
                                if (event_pool->pollers[i] == 0) {
                                        ev_data = GF_CALLOC (1,
                                                      sizeof (*ev_data),
                                                      gf_common_mt_event_pool);
                                        if (!ev_data) {
                                                gf_log ("epoll", GF_LOG_WARNING,
                                                  "Allocation failure for"
                                                  " index %d", i);
                                                continue;
                                        }

                                        ev_data->event_pool = event_pool;
                                        ev_data->event_index = i + 1;

                                        ret = pthread_create (&t_id, NULL,
                                                event_dispatch_epoll_worker,
                                                ev_data);
                                        if (ret) {
                                                gf_log ("epoll", GF_LOG_WARNING,
                                                  "Failed to start thread for"
                                                  " index %d", i);
                                                GF_FREE (ev_data);
                                        } else {
                                                pthread_detach (t_id);
                                                event_pool->pollers[i] = t_id;
                                        }
                                }
                        }
                }

                /* if value decreases, threads will terminate, themselves */
                event_pool->eventthreadcount = value;
        }
        pthread_mutex_unlock (&event_pool->mutex);

        return 0;
}
>>>>>>> 2acfbcf... event_pool: Add the code to destroy the poller threads and event pool gracefully.

/* This function is the destructor for the event_pool data structure
 * Should be called only after poller_threads_destroy() is called,
 * else will lead to crashes.
 */
static int
event_pool_destroy_epoll (struct event_pool *event_pool)
{
        int ret = 0, i = 0, j = 0;
        struct event_slot_epoll *table = NULL;

        ret = close (event_pool->fd);

        for (i = 0; i < EVENT_EPOLL_TABLES; i++) {
                if (event_pool->ereg[i]) {
                        table = event_pool->ereg[i];
                        event_pool->ereg[i] = NULL;
                                for (j = 0; j < EVENT_EPOLL_SLOTS; j++) {
                                        LOCK_DESTROY (&table[j].lock);
                                }
                        GF_FREE (table);
                }
        }

        pthread_mutex_destroy (&event_pool->mutex);
        pthread_cond_destroy (&event_pool->cond);

        GF_FREE (event_pool->evcache);
        GF_FREE (event_pool->reg);
        GF_FREE (event_pool);

        return ret;
}

struct event_ops event_ops_epoll = {
        .new                       = event_pool_new_epoll,
        .event_register            = event_register_epoll,
        .event_select_on           = event_select_on_epoll,
        .event_unregister          = event_unregister_epoll,
        .event_unregister_close    = event_unregister_close_epoll,
        .event_dispatch            = event_dispatch_epoll,
        .event_reconfigure_threads = event_reconfigure_threads_epoll,
        .event_pool_destroy        = event_pool_destroy_epoll
};

#endif
