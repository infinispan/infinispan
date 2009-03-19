package org.horizon.eviction;

import net.jcip.annotations.ThreadSafe;
import org.horizon.eviction.events.EvictionEvent;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;
import org.horizon.lifecycle.Lifecycle;

import java.util.concurrent.TimeUnit;

/**
 * Central component that deals with eviction of cache entries.  This component manages a queue of events taking place
 * on the cache.  This queue is typically populated by the {@link org.horizon.interceptors.EvictionInterceptor} calling
 * {@link #registerEvictionEvent(Object, org.horizon.eviction.events.EvictionEvent.Type)}.
 * <p/>
 * In certain special cases, this component may be manipulated by direct interaction with end user code.  An example of
 * this is the use of the {@link #markKeyCurrentlyInUse(Object, long, java.util.concurrent.TimeUnit)} and {@link #
 * markKeyCurrentlyInUse (Object, long, java.util.concurrent.TimeUnit)} methods, to prevent certain entries from being
 * evicted even if their "time is up".
 * <p/>
 * Another case where direct interaction may take place is where no eviction thread is configured (e.g., with
 * <tt>wakeUpInterval</tt> set to <tt>0</tt>, the same as {@link org.horizon.config.EvictionConfig#setWakeUpInterval(long)}
 * being set to <tt>0</tt>).
 * <p/>
 * In such cases, user code may want to manually process the eviction queue, by calling {@link
 * #processEvictionQueues()}. This is sometimes desirable, such as when user code already has a maintenance thread
 * periodically running and does not want to incur the cost of an additional eviction thread.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
@NonVolatile
@Scope(Scopes.NAMED_CACHE)
public interface EvictionManager extends Lifecycle {

   /**
    * Processes the eviction event queue.
    */
   void processEvictionQueues();

   /**
    * Clears the eviction event queue.
    */
   void resetEvictionQueues();

   /**
    * Registers an event on the eviction event queue for later processing by {@link #processEvictionQueues()}.
    *
    * @param key       key of the cache entry to register an event for
    * @param eventType type of event
    * @return the EvictionEvent instance after being registered
    */
   EvictionEvent registerEvictionEvent(Object key, EvictionEvent.Type eventType);

   /**
    * Marks a key as being currently in use, so that it is not considered for eviction even if the condifured algorithm
    * selects it for eviction.  It may be considered for eviction when the queue is next processed though.
    *
    * @param key     entry key to mark
    * @param timeout duration for which the entry should be considered as in-use
    * @param unit    time unit
    */
   void markKeyCurrentlyInUse(Object key, long timeout, TimeUnit unit);

   /**
    * Un-marks a key as being currently in use, if it was marked using {@link #markKeyCurrentlyInUse(Object, long,
    * java.util.concurrent.TimeUnit)} Unmarking makes the entry vulnerable to eviction again.
    *
    * @param key entry key to unmark
    */
   void unmarkKeyCurrentlyInUse(Object key);
}
