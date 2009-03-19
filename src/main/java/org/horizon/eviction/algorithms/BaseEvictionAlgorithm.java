/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.horizon.eviction.algorithms;

import org.horizon.Cache;
import org.horizon.container.DataContainer;
import org.horizon.eviction.EvictionAction;
import org.horizon.eviction.EvictionAlgorithm;
import org.horizon.eviction.EvictionAlgorithmConfig;
import org.horizon.eviction.EvictionException;
import org.horizon.eviction.EvictionQueue;
import org.horizon.eviction.events.EvictionEvent;
import org.horizon.eviction.events.EvictionEvent.Type;
import org.horizon.eviction.events.InUseEvictionEvent;
import org.horizon.eviction.events.PurgedDataEndEvent;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Abstract Eviction Algorithm. This class is used to implement basic event processing for eviction algorithms.
 *
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @author Manik Surtani
 * @since 1.0
 */
public abstract class BaseEvictionAlgorithm implements EvictionAlgorithm {
   private static final Log log = LogFactory.getLog(BaseEvictionAlgorithm.class);
   private static final boolean trace = log.isTraceEnabled();

   protected EvictionAction action;
   protected BaseEvictionAlgorithmConfig config;
   // blocking queue of cache keys
   protected BlockingQueue<Object> recycleQueue;
   protected EvictionQueue evictionQueue;
   protected Cache cache;
   protected DataContainer dataContainer;
   // cache keys and expiry time
   protected Map<Object, Long> keysInUse = new HashMap<Object, Long>();

   /**
    * This method will create a new EvictionQueue instance and prepare it for use.
    *
    * @return A new EvictionQueue
    */
   protected abstract EvictionQueue createEvictionQueue();

   /**
    * This method tests whether the next entry should be evicted due to cache size constraints being hit.
    *
    * @return true if the entry should be evicted, false if no more evictions are needed.
    */
   protected boolean needToEvict(int originalQueueSize) {
      // test max and min entries
      if (config.getMaxEntries() > -1) {
         // entry count eviction is enabled!
         int currentSize = evictionQueue.size();

         // exceeded max entries!
         if (config.getMaxEntries() < currentSize) return true;

         // below max entries now, but the current run exceeded, and we need to get down to minEntries (if configured)
         if (config.getMaxEntries() < originalQueueSize &&
               config.getMinEntries() > -1 &&
               config.getMinEntries() < currentSize)
            return true;
      }
      // configured to hold unlimited entries, or we haven't hit any thresholds yet
      return false;
   }

   protected BaseEvictionAlgorithm() {
      // size of the recycle queue
      // TODO make this configurable!!
      recycleQueue = new LinkedBlockingQueue<Object>(500000);
   }

   public synchronized void start() {
      if (evictionQueue == null) evictionQueue = createEvictionQueue();
   }

   public synchronized void stop() {
      if (evictionQueue != null) evictionQueue.clear();
      keysInUse.clear();
      recycleQueue.clear();
   }

   public void setEvictionAction(EvictionAction evictionAction) {
      this.action = evictionAction;
   }

   public void init(Cache<?, ?> cache, DataContainer dataContainer, EvictionAlgorithmConfig evictionAlgorithmConfig) {
      this.cache = cache;
      this.dataContainer = dataContainer;
      this.config = (BaseEvictionAlgorithmConfig) evictionAlgorithmConfig;
   }

   public boolean canIgnoreEvent(Type eventType) {
      return false; // don't ignore anything by default
   }

   /**
    * Process the given eviction event queue.  Eviction Pprocessing encompasses the following:
    *
    * @param eventQueue queue containing eviction events
    * @throws EvictionException
    */
   public void process(BlockingQueue<EvictionEvent> eventQueue) throws EvictionException {
      if (trace) log.trace("processing eviction event queue");
      processQueues(eventQueue);
      emptyRecycleQueue();
      prune();
   }

   public void resetEvictionQueue() {
      if (evictionQueue != null) evictionQueue.clear();
   }

   /**
    * Get the underlying EvictionQueue implementation.
    *
    * @return the EvictionQueue used by this algorithm
    * @see EvictionQueue
    */
   public EvictionQueue getEvictionQueue() {
      return evictionQueue;
   }

   protected EvictionEvent nextEvent(BlockingQueue<EvictionEvent> queue) {
      try {
         return queue.poll(0, TimeUnit.SECONDS);
      }
      catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      return null;
   }

   /**
    * Process the event queue
    *
    * @param queue queue to inspect
    * @throws EvictionException in the event of problems
    */
   protected void processQueues(BlockingQueue<EvictionEvent> queue) throws EvictionException {
      EvictionEvent event;
      int count = 0;
      long startTime = System.currentTimeMillis();
      Set<Object> keysToRetainInQueue = null;
      while ((event = nextEvent(queue)) != null) {
         if (trace) count++;
         switch (event.getEventType()) {
            case ADD_ENTRY_EVENT:
               Object key = event.getKey();
               processAddedEntries(key);
               recordEventKey(keysToRetainInQueue, key);
               break;
            case REMOVE_ENTRY_EVENT:
               processRemovedEntries(event.getKey());
               break;
            case VISIT_ENTRY_EVENT:
               processVisitedEntries(event.getKey());
               break;
            case CLEAR_CACHE_EVENT:
               processClearCacheEvent();
               break;
            case MARK_IN_USE_EVENT:
               processMarkInUse(event.getKey(), ((InUseEvictionEvent) event).getInUseTimeout());
               break;
            case UNMARK_IN_USE_EVENT:
               processUnmarkInUse(event.getKey());
               break;
            case EXPIRED_DATA_PURGE_START:
               if (keysToRetainInQueue == null) keysToRetainInQueue = new HashSet<Object>();
               break;
            case EXPIRED_DATA_PURGE_END:
               Set<Object> keysPurged = ((PurgedDataEndEvent) event).getKeysPurged();
               if (keysToRetainInQueue != null) keysPurged.removeAll(keysToRetainInQueue);
               for (Object o : keysPurged) evictionQueue.remove(o);
               break;
            default:
               throw new EvictionException("Illegal eviction event type " + event.getEventType());
         }
      }
      if (trace)
         log.trace("processed {0} eviction events in {1} millis", count, System.currentTimeMillis() - startTime);
   }

   private void recordEventKey(Set<Object> setToRecordIn, Object key) {
      if (setToRecordIn != null) setToRecordIn.add(key);
   }

   protected void evictOrRecycle(Object key) {
      log.trace("Attempting to evict {0}", key);
      if (!action.evict(key)) {
         try {
            boolean result = recycleQueue.offer(key, 5, TimeUnit.SECONDS);
            if (!result) {
               log.warn("Unable to add key {0} to the recycle queue." +
                     "This is often sign that " +
                     "evictions are not occurring and entries that should be " +
                     "evicted are piling up waiting to be evicted.", key);
            }
         }
         catch (InterruptedException e) {
            log.debug("InterruptedException", e);
         }
      }
   }

   protected void processMarkInUse(Object key, long inUseTimeout) throws EvictionException {
      log.trace("Marking {0} as in use with a usage timeout of {1}", key, inUseTimeout);
      keysInUse.put(key, inUseTimeout + System.currentTimeMillis());
   }

   protected void processUnmarkInUse(Object key) throws EvictionException {
      log.trace("Unmarking {0} as in use", key);
      keysInUse.remove(key);
   }

   protected void processAddedEntries(Object key) throws EvictionException {
      log.trace("Adding entry {0} to eviction queue", key);
      if (evictionQueue.contains(key)) {
         evictionQueue.visit(key);
         log.trace("Key already exists so treating as a visit");
      } else {
         evictionQueue.add(key);
      }
   }

   protected void processRemovedEntries(Object key) throws EvictionException {
      log.trace("Removing key {0} from eviction queue and attempting eviction", key);
      evictionQueue.remove(key);
   }

   protected void processClearCacheEvent() throws EvictionException {
      log.trace("Clearing eviction queue");
      evictionQueue.clear();
   }

   protected void processVisitedEntries(Object key) throws EvictionException {
      log.trace("Visiting entry {0}", key);
      evictionQueue.visit(key);
   }

   /**
    * Empty the Recycle Queue.
    * <p/>
    * This method will go through the recycle queue and retry to evict the entries from cache.
    *
    * @throws EvictionException
    */
   protected void emptyRecycleQueue() throws EvictionException {
      while (true) {
         Object key;
         try {
            key = recycleQueue.poll(0, TimeUnit.SECONDS);
         }
         catch (InterruptedException e) {
            log.debug(e, e);
            break;
         }

         if (key == null) {
            if (trace) log.trace("Recycle queue is empty");
            break;
         }

         if (trace) log.trace("emptying recycle bin. Evict key " + key);

         // Still doesn't work
         if (!action.evict(key)) {
            try {
               recycleQueue.put(key);
            }
            catch (InterruptedException e) {
               if (trace) log.trace(e, e);
            }
            break;
         }
      }
   }

   protected void prune() throws EvictionException {
      int originalQueueSize = evictionQueue.size();
      for (Iterator<Object> it = evictionQueue.iterator(); it.hasNext();) {
         Object key = it.next();
         if (key != null && needToEvict(originalQueueSize)) {
            if (shouldNotOverrideEviction(key) && isNotMarkedInUse(key)) {
               evictOrRecycle(key);
               it.remove();
            }
         } else {
            break; // assume the rest won't need to be evicted either
         }
      }
   }

   /**
    * Returns debug information.
    */
   @Override
   public String toString() {
      return super.toString() + " recycleQueueSize=" + recycleQueue.size() + " evictionQueueSize=" + evictionQueue.size();
   }

   /**
    * A helper for implementations that support the minimum time to live property
    *
    * @param key key to consider
    * @return true if the entry is younger than the minimum time to live, false otherwise.
    */
   private boolean shouldNotOverrideEviction(Object key) {
      long minTTL = config.getMinTimeToLive();
      long lastModified = dataContainer.getModifiedTimestamp(key);
      return minTTL < 1 || lastModified < 0 || (lastModified + minTTL < System.currentTimeMillis());
   }

   private boolean isNotMarkedInUse(Object key) {
      Long expiryTime = keysInUse.get(key);
      return expiryTime == null || (expiryTime < System.currentTimeMillis() && keysInUse.remove(key) != null);
   }
}
