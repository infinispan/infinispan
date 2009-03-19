package org.horizon.eviction;

import net.jcip.annotations.ThreadSafe;
import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.config.EvictionConfig;
import org.horizon.container.DataContainer;
import org.horizon.eviction.events.EvictionEvent;
import org.horizon.eviction.events.InUseEvictionEvent;
import org.horizon.eviction.events.PurgedDataEndEvent;
import org.horizon.factories.KnownComponentNames;
import org.horizon.factories.annotations.ComponentName;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.factories.annotations.Stop;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.util.Util;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@ThreadSafe
public class EvictionManagerImpl implements EvictionManager {
   private static final Log log = LogFactory.getLog(EvictionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   // elements
   EvictionAlgorithm evictionAlgorithm;
   EvictionAlgorithmConfig evictionAlgorithmConfig;
   EvictionConfig evictionConfig;
   ScheduledFuture evictionTask;

   // event queue
   BlockingQueue<EvictionEvent> evictionEventQueue;
   int capacityWarnThreshold = 0;

   // components to be injected
   ScheduledExecutorService executor;
   Configuration configuration;
   Cache cache;
   private DataContainer dataContainer;

   @Inject
   public void initialize(@ComponentName(KnownComponentNames.EVICTION_SCHEDULED_EXECUTOR) ScheduledExecutorService executor,
                          Configuration configuration, Cache cache, DataContainer dataContainer) {
      this.executor = executor;
      this.configuration = configuration;
      this.cache = cache;
      this.dataContainer = dataContainer;
   }

   @Start
   public void start() {
      // first check if eviction is enabled!
      if (configuration.getEvictionConfig() != null) {
         evictionConfig = configuration.getEvictionConfig();

         // 1.  set up the eviction event queue
         int size = evictionConfig.getEventQueueSize();
         capacityWarnThreshold = (98 * size) / 100 - 100;
         if (capacityWarnThreshold <= 0) {
            if (log.isWarnEnabled()) log.warn("Capacity warn threshold used in eviction is smaller than 1.");
         }
         evictionEventQueue = new LinkedBlockingQueue<EvictionEvent>(size);

         // 2.  now ensure we instantiate the eviction algorithm class
         evictionAlgorithmConfig = evictionConfig.getAlgorithmConfig();
         evictionAlgorithm = createEvictionAlgorithm(evictionAlgorithmConfig, evictionConfig.getActionPolicyClass());
         evictionAlgorithm.start();

         // 3.  And finally set up the eviction timer task
         if (evictionConfig.getWakeUpInterval() <= 0) {
            log.info("wakeUpInterval is <= 0, not starting eviction thread");
         } else {
            evictionTask = executor.scheduleWithFixedDelay(new ScheduledTask(), evictionConfig.getWakeUpInterval(),
                                                           evictionConfig.getWakeUpInterval(), TimeUnit.MILLISECONDS);
         }
      }
   }

   class ScheduledTask implements Runnable {
      public void run() {
         registerEvictionEvent(new EvictionEvent(null, EvictionEvent.Type.EXPIRED_DATA_PURGE_START));
         Set purgedKeys = dataContainer.purgeExpiredEntries();
         registerEvictionEvent(new PurgedDataEndEvent(purgedKeys));
         processEvictionQueues();
      }
   }

   @Stop
   public void stop() {
      if (evictionTask != null) evictionTask.cancel(true);
      if (evictionAlgorithm != null) evictionAlgorithm.stop();
      evictionAlgorithm = null;
      if (evictionEventQueue != null) evictionEventQueue.clear();
      evictionEventQueue = null;
   }

   public void processEvictionQueues() {
      evictionAlgorithm.process(evictionEventQueue);
   }

   public void resetEvictionQueues() {
      evictionEventQueue.clear();
   }

   public EvictionEvent registerEvictionEvent(Object key, EvictionEvent.Type eventType) {
      if (evictionAlgorithm.canIgnoreEvent(eventType)) return null;
      EvictionEvent ee = new EvictionEvent(key, eventType);
      registerEvictionEvent(ee);
      return ee;
   }

   public void markKeyCurrentlyInUse(Object key, long duration, TimeUnit unit) {
      registerEvictionEvent(new InUseEvictionEvent(key, unit.toMillis(duration)));
   }

   public void unmarkKeyCurrentlyInUse(Object key) {
      registerEvictionEvent(key, EvictionEvent.Type.UNMARK_IN_USE_EVENT);
   }

   private void registerEvictionEvent(EvictionEvent ee) {
      try {
         if (evictionEventQueue.size() > capacityWarnThreshold) {
            if (log.isWarnEnabled())
               log.warn("Eviction event queue size is at 98% capacity of {0} on cache {1}. You should reduce the wakeUpInterval attribute.",
                        evictionConfig.getEventQueueSize(), cache.getName());
         }
         evictionEventQueue.put(ee);
      }
      catch (InterruptedException e) {
         if (log.isDebugEnabled()) log.debug("Interrupted on adding event", e);
         // reinstate interrupt flag
         Thread.currentThread().interrupt();
      }
   }

   private EvictionAlgorithm createEvictionAlgorithm(EvictionAlgorithmConfig algoConfig, String evictionActionClass) {
      if (algoConfig == null)
         throw new IllegalArgumentException("Eviction algorithm class must not be null!");

      if (evictionActionClass == null)
         throw new IllegalArgumentException("Eviction action policy class must not be null!");

      try {
         if (trace) log.trace("Instantiating {0}", evictionActionClass);
         EvictionAction evictionAction = (EvictionAction) Util.getInstance(evictionActionClass);
         evictionAction.setCache(cache);

         if (trace) log.trace("Instantiating {0}", algoConfig.getEvictionAlgorithmClassName());
         EvictionAlgorithm algorithm = (EvictionAlgorithm) Util.getInstance(algoConfig.getEvictionAlgorithmClassName());
         algorithm.setEvictionAction(evictionAction);
         algorithm.init(cache, dataContainer, algoConfig);
         return algorithm;
      } catch (Exception e) {
         log.fatal("Unable to instantiate eviction algorithm {0}", e, algoConfig.getEvictionAlgorithmClassName());
         throw new IllegalStateException(e);
      }
   }
}
