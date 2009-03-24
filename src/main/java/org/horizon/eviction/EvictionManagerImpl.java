package org.horizon.eviction;

import net.jcip.annotations.ThreadSafe;
import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.container.DataContainer;
import org.horizon.factories.KnownComponentNames;
import org.horizon.factories.annotations.ComponentName;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.factories.annotations.Stop;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

@ThreadSafe
public class EvictionManagerImpl implements EvictionManager {
   private static final Log log = LogFactory.getLog(EvictionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   // elements
//   EvictionAlgorithm evictionAlgorithm;
//   EvictionAlgorithmConfig evictionAlgorithmConfig;
//   EvictionConfig evictionConfig;
   ScheduledFuture evictionTask;

   // event queue
//   BlockingQueue<EvictionEvent> evictionEventQueue;
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
//      if (configuration.getEvictionConfig() != null) {
//         evictionConfig = configuration.getEvictionConfig();
//
//         // 1.  set up the eviction event queue
//         int size = evictionConfig.getEventQueueSize();
//         capacityWarnThreshold = (98 * size) / 100 - 100;
//         if (capacityWarnThreshold <= 0) {
//            if (log.isWarnEnabled()) log.warn("Capacity warn threshold used in eviction is smaller than 1.");
//         }
//         evictionEventQueue = new LinkedBlockingQueue<EvictionEvent>(size);
//
//         // 2.  now ensure we instantiate the eviction algorithm class
//         evictionAlgorithmConfig = evictionConfig.getAlgorithmConfig();
//         evictionAlgorithm = createEvictionAlgorithm(evictionAlgorithmConfig, evictionConfig.getActionPolicyClass());
//         evictionAlgorithm.start();
//
//         // 3.  And finally set up the eviction timer task
//         if (evictionConfig.getWakeUpInterval() <= 0) {
//            log.info("wakeUpInterval is <= 0, not starting eviction thread");
//         } else {
//            evictionTask = executor.scheduleWithFixedDelay(new ScheduledTask(), evictionConfig.getWakeUpInterval(),
//                                                           evictionConfig.getWakeUpInterval(), TimeUnit.MILLISECONDS);
//         }
//      }
   }

   public void processEviction() {
      // TODO: Customise this generated block
   }

   public boolean isEnabled() {
      return false;  // TODO: Customise this generated block
   }

   class ScheduledTask implements Runnable {
      public void run() {
//         registerEvictionEvent(new EvictionEvent(null, EvictionEvent.Type.EXPIRED_DATA_PURGE_START));
//         Set purgedKeys = dataContainer.purgeExpiredEntries();
//         registerEvictionEvent(new PurgedDataEndEvent(purgedKeys));
//         processEvictionQueues();
      }
   }

   @Stop
   public void stop() {
      if (evictionTask != null) evictionTask.cancel(true);
//      if (evictionAlgorithm != null) evictionAlgorithm.stop();
//      evictionAlgorithm = null;
//      if (evictionEventQueue != null) evictionEventQueue.clear();
//      evictionEventQueue = null;
   }

   public void processEvictionQueues() {
//      evictionAlgorithm.process(evictionEventQueue);
   }

   public void resetEvictionQueues() {
//      evictionEventQueue.clear();
   }

//   private EvictionAlgorithm createEvictionAlgorithm(EvictionAlgorithmConfig algoConfig, String evictionActionClass) {
//      if (algoConfig == null)
//         throw new IllegalArgumentException("Eviction algorithm class must not be null!");
//
//      if (evictionActionClass == null)
//         throw new IllegalArgumentException("Eviction action policy class must not be null!");
//
//      try {
//         if (trace) log.trace("Instantiating {0}", evictionActionClass);
//         EvictionAction evictionAction = (EvictionAction) Util.getInstance(evictionActionClass);
//         evictionAction.setCache(cache);
//
//         if (trace) log.trace("Instantiating {0}", algoConfig.getEvictionAlgorithmClassName());
//         EvictionAlgorithm algorithm = (EvictionAlgorithm) Util.getInstance(algoConfig.getEvictionAlgorithmClassName());
//         algorithm.setEvictionAction(evictionAction);
//         algorithm.init(cache, dataContainer, algoConfig);
//         return algorithm;
//      } catch (Exception e) {
//         log.fatal("Unable to instantiate eviction algorithm {0}", e, algoConfig.getEvictionAlgorithmClassName());
//         throw new IllegalStateException(e);
//      }
//   }
}
