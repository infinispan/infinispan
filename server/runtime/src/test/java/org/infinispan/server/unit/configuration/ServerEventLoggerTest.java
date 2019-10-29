package org.infinispan.server.unit.configuration;

import static org.infinispan.test.TestingUtil.blockUntilViewReceived;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.infinispan.Cache;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.file.SingleFileStore;
import org.infinispan.server.logging.events.ServerEventImpl;
import org.infinispan.server.logging.events.ServerEventLogger;
import org.infinispan.server.test.TestThreadTrackerRule;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;
import org.junit.Rule;
import org.junit.Test;

/**
 * ServerEventLoggerTest.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */

public class ServerEventLoggerTest {
   @Rule
   public TestThreadTrackerRule tracker = new TestThreadTrackerRule();

   @Test
   public void testLocalServerEventLogging() {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(amendGlobalConfiguration(new GlobalConfigurationBuilder()), new ConfigurationBuilder())) {
         @Override
         public void call() {
            cm.getCache();
            EventLogger eventLogger = EventLogManager.getEventLogger(cm);
            assertTrue(eventLogger.getClass().getName(), eventLogger instanceof ServerEventLogger);
            eventLogger.info(EventLogCategory.CLUSTER, "message #1");
            eventLogger.info(EventLogCategory.TASKS, "message #2");
            eventLogger.warn(EventLogCategory.CLUSTER, "message #3");
            eventLogger.warn(EventLogCategory.TASKS, "message #4");
            List<EventLog> events = eventLogger.getEvents(Instant.now(), 10, Optional.of(EventLogCategory.CLUSTER), Optional.empty());
            assertEquals(2, events.size());
            assertEquals("message #3", events.get(0).getMessage());
            assertEquals(EventLogLevel.WARN, events.get(0).getLevel());
            assertEquals("message #1", events.get(1).getMessage());
            assertEquals(EventLogLevel.INFO, events.get(1).getLevel());
            events = eventLogger.getEvents(Instant.now(), 10, Optional.empty(), Optional.of(EventLogLevel.INFO));
            assertEquals(2, events.size());
            assertEquals("message #2", events.get(0).getMessage());
            assertEquals(EventLogCategory.TASKS, events.get(0).getCategory());
            assertEquals("message #1", events.get(1).getMessage());
            assertEquals(EventLogCategory.CLUSTER, events.get(1).getCategory());
         }
      });
   }

   @Test
   public void testClusteredServerEventLogging() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createClusteredCacheManager(amendGlobalConfiguration(GlobalConfigurationBuilder.defaultClusteredBuilder()), builder),
            TestCacheManagerFactory.createClusteredCacheManager(amendGlobalConfiguration(GlobalConfigurationBuilder.defaultClusteredBuilder()), builder),
            TestCacheManagerFactory.createClusteredCacheManager(amendGlobalConfiguration(GlobalConfigurationBuilder.defaultClusteredBuilder()), builder)) {
         @Override
         public void call() {
            int msg = 1;
            blockUntilViewReceived(cms[0].getCache(), 3);
            // Fill all nodes with logs
            for (int i = 0; i < cms.length; i++) {
               EventLogger eventLogger = EventLogManager.getEventLogger(cms[i]);
               assertTrue(eventLogger.getClass().getName(), eventLogger instanceof ServerEventLogger);
               eventLogger.info(EventLogCategory.SECURITY, "message #" + msg++);
               eventLogger.warn(EventLogCategory.SECURITY, "message #" + msg++);
               eventLogger.info(EventLogCategory.TASKS, "message #" + msg++);
               eventLogger.warn(EventLogCategory.TASKS, "message #" + msg++);
            }
            // query all nodes
            for (int i = 0; i < cms.length; i++) {
               EventLogger eventLogger = EventLogManager.getEventLogger(cms[i]);
               List<EventLog> events = eventLogger.getEvents(Instant.now(), 10, Optional.of(EventLogCategory.TASKS), Optional.empty());
               assertEquals("Result count discrepancy on node " + i, 2 * cms.length, events.size());
               for (EventLog event : events) {
                  assertEquals(EventLogCategory.TASKS, event.getCategory());
               }
               events = eventLogger.getEvents(Instant.now(), 10, Optional.empty(), Optional.of(EventLogLevel.INFO));
               for (EventLog event : events) {
                  assertEquals(EventLogLevel.INFO, event.getLevel());
               }
            }
         }
      });
   }

   @Test
   public void testLocalManagerNotStarted() {
      withCacheManager(TestCacheManagerFactory.createCacheManager(false), cm -> {
         Exceptions.expectException(IllegalLifecycleStateException.class, () -> EventLogManager.getEventLogger(cm));
      });
   }

   @Test
   public void testLocalServerEventLoggingPreloading() {
      GlobalConfigurationBuilder global = amendGlobalConfiguration(new GlobalConfigurationBuilder());
      global.globalState().enable();
      deleteGlobalPersistentState(global);

      withCacheManager(startCacheManagerWithGlobalState(global), cm -> {
         EventLogger eventLogger = EventLogManager.getEventLogger(cm);
         eventLogger.info(EventLogCategory.CLUSTER, "message #1");
      });
      withCacheManager(startCacheManagerWithGlobalState(global), cm -> {
         EventLogger eventLogger = EventLogManager.getEventLogger(cm);
         eventLogger.info(EventLogCategory.CLUSTER, "message #5");
      });
   }

   @Test
   public void testCacheContentCanBePassivated() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      GlobalConfigurationBuilder globalBuilder = amendGlobalConfiguration(GlobalConfigurationBuilder.defaultClusteredBuilder());
      globalBuilder.globalState().enable();
      deleteGlobalPersistentState(globalBuilder);
      withCacheManager(TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, builder), cm -> {
         EventLogger eventLogger = EventLogManager.getEventLogger(cm);
         eventLogger.info(EventLogCategory.CLUSTER, "message #1");
         Cache<UUID, ServerEventImpl> cache = cm.getCache(ServerEventLogger.EVENT_LOG_CACHE);
         TestingUtil.extractComponent(cache, PassivationManager.class).passivateAll();
         SingleFileStore<UUID, ServerEventImpl> sfs = TestingUtil.getFirstWriter(cache);
         assertEquals(1, sfs.size());
      });
   }

   public EmbeddedCacheManager startCacheManagerWithGlobalState(GlobalConfigurationBuilder global) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(global, new ConfigurationBuilder());
      cm.start();
      return cm;
   }

   private static GlobalConfigurationBuilder amendGlobalConfiguration(GlobalConfigurationBuilder global) {
      String stateDirectory = TestingUtil.tmpDirectory(TestResourceTracker.getCurrentTestName());
      global.globalState().persistentLocation(stateDirectory);
      return global;
   }

   private static void deleteGlobalPersistentState(GlobalConfigurationBuilder global) {
      GlobalConfiguration globalCfg = global.build();
      new File(globalCfg.globalState().persistentLocation() + "/___event_log_cache.dat").delete();
   }

}
