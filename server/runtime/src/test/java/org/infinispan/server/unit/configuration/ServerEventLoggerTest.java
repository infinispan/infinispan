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

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.logging.events.ServerEventLogger;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * ServerEventLoggerTest.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */

public class ServerEventLoggerTest {
   @BeforeClass
   public static void before() {
      TestResourceTracker.testStarted(ServerConfigurationParserTest.class.getName());
   }

   @AfterClass
   public static void after() {
      TestResourceTracker.testFinished(ServerConfigurationParserTest.class.getName());
   }

   @Test
   public void testLocalServerEventLogging() {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(amendGlobalConfiguration(new GlobalConfigurationBuilder()), new ConfigurationBuilder())) {
         @Override
         public void call() throws Exception {
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
         public void call() throws Exception {
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
               for(EventLog event : events) {
                  assertEquals(EventLogCategory.TASKS, event.getCategory());
               }
               events = eventLogger.getEvents(Instant.now(), 10, Optional.empty(), Optional.of(EventLogLevel.INFO));
               for(EventLog event : events) {
                  assertEquals(EventLogLevel.INFO, event.getLevel());
               }
            }
         }
      });
   }

   @Test
   public void testLocalServerEventLoggingPreloading() {
      GlobalConfigurationBuilder global = amendGlobalConfiguration(new GlobalConfigurationBuilder());
      global.globalState().enable();
      deleteGlobalPersistentState(global);
      EmbeddedCacheManager cm = startCacheManager(global);
      EventLogger eventLogger = EventLogManager.getEventLogger(cm);
      eventLogger.info(EventLogCategory.CLUSTER, "message #1");
      TestingUtil.killCacheManagers(cm);
      cm = startCacheManager(global);
      eventLogger = EventLogManager.getEventLogger(cm);
      eventLogger.info(EventLogCategory.CLUSTER, "message #5");
   }

   public EmbeddedCacheManager startCacheManager(GlobalConfigurationBuilder global) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(global, new ConfigurationBuilder());
      cm.getCache();
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
