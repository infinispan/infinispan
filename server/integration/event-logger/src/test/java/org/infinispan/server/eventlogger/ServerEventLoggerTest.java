package org.infinispan.server.eventlogger;

import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


/**
 * ServerEventLoggerTest.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */

@Test(testName = "server.eventlogger.ServerEventLoggerTest", groups = "functional")
public class ServerEventLoggerTest extends AbstractInfinispanTest {
    public void testServerEventLogging() {

        withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
            @Override
            public void call() throws Exception {
                cm.getCache();
                EventLogger eventLogger = EventLogManager.getEventLogger(cm);
                assertTrue(eventLogger.getClass().getName(), eventLogger instanceof ServerEventLogger);
                eventLogger.info(EventLogCategory.CLUSTER, "message #1");
                eventLogger.info(EventLogCategory.TASKS, "message #2");
                eventLogger.warn(EventLogCategory.CLUSTER, "message #3");
                eventLogger.warn(EventLogCategory.TASKS, "message #4");
                List<EventLog> events = eventLogger.getEvents(0, 10, Optional.of(EventLogCategory.CLUSTER), Optional.empty());
                assertEquals(2, events.size());
                assertEquals("message #3", events.get(0).getMessage());
                assertEquals(EventLogLevel.WARN, events.get(0).getLevel());
                assertEquals("message #1", events.get(1).getMessage());
                assertEquals(EventLogLevel.INFO, events.get(1).getLevel());
                events = eventLogger.getEvents(0, 10, Optional.empty(), Optional.of(EventLogLevel.INFO));
                assertEquals(2, events.size());
                assertEquals("message #2", events.get(0).getMessage());
                assertEquals(EventLogCategory.TASKS, events.get(0).getCategory());
                assertEquals("message #1", events.get(1).getMessage());
                assertEquals(EventLogCategory.CLUSTER, events.get(1).getCategory());
            }
        });
    }
}
