package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerLocalTest")
public class ClusterListenerLocalTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   public void testInsertEvent() {
      Cache<Object, String> cache0 = cache();
      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);
      cache.put(1, "v1");
      verifySimpleInsertionEvents(clusterListener, 1, "v1");
   }

   protected void verifySimpleInsertionEvents(ClusterListener listener, Object key, Object expectedValue) {
      assertEquals(listener.events.size(), 1);
      CacheEntryEvent event = listener.events.get(0);

      assertEquals(Event.Type.CACHE_ENTRY_CREATED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(expectedValue, event.getValue());
   }

   @Listener(clustered = true)
   protected class ClusterListener {
      List<CacheEntryEvent> events = Collections.synchronizedList(new ArrayList<CacheEntryEvent>());

      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      public void onCacheEvent(CacheEntryEvent event) {
         log.debugf("Adding new cluster event %s", event);
         events.add(event);
      }
   }

}
