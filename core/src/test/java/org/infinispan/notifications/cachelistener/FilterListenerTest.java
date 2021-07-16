package org.infinispan.notifications.cachelistener;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "notifications.cachelistener.FilterListenerTest")
public class FilterListenerTest extends AbstractInfinispanTest {

   public void testLocal() throws IOException {
      this.test(false);
   }

   public void testSimple() throws IOException {
      this.test(true);
   }

   private void test(boolean simple) throws IOException {
      GlobalConfiguration global = new GlobalConfigurationBuilder().nonClusteredDefault().build();
      Configuration local = new ConfigurationBuilder().clustering().cacheMode(CacheMode.LOCAL).simpleCache(simple).build();
      try (EmbeddedCacheManager manager = new DefaultCacheManager(global)) {
         manager.defineConfiguration("local", local);

         Cache<Object, Object> cache = manager.getCache("local");
         TestListener listener = new TestListener();
         cache.addListener(listener, listener, null);
         cache.put("foo", "bar");
         cache.put(1, 2);
         try {
            assertEquals(1, listener.events.size());
            assertEquals("foo", listener.events.remove());
         } finally {
            cache.removeListener(listener);
            cache.stop();
         }
      }
   }

   @Listener
   public static class TestListener implements CacheEventFilter<Object, Object> {
      private final Queue<Object> events = new LinkedBlockingQueue<>();

      @CacheEntryCreated
      public void onEvent(CacheEntryCreatedEvent<Object, Object> event) {
         if (!event.isPre()) {
            this.events.add(event.getKey());
         }
      }

      @Override
      public boolean accept(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
         return key instanceof String;
      }
   }
}
