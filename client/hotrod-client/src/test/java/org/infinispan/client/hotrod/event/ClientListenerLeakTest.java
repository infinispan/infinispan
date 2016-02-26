package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.detectThreadLeaks;

@Test(groups = "functional", testName = "client.hotrod.event.ClientListenerLifecycleTest")
public class ClientListenerLeakTest extends SingleHotRodServerTest {

   private String cacheName;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = super.createCacheManager();
      ConfigurationBuilder cfg = hotRodCacheConfiguration();
      cacheName = this.getClass().getSimpleName();
      cm.defineConfiguration(cacheName, cfg.build());
      cm.getCache(cacheName);
      return cm;
   }

   public void testNoLeaksAfterShutdown() {
      withRemoteCacheManager(new RemoteCacheManagerCallable(getRemoteCacheManager()) {
         @Override
         public void call() {
            RemoteCache<Integer, String> remote = rcm.getCache(cacheName);
            EventLogListener<Integer> eventListener = new EventLogListener<>();
            remote.addClientListener(eventListener);
            eventListener.expectNoEvents();
            remote.put(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache(cacheName));
         }
      });
      detectThreadLeaks(".*Client-Listener-ClientListenerLeakTest-.*");
   }

}
