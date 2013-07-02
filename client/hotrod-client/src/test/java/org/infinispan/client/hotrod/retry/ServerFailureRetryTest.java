package org.infinispan.client.hotrod.retry;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

/**
 * Test different server error situations and check how clients behave under
 * those circumstances. Also verify whether failover is happening accordingly.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "client.hotrod.retry.ServerFailureRetryTest")
public class ServerFailureRetryTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      return hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
   }

   public void testRetryWithSuspectException(Method m) {
      ErrorInducingListener listener = new ErrorInducingListener();
      manager(0).getCache().addListener(listener);
      try {
         remoteCache.put(k(m), v(m));
      } finally {
         manager(0).getCache().removeListener(listener);
      }
   }

   @Listener
   public static class ErrorInducingListener {
      boolean induceError = true;

      @CacheEntryCreated
      public void entryCreated(CacheEntryEvent event) throws Exception {
         if (!event.isPre() && event.isOriginLocal() && induceError) {
            throw new SuspectException("Simulated suspicion");
         }
      }
   }
}
