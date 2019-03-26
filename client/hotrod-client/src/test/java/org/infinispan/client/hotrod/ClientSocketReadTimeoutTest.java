package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.CacheException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.impl.AbstractDelegatingEmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * This test is used to verify that clients get a timeout when the server does
 * not respond with the requested bytes.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(testName = "client.hotrod.ClientSocketReadTimeoutTest", groups = "functional" )
public class ClientSocketReadTimeoutTest extends SingleCacheManagerTest {

   HotRodServer hotrodServer;
   RemoteCacheManager remoteCacheManager;
   CountDownLatch latch;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      latch = new CountDownLatch(1);
      cacheManager = new HangingCacheManager(
            TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration()),
            latch);
      // cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      // pass the config file to the cache
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      remoteCacheManager = getRemoteCacheManager();

      return cacheManager;
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotrodServer)
         .socketTimeout(5000).connectionTimeout(5000)
         .connectionPool().maxActive(2)
         .maxRetries(0);
      return new RemoteCacheManager(builder.create());
   }

   @Override
   protected void teardown() {
      latch.countDown();
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
      super.teardown();
   }

   @Test(expectedExceptions = SocketTimeoutException.class)
   public void testPutTimeout(Method m) throws Throwable {
      try {
         assert null == remoteCacheManager.getCache().put(k(m), v(m));
      } catch (TransportException e) {
         throw e.getCause();
      }
   }

   private static class HangingCacheManager extends AbstractDelegatingEmbeddedCacheManager {

      static Log log = LogFactory.getLog(HangingCacheManager.class);

      final CountDownLatch latch;

      public HangingCacheManager(EmbeddedCacheManager delegate, CountDownLatch latch) {
         super(delegate);
         this.latch = latch;
      }

      @Override
      public <K, V> Cache<K, V> getCache(String cacheName) {
         log.info("Retrieve cache from hanging cache manager");
         // TODO: Hacky but it's the easiest thing to do - consider ByteMan
         // ByteMan apparently supports testng since 1.5.1 but no clear
         // example out there, with more time it should be considered.
         String threadName = Thread.currentThread().getName();
         if (threadName.startsWith("HotRod")) {
            log.info("Thread is a HotRod server worker thread, so force wait");
            try {
               // Wait a max of 3 minutes, otherwise socket timeout's not working
               latch.await(180, TimeUnit.SECONDS);
               log.info("Wait finished, return the cache");
               return super.getCache();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new CacheException(e);
            }
         }
         return super.getCache(cacheName);
      }

   }

}
