package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.Exceptions;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
   CyclicBarrier barrier;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      barrier = new CyclicBarrier(2);
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      BlockingInterceptor<PutKeyValueCommand> blockingInterceptor =
         new BlockingInterceptor<>(barrier, PutKeyValueCommand.class, true, true);
      cacheManager.getCache().getAdvancedCache().getAsyncInterceptorChain()
                  .addInterceptorBefore(blockingInterceptor, EntryWrappingInterceptor.class);
      // cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      // pass the config file to the cache
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      remoteCacheManager = getRemoteCacheManager();

      return cacheManager;
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotrodServer)
         .socketTimeout(1000).connectionTimeout(5000)
         .connectionPool().maxActive(2)
         .maxRetries(0);
      return new RemoteCacheManager(builder.create());
   }

   @Override
   protected void teardown() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
      super.teardown();
   }

   public void testPutTimeout(Method m) throws Throwable {
      Exceptions.expectException(TransportException.class, SocketTimeoutException.class,
                                 () -> remoteCacheManager.getCache().put(k(m), v(m)));
      barrier.await(10, TimeUnit.SECONDS);
      barrier.await(10, TimeUnit.SECONDS);
   }
}
