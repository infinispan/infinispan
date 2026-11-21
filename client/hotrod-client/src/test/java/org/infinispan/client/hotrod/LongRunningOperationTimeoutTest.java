package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifies that operations which may span the whole data set are not bound to the socket timeout, but to a multiple
 * of it.
 *
 * @since 16.3
 */
@Test(testName = "client.hotrod.LongRunningOperationTimeoutTest", groups = "functional")
public class LongRunningOperationTimeoutTest extends SingleCacheManagerTest {

   private static final int SOCKET_TIMEOUT = 1_000;
   private static final long LONG_RUNNING_TIMEOUT = ConfigurationProperties.longRunningOperationTimeout(SOCKET_TIMEOUT);

   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;
   private CyclicBarrier barrier;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      barrier = new CyclicBarrier(2);
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      extractInterceptorChain(cacheManager.getCache())
            .addInterceptorBefore(new BlockingInterceptor<>(barrier, SizeCommand.class, true, true),
                  EntryWrappingInterceptor.class);
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotrodServer)
            .socketTimeout(SOCKET_TIMEOUT)
            .maxRetries(0);
      remoteCacheManager = new RemoteCacheManager(builder.build());
      return cacheManager;
   }

   @Override
   protected void teardown() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
      hotrodServer = null;
      super.teardown();
   }

   public void testSizeIsNotBoundToTheSocketTimeout() throws Exception {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();

      Future<Integer> size = fork(cache::size);
      // The server side command is now blocked. Hold it for longer than the socket timeout, but for less than the
      // long running timeout: the operation must survive.
      barrier.await(10, TimeUnit.SECONDS);
      Thread.sleep(SOCKET_TIMEOUT * 2L);
      barrier.await(10, TimeUnit.SECONDS);

      assertEquals(Integer.valueOf(0), size.get(10, TimeUnit.SECONDS));
   }

   public void testOperationTimeouts() {
      InternalRemoteCache<String, String> cache = (InternalRemoteCache<String, String>) remoteCacheManager.<String, String>getCache();
      CacheOperationsFactory factory = cache.getOperationsFactory();

      // Single entry operations keep using the socket timeout, signalled by a non positive value
      assertEquals(-1L, factory.newGetOperation("k").timeout());
      assertEquals(-1L, factory.newContainsKeyOperation("k").timeout());

      // Operations which span the whole data set scale the socket timeout instead
      assertEquals(LONG_RUNNING_TIMEOUT, factory.newSizeOperation().timeout());
      assertEquals(LONG_RUNNING_TIMEOUT, factory.newClearOperation().timeout());
      assertEquals(LONG_RUNNING_TIMEOUT, factory.newStatsOperation().timeout());
      assertEquals(LONG_RUNNING_TIMEOUT, factory.newGetAllBytesOperation(Collections.emptySet()).timeout());
      assertEquals(LONG_RUNNING_TIMEOUT, factory.newRemoveAllBytesOperation(Collections.emptySet()).timeout());
      assertEquals(LONG_RUNNING_TIMEOUT,
            factory.newPutAllBytesOperation(Collections.emptyMap(), 0, TimeUnit.SECONDS, 0, TimeUnit.SECONDS).timeout());
      assertEquals(LONG_RUNNING_TIMEOUT,
            factory.newIterationStartOperation(null, null, IntSets.immutableEmptySet(), 10, false).timeout());
      assertEquals(LONG_RUNNING_TIMEOUT, factory.newIterationEndOperation(new byte[]{1}).timeout());
      assertEquals(LONG_RUNNING_TIMEOUT, factory.executeOperation("task", Map.<String, byte[]>of(), null).timeout());
   }
}
