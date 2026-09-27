package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.Test;

/**
 * Verifies that operations which may span the whole data set are not bound to the socket timeout, but to the
 * configured long running operation timeout, and that both can be overridden per cache instance.
 *
 * @since 16.3
 */
@Test(testName = "client.hotrod.LongRunningOperationTimeoutTest", groups = "functional")
public class LongRunningOperationTimeoutTest extends SingleCacheManagerTest {

   private static final int SOCKET_TIMEOUT = 500;
   private static final long LONG_RUNNING_TIMEOUT = 30_000;

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
            .longRunningOperationTimeout(LONG_RUNNING_TIMEOUT, TimeUnit.MILLISECONDS)
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
      // The server side command is now blocked, hold it for longer than the socket timeout.
      barrier.await(10, TimeUnit.SECONDS);
      Thread.sleep(SOCKET_TIMEOUT * 4L);
      barrier.await(10, TimeUnit.SECONDS);

      assertEquals(Integer.valueOf(0), size.get(10, TimeUnit.SECONDS));
   }

   public void testWithTimeoutOverridesTheLongRunningTimeout() throws Exception {
      RemoteCache<String, String> cache =
            remoteCacheManager.<String, String>getCache().withTimeout(250, TimeUnit.MILLISECONDS);

      Future<Void> size = fork(() -> {
         Exceptions.expectException(TransportException.class, SocketTimeoutException.class, cache::size);
         return null;
      });
      barrier.await(10, TimeUnit.SECONDS);
      size.get(10, TimeUnit.SECONDS);
      // Release the server side command so that the cache can be reused.
      barrier.await(10, TimeUnit.SECONDS);
   }

   public void testWithTimeoutReturnsANewCacheAndDoesNotLeak() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      assertEquals(-1L, ((InternalRemoteCache<String, String>) cache).getTimeout());

      RemoteCache<String, String> withTimeout = cache.withTimeout(5, TimeUnit.SECONDS);
      assertEquals(5_000L, ((InternalRemoteCache<String, String>) withTimeout).getTimeout());
      // The original instance must not be affected
      assertEquals(-1L, ((InternalRemoteCache<String, String>) cache).getTimeout());

      // Asking for the very same timeout returns the same instance
      assertSame(withTimeout, withTimeout.withTimeout(5_000, TimeUnit.MILLISECONDS));
      // The timeout survives the other decorators
      assertEquals(5_000L,
            ((InternalRemoteCache<String, String>) withTimeout.withFlags(Flag.SKIP_LISTENER_NOTIFICATION)).getTimeout());
   }

   @Test(expectedExceptions = IllegalArgumentException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Invalid operation timeout: 0. It must be greater than zero")
   public void testWithTimeoutRejectsNonPositiveValues() {
      remoteCacheManager.getCache().withTimeout(0, TimeUnit.SECONDS);
   }

   public void testOperationTimeouts() {
      InternalRemoteCache<String, String> cache = (InternalRemoteCache<String, String>) remoteCacheManager.<String, String>getCache();
      CacheOperationsFactory factory = cache.getOperationsFactory();

      // Single entry operations keep using the socket timeout, signalled by a non positive value
      assertEquals(-1L, factory.newGetOperation("k").timeout());
      assertEquals(-1L, factory.newContainsKeyOperation("k").timeout());

      // Operations which span the whole data set use the long running operation timeout
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
      assertEquals(LONG_RUNNING_TIMEOUT,
            factory.executeOperation("task", Map.<String, byte[]>of(), null).timeout());
   }

   public void testAdministrationWithTimeout() {
      RemoteCacheManagerAdmin admin = remoteCacheManager.administration();
      assertEquals(-1L, adminTimeout(admin));

      RemoteCacheManagerAdmin withTimeout = admin.withTimeout(1, TimeUnit.MINUTES);
      assertNotSame(admin, withTimeout);
      assertEquals(60_000L, adminTimeout(withTimeout));
      // The timeout must survive the other decorators
      assertEquals(60_000L, adminTimeout(withTimeout.withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)));

      Exceptions.expectException(IllegalArgumentException.class,
            "ISPN(\\d)*: Invalid operation timeout: -1000. It must be greater than zero",
            () -> admin.withTimeout(-1, TimeUnit.SECONDS));
   }

   public void testWithTimeoutAppliesToEveryOperation() {
      InternalRemoteCache<String, String> cache =
            (InternalRemoteCache<String, String>) remoteCacheManager.<String, String>getCache()
                  .withTimeout(7, TimeUnit.SECONDS);
      CacheOperationsFactory factory = cache.getOperationsFactory();

      assertEquals(7_000L, factory.newGetOperation("k").timeout());
      assertEquals(7_000L, factory.newSizeOperation().timeout());
      assertEquals(7_000L, factory.newClearOperation().timeout());
   }

   private static long adminTimeout(RemoteCacheManagerAdmin admin) {
      return TestingUtil.<Long>extractField(admin, "timeout");
   }
}
