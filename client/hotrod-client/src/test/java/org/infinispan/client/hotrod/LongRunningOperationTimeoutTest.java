package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests that operations which are expected to run longer than a regular single key operation use
 * {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#longRunningOperationTimeout(long, TimeUnit)}
 * instead of the much shorter
 * {@link org.infinispan.client.hotrod.configuration.ConfigurationBuilder#socketTimeout(int)}.
 *
 * @since 16.0
 */
@Test(groups = "functional", testName = "client.hotrod.LongRunningOperationTimeoutTest")
public class LongRunningOperationTimeoutTest extends SingleHotRodServerTest {

   private static final int SOCKET_TIMEOUT = 1_000;
   private static final long SHORT_LONG_RUNNING_TIMEOUT = 500;

   private final BlockingInterceptor interceptor = new BlockingInterceptor();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().nonClusteredDefault();
      TestCacheManagerFactory.addInterceptor(global, TestCacheManagerFactory.DEFAULT_CACHE_NAME::equals, interceptor,
            TestCacheManagerFactory.InterceptorPosition.AFTER, EntryWrappingInterceptor.class);
      return TestCacheManagerFactory.createCacheManager(global, hotRodCacheConfiguration(new ConfigurationBuilder()));
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      return createClient(TimeUnit.MINUTES.toMillis(1));
   }

   private RemoteCacheManager createClient(long longRunningTimeoutMillis) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotrodServer);
      builder.socketTimeout(SOCKET_TIMEOUT)
            .longRunningOperationTimeout(longRunningTimeoutMillis, TimeUnit.MILLISECONDS)
            .maxRetries(0);
      return new InternalRemoteCacheManager(builder.build());
   }

   @AfterMethod(alwaysRun = true)
   public void unblock() {
      interceptor.unblock();
   }

   /**
    * A long running operation must survive a server response that takes longer than the socket timeout.
    */
   public void testLongRunningOperationOutlivesSocketTimeout() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      cache.put("key", "value");
      interceptor.blockSizeFor(SOCKET_TIMEOUT * 2L);

      assertEquals(1, cache.size());
   }

   /**
    * A regular operation is not affected by the long running timeout, it keeps using the socket timeout.
    */
   public void testRegularOperationStillUsesSocketTimeout() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      interceptor.blockPut();

      long start = System.nanoTime();
      Exceptions.expectException(TransportException.class, SocketTimeoutException.class,
            () -> cache.put("key", "value"));
      assertTimedOutAround(start, SOCKET_TIMEOUT);
   }

   /**
    * A long running timeout shorter than the socket timeout is still the one being applied, which proves that long
    * running operations do not silently fall back to the socket timeout.
    */
   public void testLongRunningOperationTimesOut() {
      try (RemoteCacheManager rcm = createClient(SHORT_LONG_RUNNING_TIMEOUT)) {
         RemoteCache<String, String> cache = rcm.getCache();
         interceptor.blockSize();

         long start = System.nanoTime();
         Exceptions.expectException(TransportException.class, SocketTimeoutException.class, cache::size);
         assertTimedOutAround(start, SHORT_LONG_RUNNING_TIMEOUT);
      }
   }

   /**
    * Regular operations must keep working on a connection on which a long running operation timed out, since both
    * kinds of operations are tracked independently.
    */
   public void testRegularOperationsAfterLongRunningTimeout() {
      try (RemoteCacheManager rcm = createClient(SHORT_LONG_RUNNING_TIMEOUT)) {
         RemoteCache<String, String> cache = rcm.getCache();
         interceptor.blockSize();
         Exceptions.expectException(TransportException.class, SocketTimeoutException.class, cache::size);

         cache.put("key", "value");
         assertEquals("value", cache.get("key"));
      }
   }

   /**
    * Long running operations no longer share the ring buffer used by the socket timeout, so its message ids are not
    * contiguous any more. Interleaving both kinds of operations must still complete every one of them.
    */
   public void testInterleavedRegularAndLongRunningOperations() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      for (int i = 0; i < 10; i++) {
         cache.put("key-" + i, "value-" + i);
         assertEquals(i + 1, cache.size());
         assertEquals("value-" + i, cache.get("key-" + i));
      }

      Map<String, String> all = cache.getAll(Set.of("key-0", "key-9"));
      assertEquals(2, all.size());
      assertEquals("value-0", all.get("key-0"));

      cache.clear();
      assertEquals(0, cache.size());
   }

   /**
    * {@link RemoteCacheManagerAdmin#withTimeout(long, TimeUnit)} returns a new instance, is preserved by
    * {@link RemoteCacheManagerAdmin#withFlags(AdminFlag...)} and its operations succeed.
    */
   public void testAdminWithTimeoutOverride() {
      RemoteCacheManagerAdmin admin = remoteCacheManager.administration();
      RemoteCacheManagerAdmin withTimeout = admin.withTimeout(30, TimeUnit.SECONDS).withFlags(AdminFlag.VOLATILE);
      assertNotSame(admin, withTimeout);

      withTimeout.createCache("timeout-override-cache", (String) null);
      assertEquals(0, remoteCacheManager.getCache("timeout-override-cache").size());
      withTimeout.removeCache("timeout-override-cache");
   }

   @Test(expectedExceptions = IllegalArgumentException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Invalid administration operation timeout .*")
   public void testAdminWithNonPositiveTimeout() {
      remoteCacheManager.administration().withTimeout(0, TimeUnit.SECONDS);
   }

   private static void assertTimedOutAround(long startNanos, long expectedMillis) {
      long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      // Generous upper bound, we only need to prove that the other timeout was not the one applied
      assertTrue(elapsed < expectedMillis * 10,
            "Expected to time out after about " + expectedMillis + " ms, but took " + elapsed + " ms");
   }

   /**
    * Delays {@link SizeCommand} and {@link PutKeyValueCommand} on demand, so that the client side timeout is the only
    * thing which can complete the operation.
    */
   public static class BlockingInterceptor extends BaseCustomAsyncInterceptor {
      private volatile CompletableFuture<Void> sizeDelay;
      private volatile CompletableFuture<Void> putDelay;

      @Override
      public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
         CompletableFuture<Void> delay = sizeDelay;
         return delay == null ? super.visitSizeCommand(ctx, command) : asyncInvokeNext(ctx, command, delay);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         CompletableFuture<Void> delay = putDelay;
         return delay == null ? super.visitPutKeyValueCommand(ctx, command) : asyncInvokeNext(ctx, command, delay);
      }

      void blockSize() {
         sizeDelay = new CompletableFuture<>();
      }

      void blockSizeFor(long millis) {
         CompletableFuture<Void> delay = new CompletableFuture<>();
         sizeDelay = delay;
         delay.completeOnTimeout(null, millis, TimeUnit.MILLISECONDS);
      }

      void blockPut() {
         putDelay = new CompletableFuture<>();
      }

      void unblock() {
         CompletableFuture<Void> delay = sizeDelay;
         sizeDelay = null;
         if (delay != null) {
            delay.complete(null);
         }
         delay = putDelay;
         putDelay = null;
         if (delay != null) {
            delay.complete(null);
         }
      }
   }
}
