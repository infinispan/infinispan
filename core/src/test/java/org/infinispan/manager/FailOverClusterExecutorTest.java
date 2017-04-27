package org.infinispan.manager;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = {"functional", "smoke"}, testName = "manager.FailOverClusterExecutorTest")
public class FailOverClusterExecutorTest extends MultipleCacheManagersTest {

   private static AtomicInteger failureCount = new AtomicInteger();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);

      waitForClusterToForm();
   }

   @Test
   public void testSimpleFailover() throws InterruptedException, ExecutionException, TimeoutException {
      int failOverAllowed = 2;
      failureCount.set(failOverAllowed);
      CompletableFuture<Void> fut = cacheManagers.get(0).executor().singleNodeSubmission(failOverAllowed).submit(() -> {
         if (failureCount.decrementAndGet() != 0) {
            throw new IllegalArgumentException();
         }
      });
      fut.get(10, TimeUnit.SECONDS);
      assertEquals(0, failureCount.get());
   }

   @Test
   public void testTimeoutOccursWithRetry() throws InterruptedException, ExecutionException, TimeoutException {
      CompletableFuture<Void> fut = cacheManagers.get(0).executor().timeout(10, TimeUnit.MILLISECONDS)
            .singleNodeSubmission(2).submit(() -> {
               try {
                  Thread.sleep(TimeUnit.SECONDS.toMillis(2));
               } catch (InterruptedException e) {
                  throw new RuntimeException(e);
               }
            });
      Exceptions.expectExecutionException(org.infinispan.util.concurrent.TimeoutException.class, fut);
   }
}
