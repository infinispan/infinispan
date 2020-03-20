package org.infinispan.notifications.cachelistener.cluster;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.CompletableFutures;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "notifications.cachelistener.NonBlockingClusterListener")
public class NonBlockingClusterListener extends AbstractClusterListenerUtilTest {
   public NonBlockingClusterListener() {
      super(false, CacheMode.DIST_SYNC);
   }

   public void testNonBlocking() throws InterruptedException, ExecutionException, TimeoutException {
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      Executor nonBlockingExecutor = TestingUtil.extractGlobalComponent(cache1.getCacheManager(), Executor.class,
            KnownComponentNames.NON_BLOCKING_EXECUTOR);

      CompletableFuture<Void> future = CompletableFutures.completedNull().thenComposeAsync(ignore ->
            cache1.addListenerAsync(new ClusterListener()), nonBlockingExecutor);
      future.get(10, TimeUnit.SECONDS);
   }

   @Listener(clustered = true, includeCurrentState = true)
   protected class ClusterListener {
      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      public void onCacheEvent(CacheEntryEvent event) {
      }
   }
}
