package org.infinispan.remoting.transport;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

@Test(testName = "transport.InitialClusterSizeTest", groups = "functional")
@CleanupAfterMethod
public class InitialClusterSizeTest extends MultipleCacheManagersTest {
   public static final int CLUSTER_SIZE = 4;
   public static final int CLUSTER_TIMEOUT_SECONDS = 5;

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();
         gc.transport().initialClusterSize(CLUSTER_SIZE).initialClusterTimeout(CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
         cacheManagers.add(TestCacheManagerFactory.createClusteredCacheManager(false, gc,
               getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC), new TransportFlags().withPortRange(i), false));
      }
   }

   public void testInitialClusterSize() throws ExecutionException, InterruptedException, TimeoutException {
      Future<?>[] threads = new Future[CLUSTER_SIZE];
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         final int index = i;
         threads[i] = fork(() -> {
            manager(index).start();
         });
      }
      for(Future<?> f : threads) {
         f.get(15, TimeUnit.SECONDS);
      }
      assertEquals(CLUSTER_SIZE, manager(0).getMembers().size());
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "ISPN000399:.*")
   public void testInitialClusterSizeFail() throws Throwable {
      try {
         Future<?>[] threads = new Future[CLUSTER_SIZE - 1];
         for (int i = 0; i < CLUSTER_SIZE - 1; i++) {
            final int index = i;
            threads[i] = fork(() -> {
               manager(index).start();
               return true;
            });
         }
         for (Future<?> f : threads) {
            f.get(CLUSTER_TIMEOUT_SECONDS + 1, TimeUnit.SECONDS);
         }
      } catch (ExecutionException e) {
         throw e.getCause().getCause().getCause();
      }
   }

}
