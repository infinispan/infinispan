package org.infinispan.remoting.transport;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.math.FieldElement;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.test.Exceptions;
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

   public <T extends FieldElement<T>> void testInitialClusterSizeFail() throws Throwable {
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < CLUSTER_SIZE - 1; i++) {
         EmbeddedCacheManager manager = manager(i);
         futures.add(fork(() -> {
            manager.start();
            return null;
         }));
      }

      for (Future<Void> future : futures) {
         try {
            // JGroupsTransport only starts counting down on initialClusterTimeout *after* it connects.
            // The initial connection may take take 3 seconds (GMS.join_timeout) because of JGRP-2028
            // Shutdown may also take 2 seconds (GMS.view_ack_collection_timeout) because of JGRP-2030
            future.get(CLUSTER_TIMEOUT_SECONDS + 5, TimeUnit.SECONDS);
            fail("Should have thrown an exception");
         } catch (ExecutionException ee) {
            Exceptions.assertException(EmbeddedCacheManagerStartupException.class, ee.getCause());
            Exceptions.assertException(CacheException.class,
                  org.infinispan.util.concurrent.TimeoutException.class, "ISPN000399:.*",
                  ee.getCause().getCause());
         }
      }
   }

}
