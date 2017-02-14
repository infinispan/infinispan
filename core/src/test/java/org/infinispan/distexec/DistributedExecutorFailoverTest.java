package org.infinispan.distexec;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.*;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests dist.exec failover after node kill.
 *
 * @author Vladimir Blagojevic
 * @since 7.1
 */
@Test(groups = "functional", testName = "distexec.DistributedExecutorFailoverTest")
public class DistributedExecutorFailoverTest extends MultipleCacheManagersTest {

   private static CountDownLatch latch = new CountDownLatch(1);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);

      waitForClusterToForm(cacheName());
   }

   protected String cacheName() {
      return "DistributedExecutorFailoverTest";
   }

   public void testBasicLocalDistributedCallable() throws Exception {
       long taskTimeout = TimeUnit.SECONDS.toMillis(15);
      EmbeddedCacheManager cacheManager1 = manager(0);
      final EmbeddedCacheManager cacheManager2 = manager(1);
      final EmbeddedCacheManager cacheManager3 = manager(2);
      TestingUtil.killCacheManagers(cacheManager2);
      TestingUtil.killCacheManagers(cacheManager3);
      Cache<Object, Object> cache1 = cacheManager1.getCache();
      DistributedExecutorService des = null;

      try {
         // initiate task from cache1 and execute on same node
         des = new DefaultExecutorService(cache1);

         SameNodeTaskFailoverPolicy sameNodeTaskFailoverPolicy = new SameNodeTaskFailoverPolicy(3);
         DistributedTaskBuilder<Void> builder = des
                 .createDistributedTaskBuilder(new TestCallable())
                 .failoverPolicy(sameNodeTaskFailoverPolicy)
                 .timeout(taskTimeout, TimeUnit.MILLISECONDS);

         CompletableFuture<Void> future = des.submit(builder.build());
         future.whenComplete((aVoid,throwable) -> {
            Executors.newSingleThreadExecutor().submit((Callable<Void>)() -> {
                future.get();
                return null;
            });
         });
         eventuallyEquals(3, () -> sameNodeTaskFailoverPolicy.failoverCount);
      } catch (Exception ex) {
         AssertJUnit.fail("Task did not failover properly " + ex);
      } finally {
         des.shutdown();
      }
   }

   public void testBasicTargetRemoteDistributedCallable() throws Exception {
      long taskTimeout = TimeUnit.SECONDS.toMillis(15);
      EmbeddedCacheManager cacheManager1 = manager(0);
      final EmbeddedCacheManager cacheManager2 = manager(1);

      Cache<Object, Object> cache1 = cacheManager1.getCache();
      Cache<Object, Object> cache2 = cacheManager2.getCache();
      DistributedExecutorService des = null;
      try {
         // initiate task from cache1 and execute on same node
         des = new DefaultExecutorService(cache1);
         Address target = cache2.getAdvancedCache().getRpcManager().getAddress();

         DistributedTaskBuilder<Integer> builder = des
               .createDistributedTaskBuilder(new SleepingSimpleCallable())
               .failoverPolicy(new RandomNodeTaskFailoverPolicy(2))
               .timeout(taskTimeout, TimeUnit.MILLISECONDS);

         Future<Integer> future = des.submit(target, builder.build());
         fork(new Runnable() {

            @Override
            public void run() {
               TestingUtil.killCacheManagers(cacheManager2);
               latch.countDown();
            }
         });

         AssertJUnit.assertEquals((Integer) 1, future.get());
      } catch (Exception ex) {
         AssertJUnit.fail("Task did not failover properly " + ex);
      } finally {
         des.shutdown();
      }
   }

   static class TestCallable implements Callable<Void>, Serializable, ExternalPojo {

      @Override
      public Void call() throws Exception {
         throw new Exception("test");
      }
   }

   static class SleepingSimpleCallable implements Callable<Integer>, Serializable, ExternalPojo {

      /**
       *
       */
      private static final long serialVersionUID = 6189005766285399200L;

      public SleepingSimpleCallable() {
      }

      @Override
      public Integer call() throws Exception {
         latch.await();
         return 1;
      }
   }

   static class RandomNodeTaskFailoverPolicy implements DistributedTaskFailoverPolicy {
      private final int maxFailoverCount;

      public RandomNodeTaskFailoverPolicy(int maxFailoverCount) {
         super();
         this.maxFailoverCount = maxFailoverCount;
      }

      @Override
      public Address failover(FailoverContext fc) {
         return randomNode(fc.executionCandidates(), fc.executionFailureLocation());
      }

      protected Address randomNode(List<Address> candidates, Address failedExecutionLocation) {
         Random r = new Random();
         candidates.remove(failedExecutionLocation);
         if (candidates.isEmpty())
            throw new IllegalStateException("There are no candidates for failover: " + candidates);
         int tIndex = r.nextInt(candidates.size());
         return candidates.get(tIndex);
      }

      @Override
      public int maxFailoverAttempts() {
         return maxFailoverCount;
      }
   }

   static class SameNodeTaskFailoverPolicy implements DistributedTaskFailoverPolicy {
      private final int maxFailoverCount;
      private int failoverCount = 0;

      public SameNodeTaskFailoverPolicy(int maxFailoverCount) {
         super();
         this.maxFailoverCount = maxFailoverCount;
      }

      @Override
      public Address failover(FailoverContext fc) {
         failoverCount++;
         return fc.executionFailureLocation();
      }

      @Override
      public int maxFailoverAttempts() {
         return maxFailoverCount;
      }
   }
}
