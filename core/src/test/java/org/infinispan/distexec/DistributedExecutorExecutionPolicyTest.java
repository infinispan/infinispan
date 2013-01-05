package org.infinispan.distexec;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Tests are added for testing execution policy for distributed executors.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.DistributedExecutorExecutionPolicyTest")
public class DistributedExecutorExecutionPolicyTest extends MultipleCacheManagersTest {

   public static final String CACHE_NAME = "TestCache";

   public DistributedExecutorExecutionPolicyTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
   }

   public EmbeddedCacheManager createCacheManager(int counter, int siteId, int machineId, int rackId) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), false);

      GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfigurationBuilder.transport().machineId("m" + (machineId > 0 ? machineId : counter))
            .rackId("r" + (rackId > 0 ? rackId : counter)).siteId("s" + (siteId > 0 ? siteId : counter));

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(globalConfigurationBuilder,
                                                                                     builder);
      cm1.defineConfiguration(CACHE_NAME, builder.build());

      return cm1;
   }

   public CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }

   public void testExecutionPolicyNotSameMachine() throws ExecutionException, InterruptedException {
      for(int i = 1; i <= 2; i++) {
         cacheManagers.add(createCacheManager(i, 0, 0 ,0));
      }

      waitForClusterToForm();

      executeDifferentExecutionPolicies(DistributedTaskExecutionPolicy.SAME_MACHINE);
   }

   public void testExecutionPolicySameMachine() throws ExecutionException, InterruptedException {
      cacheManagers.add(createCacheManager(1, 0, 0 ,0));
      cacheManagers.add(createCacheManager(2, 1, 1 ,1));

      waitForClusterToForm();

      executeDifferentExecutionPolicies(DistributedTaskExecutionPolicy.SAME_MACHINE);
   }

   public void testExecutionPolicyNotSameSiteFilter() throws ExecutionException, InterruptedException {
      for(int i = 1; i <= 2; i++) {
         cacheManagers.add(createCacheManager(i, 0, 0 ,0));
      }

      waitForClusterToForm();

      executeDifferentExecutionPolicies(DistributedTaskExecutionPolicy.SAME_SITE);
   }

   public void testExecutionPolicySameSiteFilter() throws ExecutionException, InterruptedException {
      cacheManagers.add(createCacheManager(1, 0, 0 ,0));
      cacheManagers.add(createCacheManager(2, 1, 0 ,0));

      waitForClusterToForm();

      executeDifferentExecutionPolicies(DistributedTaskExecutionPolicy.SAME_SITE);
   }

   public void testExecutionPolicyNotSameRackFilter() throws ExecutionException, InterruptedException {
      for(int i = 1; i <= 2; i++) {
         cacheManagers.add(createCacheManager(i, 0, 0 ,0));
      }

      waitForClusterToForm();

      executeDifferentExecutionPolicies(DistributedTaskExecutionPolicy.SAME_RACK);
   }

   public void testExecutionPolicySameRackFilter() throws ExecutionException, InterruptedException {
      cacheManagers.add(createCacheManager(1, 0, 0 ,0));
      cacheManagers.add(createCacheManager(2, 1, 0 ,1));

      waitForClusterToForm();

      executeDifferentExecutionPolicies(DistributedTaskExecutionPolicy.SAME_RACK);
   }

   private void executeDifferentExecutionPolicies(DistributedTaskExecutionPolicy policy) throws ExecutionException, InterruptedException {
      assert address(0) instanceof TopologyAwareAddress;
      assert address(1) instanceof TopologyAwareAddress;

      Cache<Object, Object> cache1 = cache(0, CACHE_NAME);
      Cache<Object, Object> cache2 = cache(1, CACHE_NAME);
      cache1.put("key1", "value1");
      cache1.put("key2", "value2");
      cache1.put("key3", "value3");
      cache1.put("key4", "value4");
      cache2.put("key5", "value5");
      cache2.put("key6", "value6");

      //initiate task from cache1 and select cache2 as target
      DistributedExecutorService des = new DefaultExecutorService(cache1);

      //the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des.createDistributedTaskBuilder(new LocalDistributedExecutorTest.SimpleDistributedCallable(true));
      taskBuilder.executionPolicy(policy);

      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      Future<Boolean> future = des.submit(distributedTask, new String[] {"key1", "key6"});

      assert future.get();
   }
}