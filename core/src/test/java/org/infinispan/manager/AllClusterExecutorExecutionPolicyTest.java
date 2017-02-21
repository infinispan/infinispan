package org.infinispan.manager;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests to verify that execution policy is applied properly when using a cluster executor
 * @author wburns
 * @since 9.1
 */
@Test(groups = {"functional", "smoke"}, testName = "manager.AllClusterExecutorExecutionPolicyTest")
public class AllClusterExecutorExecutionPolicyTest extends MultipleCacheManagersTest {

   @Override
   public String toString() {
      return super.toString();
   }

   private static final AtomicInteger actualInvocations = new AtomicInteger();

   @DataProvider(name="params")
   public Object[][] dataProvider() {
      return new Object[][] {
            // Policy, Site #, Rack #, Machine #, Expected invocation Count
            { ClusterExecutionPolicy.ALL, 0, 0, 0, 25 },
            { ClusterExecutionPolicy.ALL, 2, 1, 1, 25 },
            { ClusterExecutionPolicy.DIFFERENT_MACHINE, 1, 0, 1, 24 },
            { ClusterExecutionPolicy.DIFFERENT_MACHINE, 0, 1, 4, 24 },
            { ClusterExecutionPolicy.SAME_MACHINE, 2, 1, 1, 1 },
            { ClusterExecutionPolicy.SAME_MACHINE, 0, 0, 1, 1 },
            { ClusterExecutionPolicy.DIFFERENT_RACK, 2, 2, 3, 21 },
            { ClusterExecutionPolicy.DIFFERENT_RACK, 1, 0, 1, 21 },
            { ClusterExecutionPolicy.DIFFERENT_RACK, 0, 1, 4, 18 },
            { ClusterExecutionPolicy.SAME_RACK, 2, 1, 1, 2 },
            { ClusterExecutionPolicy.SAME_RACK, 1, 0, 1, 4 },
            { ClusterExecutionPolicy.SAME_RACK, 0, 2, 0, 1 },
            { ClusterExecutionPolicy.DIFFERENT_SITE, 2, 0, 4, 14 },
            { ClusterExecutionPolicy.DIFFERENT_SITE, 1, 0, 3, 21 },
            { ClusterExecutionPolicy.DIFFERENT_SITE, 0, 1, 6, 15 },
            { ClusterExecutionPolicy.SAME_SITE, 2, 0, 2, 11 },
            { ClusterExecutionPolicy.SAME_SITE, 1, 0, 0, 4 },
            { ClusterExecutionPolicy.SAME_SITE, 0, 1, 4, 10 },
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      // Array where top level divides by site
      // Second array divide by rack
      // and the number within is how many machines are in each rack
      int[][] topology = {
            // Site 1
            {
                  // Rack 1 machine count
                  2,
                  // Rack 2 machine count
                  7,
                  // Rack 3 machine count
                  1
            },
            // Site 2
            {
                  // Only 1 rack with this many machines
                  4
            },
            // Site 3
            {
                  // Rack 1 machine count
                  5,
                  // Rack 2 machine count
                  2,
                  // Rack 3 machine count
                  4
            },
      };

      for (int siteNumber = 0; siteNumber < topology.length; ++siteNumber) {
         int[] racksForSite = topology[siteNumber];
         for (int rackNumber = 0; rackNumber < racksForSite.length; ++rackNumber) {
            int machines = racksForSite[rackNumber];
            for (int machineNumber = 0; machineNumber < machines; ++machineNumber) {
               ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
               GlobalConfigurationBuilder globalConfigurationBuilder = holder.getGlobalConfigurationBuilder().clusteredDefault();

               globalConfigurationBuilder.transport()
                     .machineId(String.valueOf(machineNumber))
                     .rackId(String.valueOf(rackNumber))
                     .siteId(String.valueOf(siteNumber));
               addClusterEnabledCacheManager(holder);
            }
         }
      }
   }

   @Test(dataProvider = "params")
   public void runTest(ClusterExecutionPolicy policy, int site, int rack, int machine, int invocationCount) throws InterruptedException, ExecutionException, TimeoutException {
      actualInvocations.set(0);
      EmbeddedCacheManager cacheManager = cacheManagers.stream().filter(cm -> {
         TransportConfiguration tc = cm.getCacheManagerConfiguration().transport();
         return Integer.valueOf(tc.siteId()) == site && Integer.valueOf(tc.rackId()) == rack && Integer.valueOf(tc.machineId()) == machine;
      }).findFirst().orElseThrow(() -> new AssertionError("No cache manager matches site: " + site + " rack: " + rack + " machine: " + machine));

      cacheManager.executor().filterTargets(policy).submit(() -> actualInvocations.incrementAndGet()).get(10, TimeUnit.SECONDS);

      assertEquals(invocationCount, actualInvocations.get());
   }
}
