package org.infinispan.manager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.function.TriConsumer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests to verify that execution policy is applied properly when using a cluster executor
 * @author wburns
 * @since 9.1
 */
@Test(groups = {"functional", "smoke"}, testName = "manager.SingleClusterExecutorExecutionPolicyTest")
public class SingleClusterExecutorExecutionPolicyTest extends MultipleCacheManagersTest {

   @Override
   public String toString() {
      return super.toString();
   }

   private static final int siteCount = 3;
   private static final int rackCount = 4;
   private static final int machineCount = 2;

   @DataProvider(name = "params")
   public Object[][] dataProvider() {
      return new Object[][] {
            { ClusterExecutionPolicy.ALL, 0, 0, 0, (Predicate<String>) v -> true },
            { ClusterExecutionPolicy.ALL, 2, 1, 1, (Predicate<String>) v -> true },
            { ClusterExecutionPolicy.DIFFERENT_MACHINE, 1, 0, 1, (Predicate<String>) v -> !v.equals("101") },
            { ClusterExecutionPolicy.DIFFERENT_MACHINE, 0, 1, 0, (Predicate<String>) v -> !v.equals("010") },
            { ClusterExecutionPolicy.SAME_MACHINE, 2, 1, 1, (Predicate<String>) v -> v.equals("211") },
            { ClusterExecutionPolicy.SAME_MACHINE, 0, 0, 1, (Predicate<String>) v -> v.equals("001") },
            { ClusterExecutionPolicy.DIFFERENT_RACK, 2, 2, 0, (Predicate<String>) v -> !v.startsWith("22") },
            { ClusterExecutionPolicy.DIFFERENT_RACK, 1, 0, 1, (Predicate<String>) v -> !v.startsWith("10") },
            { ClusterExecutionPolicy.DIFFERENT_RACK, 0, 1, 0, (Predicate<String>) v -> !v.startsWith("01") },
            { ClusterExecutionPolicy.SAME_RACK, 2, 1, 1, (Predicate<String>) v -> v.startsWith("21") },
            { ClusterExecutionPolicy.SAME_RACK, 1, 0, 1, (Predicate<String>) v -> v.startsWith("10") },
            { ClusterExecutionPolicy.SAME_RACK, 0, 2, 0, (Predicate<String>) v -> v.startsWith("02") },
            { ClusterExecutionPolicy.DIFFERENT_SITE, 2, 0, 0, (Predicate<String>) v -> !v.startsWith("2") },
            { ClusterExecutionPolicy.DIFFERENT_SITE, 1, 0, 1, (Predicate<String>) v -> !v.startsWith("1") },
            { ClusterExecutionPolicy.DIFFERENT_SITE, 0, 1, 0, (Predicate<String>) v -> !v.startsWith("0") },
            { ClusterExecutionPolicy.SAME_SITE, 2, 0, 1, (Predicate<String>) v -> v.startsWith("2") },
            { ClusterExecutionPolicy.SAME_SITE, 1, 0, 0, (Predicate<String>) v -> v.startsWith("1") },
            { ClusterExecutionPolicy.SAME_SITE, 0, 1, 0, (Predicate<String>) v -> v.startsWith("0") },
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {

      for (int siteNumber = 0; siteNumber < siteCount; ++siteNumber) {
         for (int rackNumber = 0; rackNumber < rackCount; ++rackNumber) {
            for (int machineNumber = 0; machineNumber < machineCount; ++machineNumber) {
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

      TestingUtil.blockUntilViewsReceived(10_000, cacheManagers);
   }

   @Test(dataProvider = "params")
   public void runTest(ClusterExecutionPolicy policy, int site, int rack, int machine, Predicate<String> passFilter) throws InterruptedException, ExecutionException, TimeoutException {
      EmbeddedCacheManager cacheManager = cacheManagers.stream().filter(cm -> {
         TransportConfiguration tc = cm.getCacheManagerConfiguration().transport();
         return Integer.valueOf(tc.siteId()) == site && Integer.valueOf(tc.rackId()) == rack && Integer.valueOf(tc.machineId()) == machine;
      }).findFirst().orElseThrow(() -> new AssertionError("No cache manager matches site: " + site + " rack: " + rack + " machine: " + machine));

      AtomicInteger invocationCount = new AtomicInteger();
      Queue<String> nonMatched = new ConcurrentLinkedQueue<>();
      TriConsumer<Address, String, Throwable> triConsumer = (a, v, t) -> {
         invocationCount.incrementAndGet();
         if (!passFilter.test(v)) {
            nonMatched.add(v);
         }
      };

      int invocations = 5;

      ClusterExecutor executor = cacheManager.executor().singleNodeSubmission().filterTargets(policy);
      for (int i = 0; i < invocations; ++i) {
         executor.submitConsumer((cm) -> {
            TransportConfiguration tc = cm.getCacheManagerConfiguration().transport();
            return tc.siteId() + tc.rackId() + tc.machineId();
         }, triConsumer).get(10, TimeUnit.SECONDS);
      }

      assertEquals(invocations, invocationCount.get());
      if (!nonMatched.isEmpty()) {
         fail("Invocations that didn't match [" + nonMatched + "]");
      }
   }
}
