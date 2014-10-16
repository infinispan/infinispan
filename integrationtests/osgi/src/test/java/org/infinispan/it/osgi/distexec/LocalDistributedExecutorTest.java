package org.infinispan.it.osgi.distexec;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
@Category(PerSuite.class)
public class LocalDistributedExecutorTest extends org.infinispan.distexec.LocalDistributedExecutorTest {
   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @Before
   public void setUp() {
      TestResourceTracker.backgroundTestStarted(this);
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), false);
      createClusteredCaches(1, cacheName(), builder);
   }

   @After
   public void tearDown() {
      super.shutDownDistributedExecutorService();
      TestingUtil.killCacheManagers(cacheManagers);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      //not used
   }

   @Test
   public void testBasicInvocation() throws Exception {
      super.testBasicInvocation();
   }

   @Test
   public void testExceptionInvocation() throws Exception {
      super.testExceptionInvocation();
   }

   @Test
   public void testRunnableInvocation() throws Exception {
      super.testRunnableInvocation();
   }

   @Test
   public void testRunnableInvocationWith2Params() throws Exception {
      super.testRunnableInvocationWith2Params();
   }

   @Test
   public void testRunnableExecution() throws InterruptedException {
     super.testRunnableExecution();
   }

   @Test(expected = IllegalArgumentException.class)
   public void testNonSerializableRunnableExecution() {
      super.testNonSerializableRunnableExecution();
   }

   @Test(expected = RejectedExecutionException.class)
   public void testRunnableExecutionOnTerminatedExecutor() {
      super.testRunnableExecutionOnTerminatedExecutor();
   }

   @Test(expected = IllegalArgumentException.class)
   public void testNullRunnableExecution() {
      super.testNullRunnableExecution();
   }

   @Test
   public void testInvokeAny() throws Exception {
      super.testInvokeAny();
   }

   @Test
   public void testInvokeAnyWithTimeout() throws Exception {
      super.testInvokeAnyWithTimeout();
   }

   @Test(expected = NullPointerException.class)
   public void testInvokeAnyNoTask() throws Exception {
      super.testInvokeAnyNoTask();
   }

   @Test(expected = IllegalArgumentException.class)
   public void testInvokeAnyEmptyTasks() throws Exception {
      super.testInvokeAnyEmptyTasks();
   }

   @Test(expected = ExecutionException.class)
   public void testInvokeAnyExceptionTasks() throws Exception {
      super.testInvokeAnyExceptionTasks();
   }

   @Test
   public void testInvokeAnySleepingTasks() throws Exception {
      super.testInvokeAnySleepingTasks();
   }

   @Test(expected = TimeoutException.class)
   public void testInvokeAnyTimedSleepingTasks() throws Exception {
      super.testInvokeAnyTimedSleepingTasks();
   }

   @Test
   public void testInvokeAll() throws Exception {
      super.testInvokeAll();
   }

   @Test
   public void testCallableIsolation() throws Exception {
      super.testCallableIsolation();
   }

   @Test
   public void testBasicDistributedCallable() throws Exception {
      super.testBasicDistributedCallable();
   }

   @Test
   public void testSleepingCallableWithTimeoutOption() throws Exception {
      super.testSleepingCallableWithTimeoutOption();
   }

   @Test(expected = TimeoutException.class)
   public void testSleepingCallableWithTimeoutExc() throws Exception {
      super.testSleepingCallableWithTimeoutExc();
   }

   @Test(expected = TimeoutException.class)
   public void testSleepingCallableWithTimeoutExcDistApi() throws Exception {
      super.testSleepingCallableWithTimeoutExcDistApi();
   }

   @Test(expected = TimeoutException.class)
   public void testExceptionCallableWithTimedCall() throws Exception {
      super.testExceptionCallableWithTimedCall();
   }

   @Test(expected = TimeoutException.class)
   public void testExceptionCallableWithTimedCallDistApi() throws Exception {
      super.testExceptionCallableWithTimedCallDistApi();
   }

   @Test(expected = IllegalArgumentException.class)
   public void testBasicTargetDistributedCallableWithNullExecutionPolicy() throws Exception {
      super.testBasicTargetDistributedCallableWithNullExecutionPolicy();
   }

   @Test(expected = NullPointerException.class)
   public void testBasicTargetCallableWithNullTarget() {
      super.testBasicTargetCallableWithNullTarget();
   }

   @Test(expected = IllegalArgumentException.class)
   public void testBasicTargetCallableWithIllegalTarget() {
      super.testBasicTargetCallableWithIllegalTarget();
   }

   @Test
   public void testBasicDistributedCallableWitkKeys() throws Exception {
      super.testBasicDistributedCallableWitkKeys();
   }

   @Test(expected = NullPointerException.class)
   public void testBasicDistributedCallableWithNullTask() throws Exception {
      super.testBasicDistributedCallableWithNullTask();
   }

   @Test
   public void testBasicDistributedCallableWithNullKeys() throws Exception {
      super.testBasicDistributedCallableWithNullKeys();
   }

   @Test
   public void testDistributedCallableEverywhereWithKeys() throws Exception {
      super.testDistributedCallableEverywhereWithKeys();
   }

   @Test
   public void testDistributedCallableEverywhereWithEmptyKeys() throws Exception {
      super.testDistributedCallableEverywhereWithEmptyKeys();
   }

   @Test(expected = NullPointerException.class)
   public void testBasicDistributedCallableEverywhereWithKeysAndNullTask() throws Exception {
      super.testBasicDistributedCallableEverywhereWithKeysAndNullTask();
   }

   @Test(expected = NullPointerException.class)
   public void testBasicDistributedCallableEverywhereWithNullTask() throws Exception {
      super.testBasicDistributedCallableEverywhereWithNullTask();
   }

   @Test
   public void testDistributedCallableEverywhere() throws Exception {
      super.testDistributedCallableEverywhere();
   }

}
