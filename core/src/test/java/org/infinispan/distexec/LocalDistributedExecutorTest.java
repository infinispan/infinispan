package org.infinispan.distexec;

import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.TestAddress;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestException;
import org.infinispan.test.eventually.Eventually;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test for verifying that the DistributedExecutors also work on the Local Cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.LocalDistributedExecutorTest")
public class LocalDistributedExecutorTest extends MultipleCacheManagersTest {

   private DistributedExecutorService cleanupService;
   static final Map<String, AtomicInteger> counterMap = new ConcurrentHashMap<>();

   public LocalDistributedExecutorTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected CacheMode getCacheMode() {
      return CacheMode.LOCAL;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), false);
      createClusteredCaches(1, cacheName(), builder);
   }

   @AfterMethod(alwaysRun = true)
   public void shutDownDistributedExecutorService() {
      if (cleanupService != null) {
         cleanupService.shutdownNow();
      } else {
         log.warn("Should have shutdown DistributedExecutorService but none was set");
      }
   }

   protected String cacheName() {
      return "LocalDistributedExecutorTest";
   }

   protected Cache<Object, Object> getCache() {
      return cache(0, cacheName());
   }

   public void testBasicInvocation() throws Exception {
      basicInvocation(new SimpleCallable());
   }

   /**
    * Helper public method (used by CDI module), disabled as some IDEs invoke it as a test method
    */
   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   public void basicInvocation(Callable<Integer> call, Object... keys) throws Exception {
      DistributedExecutorService des = createDES(getCache());
      Future<Integer> future = des.submit(call, keys);
      Integer r = future.get();
      assertEquals((Integer) 1, r);
   }


   /**
    * Helper public method (used by CDI module), disabled as some IDEs invoke it as a test method
    *
    */
   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   public void addEntries(Map<Object,Object> entries) {
      getCache().putAll(entries);
   }

   /**
    * Helper public method (used by CDI module), disabled as some IDEs invoke it as a test method
    */
   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   public void basicInvocation(Runnable call) throws Exception {
      DistributedExecutorService des = createDES(getCache());
      des.submit(call).get();
   }

   protected DistributedExecutorService createDES(Cache<?,?> cache){
      ExecutorService executorService = Executors.newCachedThreadPool(getTestThreadFactory("LocalExecutor"));
      DistributedExecutorService des = new DefaultExecutorService(cache, executorService, true);
      cleanupService = des;
      return des;
   }

   public void testExceptionInvocation() throws Exception {

      DistributedExecutorService des = createDES(getCache());

      Future<Integer> future = des.submit(new ExceptionThrowingCallable());
      int exceptionCount = 0;
      try {
         future.get();
         throw new IllegalStateException("Should not have reached this code");
      } catch (ExecutionException ex) {
         exceptionCount++;
      }
      assertEquals(1, exceptionCount);

      List<CompletableFuture<Integer>> list = des.submitEverywhere(new ExceptionThrowingCallable());
      exceptionCount = 0;
      for (Future<Integer> f : list) {
         try {
            f.get();
            throw new IllegalStateException("Should not have reached this code");
         } catch (ExecutionException ex) {
            exceptionCount++;
         }
      }
      assertEquals(list.size(), exceptionCount);
   }

   public void testRunnableInvocation() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      Future<?> future = des.submit(new BoringRunnable());
      Object object = future.get();
      assertEquals(null, object);
   }

   public void testRunnableInvocationWith2Params() throws Exception {
      DistributedExecutorService des = createDES(getCache());

      Integer result = 5;
      Future<Integer> future = des.submit(new BoringRunnable(), result);
      assertEquals(result, future.get());
   }

   public void testRunnableExecution() throws InterruptedException {
      final String uuid = UUID.randomUUID().toString();

      try {
         DistributedExecutorService des = createDES(getCache());
         BoringRunnable runnable = new BoringRunnable(uuid);

         des.execute(runnable);

         Eventually.eventually(() -> counterMap.get(uuid) != null && counterMap.get(uuid).get() >= 1);
      } finally {
         counterMap.remove(uuid);
      }
   }

   public void testNonSerializableRunnableExecution() {
      DistributedExecutorService des = createDES(getCache());
      expectException(IllegalArgumentException.class, () -> des.execute(() -> log.trace("Non Serializable Runnable")));
   }

   public void testRunnableExecutionOnTerminatedExecutor() {
      DistributedExecutorService des = createDES(getCache());
      des.shutdown();

      BoringRunnable runnable = new BoringRunnable();

      expectException(RejectedExecutionException.class, () -> des.execute(runnable));
   }

   public void testNullRunnableExecution() {
      DistributedExecutorService des = createDES(getCache());

      BoringRunnable runnable = null;
      expectException(IllegalArgumentException.class, () -> des.execute(runnable));
   }

   public void testInvokeAny() throws Exception {

      DistributedExecutorService des = createDES(getCache());
      List<SimpleCallable> tasks = new ArrayList<>();
      tasks.add(new SimpleCallable());
      Integer result = des.invokeAny(tasks);
      assertEquals((Integer) 1, result);

      tasks = new ArrayList<>();
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());
      result = des.invokeAny(tasks);
      assertEquals((Integer) 1, result);
   }

   public void testInvokeAnyWithTimeout() throws Exception {

      DistributedExecutorService des = createDES(getCache());

      List<SimpleCallable> tasks = new ArrayList<>();
      tasks.add(new SimpleCallable());
      Integer result = des.invokeAny(tasks, 1000, TimeUnit.MILLISECONDS);

      assertEquals((Integer) 1, result);

      tasks = new ArrayList<>();
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());

      result = des.invokeAny(tasks, 1000, TimeUnit.MILLISECONDS);
      assertEquals((Integer) 1, result);
   }

   public void testInvokeAnyNoTask() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      expectException(NullPointerException.class, () -> des.invokeAny(null));
   }

   public void testInvokeAnyEmptyTasks() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      expectException(IllegalArgumentException.class, () -> des.invokeAny(new ArrayList<SimpleCallable>()));
   }

   public void testInvokeAnyExceptionTasks() throws Exception {

      DistributedExecutorService des = createDES(getCache());

      List<Callable<Integer>> tasks = new ArrayList<>();
      tasks.add(new ExceptionThrowingCallable());
      tasks.add(new ExceptionThrowingCallable());
      expectException(ExecutionException.class, () -> des.invokeAny(tasks));
   }

   public void testInvokeAnySleepingTasks() throws Exception {

      DistributedExecutorService des = createDES(getCache());

      List<Callable<Integer>> tasks = new ArrayList<>();
      tasks.add(new ExceptionThrowingCallable());
      tasks.add(new SleepingSimpleCallable());
      Object result = des.invokeAny(tasks);
      assertEquals(1, result);
   }

   public void testInvokeAnyTimedSleepingTasks() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      List<SleepingSimpleCallable> tasks = new ArrayList<>();
      tasks.add(new SleepingSimpleCallable());
      expectException(TimeoutException.class, () -> des.invokeAny(tasks, 1000, TimeUnit.MILLISECONDS));
   }

   public void testInvokeAll() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      List<SimpleCallable> tasks = new ArrayList<>();
      tasks.add(new SimpleCallable());
      List<Future<Integer>> list = des.invokeAll(tasks);
      assertEquals(1, list.size());
      Future<Integer> future = list.get(0);
      assertEquals((Integer) 1, future.get());

      tasks = new ArrayList<>();
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());

      list = des.invokeAll(tasks);
      assertEquals(3, list.size());
      for (Future<Integer> f : list) {
         assertEquals((Integer) 1, f.get());
      }
   }

   /**
    * Tests Callable isolation as it gets invoked across the cluster
    * https://issues.jboss.org/browse/ISPN-1041
    */
   public void testCallableIsolation() throws Exception {
      DistributedExecutorService des = createDES(getCache());

      List<CompletableFuture<Integer>> list = des.submitEverywhere(new SimpleCallableWithField());
      assert list != null && !list.isEmpty();
      for (Future<Integer> f : list) {
         assertEquals((Integer) 0, f.get());
      }
   }

   public void testBasicDistributedCallable() throws Exception {

      DistributedExecutorService des = createDES(getCache());
      Future<Boolean> future = des.submit(new SimpleDistributedCallable(false));
      Boolean r = future.get();
      assert r;

      // the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des
               .createDistributedTaskBuilder(new SimpleDistributedCallable(false));
      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      future = des.submit(distributedTask);
      r = future.get();
      assert r;
   }

   public void testSleepingCallableWithTimeoutOption() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      Future<Integer> future = des.submit(new SleepingSimpleCallable());
      Integer r = future.get(10, TimeUnit.SECONDS);
      assertEquals((Integer) 1, r);

      //the same using DistributedTask API
      DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new SleepingSimpleCallable());
      DistributedTask<Integer> distributedTask = taskBuilder.build();
      future = des.submit(distributedTask);
      r = future.get(10, TimeUnit.SECONDS);
      assertEquals((Integer) 1, r);
   }

   public void testSleepingCallableWithTimeoutExc() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      Future<Integer> future = des.submit(new SleepingSimpleCallable());
      log.tracef("Sleeping task submitted");
      expectException(TimeoutException.class, () -> future.get(1000, TimeUnit.MILLISECONDS));
   }

   public void testSleepingCallableWithTimeoutExcDistApi() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new SleepingSimpleCallable());
      DistributedTask<Integer> distributedTask = taskBuilder.build();
      Future<Integer> future = des.submit(distributedTask);
      log.tracef("Sleeping task submitted");
      expectException(TimeoutException.class, () -> future.get(1000, TimeUnit.MILLISECONDS));
   }

   public void testExceptionCallableWithTimedCall() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      Future<Integer> future = des.submit(new ExceptionThrowingCallable(true));

      expectException(TimeoutException.class, () -> future.get(10, TimeUnit.MILLISECONDS));
   }

   public void testExceptionCallableWithTimedCallDistApi() throws Exception {
      DistributedExecutorService des = createDES(getCache());

      DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new ExceptionThrowingCallable(true));
      DistributedTask<Integer> distributedTask = taskBuilder.build();
      Future<Integer> future = des.submit(distributedTask);

      expectException(TimeoutException.class, () -> future.get(10, TimeUnit.MILLISECONDS));
   }

   public void testBasicTargetDistributedCallableWithNullExecutionPolicy() throws Exception {
      Cache<Object, Object> cache1 = getCache();

      //initiate task from cache1 and select cache2 as target
      DistributedExecutorService des = createDES(cache1);

      //the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des.createDistributedTaskBuilder(new SimpleDistributedCallable(false));
      expectException(IllegalArgumentException.class, () -> taskBuilder.executionPolicy(null));
   }

   public void testBasicTargetCallableWithNullTarget() {
      Cache<Object, Object> cache1 = getCache();

      DistributedExecutorService des = createDES(cache1);
      expectException(NullPointerException.class, () -> des.submit((Address) null, new SimpleCallable()));
   }

   public void testBasicTargetCallableWithIllegalTarget() throws InterruptedException, ExecutionException {
      Cache<Object, Object> cache1 = getCache();

      DistributedExecutorService des = createDES(cache1);
      Address fakeAddress = new TestAddress(0, "fake");
      Future<?> future = des.submit(fakeAddress, new SimpleCallable());
      try {
         future.get();
         fail("Test should have thrown an execution exception!");
      } catch (ExecutionException e) {
         Throwable t = e.getCause();
         if (!(t instanceof SuspectException)) {
            throw e;
         }
      }
   }

   public void testBasicDistributedCallableWitkKeys() throws Exception {
      Cache<Object, Object> c1 = getCache();
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DistributedExecutorService des = createDES(getCache());

      Future<Boolean> future = des.submit(new SimpleDistributedCallable(true), "key1", "key2");
      Boolean r = future.get();
      assert r;

      // the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des
               .createDistributedTaskBuilder(new SimpleDistributedCallable(true));
      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      future = des.submit(distributedTask, "key1", "key2");
      r = future.get();
      assert r;
   }

   public void testBasicDistributedCallableWithNullTask() throws Exception {
      Cache<Object, Object> c1 = getCache();
      DistributedExecutorService des = createDES(getCache());

      DistributedTask task = null;
      expectException(NullPointerException.class, () -> des.submit(task, "key1", "key2"));
   }

   public void testBasicDistributedCallableWithNullKeys() throws Exception {
      Cache<Object, Object> c1 = getCache();
      c1.put("key1", "value1");
      c1.put("key2", "value2");
      c1.put("key3", "value3");
      c1.put("key4", "value4");

      DistributedExecutorService des = createDES(getCache());

      des.submit(new SimpleDistributedCallable(false));
   }

   public void testDistributedCallableEverywhereWithKeys() throws Exception {
      Cache<Object, Object> c1 = getCache();
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DistributedExecutorService des = createDES(getCache());

      List<CompletableFuture<Boolean>> list = des.submitEverywhere(new SimpleDistributedCallable(true),
                                                                   "key1", "key2");
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }

      // the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des
               .createDistributedTaskBuilder(new SimpleDistributedCallable(true));
      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      list = des.submitEverywhere(distributedTask, "key1", "key2");
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }
   }

   public void testDistributedCallableEverywhereWithEmptyKeys() throws Exception {
      Cache<Object, Object> c1 = getCache();
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DistributedExecutorService des = createDES(getCache());

      List<CompletableFuture<Boolean>> list = des.submitEverywhere(new SimpleDistributedCallable(false),
                                                        new String[]{});
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }

      //the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des.createDistributedTaskBuilder(new SimpleDistributedCallable(false));
      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      list = des.submitEverywhere(distributedTask, new String[]{});
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }
   }

   public void testBasicDistributedCallableEverywhereWithKeysAndNullTask() throws Exception {
      DistributedExecutorService des = createDES(getCache());

      DistributedTask task = null;
      expectException(NullPointerException.class, () -> des.submitEverywhere(task, "key1", "key2"));
   }

   public void testBasicDistributedCallableEverywhereWithNullTask() throws Exception {
      DistributedExecutorService des = createDES(getCache());

      DistributedTask task = null;
      expectException(NullPointerException.class, () -> des.submitEverywhere(task));
   }

   public void testDistributedCallableEverywhere() throws Exception {

      DistributedExecutorService des = createDES(getCache());

      List<CompletableFuture<Boolean>> list = des.submitEverywhere(new SimpleDistributedCallable(false));
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }

      // the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des
               .createDistributedTaskBuilder(new SimpleDistributedCallable(false));
      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      list = des.submitEverywhere(distributedTask);
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }
   }

   static class SimpleDistributedCallable implements DistributedCallable<String, String, Boolean>,
            Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = 623845442163221832L;
      private boolean invokedProperly = false;
      private final boolean hasKeys;

      public SimpleDistributedCallable(boolean hasKeys) {
         this.hasKeys = hasKeys;
      }

      @Override
      public Boolean call() throws Exception {
         return invokedProperly;
      }

      @Override
      public void setEnvironment(Cache<String, String> cache, Set<String> inputKeys) {
         boolean keysProperlySet = hasKeys ? inputKeys != null && !inputKeys.isEmpty()
                  : inputKeys != null && inputKeys.isEmpty();
         invokedProperly = cache != null && keysProperlySet;
      }

      public boolean validlyInvoked() {
         return invokedProperly;
      }
   }

   static class SimpleCallable implements Callable<Integer>, Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;

      @Override
      public Integer call() throws Exception {
         return 1;
      }
   }

   static class SleepingSimpleCallable implements Callable<Integer>, Serializable, ExternalPojo {
      private static final Log log = LogFactory.getLog(SleepingSimpleCallable.class);

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;

      @Override
      public Integer call() throws Exception {
         log.tracef("Sleeping for 2 seconds");
         Thread.sleep(2000);

         return 1;
      }
   }

   static class SimpleCallableWithField implements Callable<Integer>, Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = -6262148927734766558L;
      private int count;

      @Override
      public Integer call() throws Exception {
         return count++;
      }
   }

   static class ExceptionThrowingCallable implements Callable<Integer>, Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8682463816319507893L;
      private boolean needToSleep;

      public ExceptionThrowingCallable() {
         this.needToSleep = false;
      }

      public ExceptionThrowingCallable(boolean needToSleep) {
         this.needToSleep = needToSleep;
      }

      @Override
      public Integer call() throws Exception {
         if(needToSleep) {
            Thread.sleep(10000);
         }

         throw new TestException("Intentional Exception from ExceptionThrowingCallable");
      }
   }

   static class BoringRunnable implements Runnable, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = 6898519516955822402L;
      private String uuid;

      public BoringRunnable() {
      }

      public BoringRunnable(final String uuid) {
         this.uuid = uuid;
      }

      @Override
      public void run() {
         if(uuid != null) {
            AtomicInteger counter = counterMap.computeIfAbsent(uuid, k -> new AtomicInteger());
            counter.incrementAndGet();
         }
      }
   }
}
