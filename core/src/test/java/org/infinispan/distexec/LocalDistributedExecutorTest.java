package org.infinispan.distexec;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test for verifying that the DistributedExecutors also work on the Local Cache.
 * 
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.LocalDistributedExecutorTest")
public class LocalDistributedExecutorTest extends MultipleCacheManagersTest {
   
   private DistributedExecutorService cleanupService;
   private static final Map<String, AtomicInteger> counterMap = new ConcurrentHashMap<String, AtomicInteger>();

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
   
   @AfterMethod
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
    * 
    * @param call
    * @throws Exception
    */
   @Test(enabled = false)
   public void basicInvocation(Callable<Integer> call) throws Exception {
      DistributedExecutorService des = createDES(getCache());
      Future<Integer> future = des.submit(call);
      Integer r = future.get();
      assert r == 1;
   }
   
   protected DistributedExecutorService createDES(Cache<?,?> cache){
      DistributedExecutorService des = new DefaultExecutorService(cache);
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
      assert exceptionCount == 1;

      List<Future<Integer>> list = des.submitEverywhere(new ExceptionThrowingCallable());
      exceptionCount = 0;
      for (Future<Integer> f : list) {
         try {
            f.get();
            throw new IllegalStateException("Should not have reached this code");
         } catch (ExecutionException ex) {
            exceptionCount++;
         }
      }
      assert exceptionCount == list.size();
   }

   public void testRunnableInvocation() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      Future<?> future = des.submit(new BoringRunnable());
      Object object = future.get();
      assert object == null;
   }

   public void testRunnableInvocationWith2Params() throws Exception {

      DistributedExecutorService des = createDES(getCache());

      Integer result = 5;
      Future<Integer> future = des.submit(new BoringRunnable(), result);
      Integer object = future.get();
      assert object == result;
   }

   public void testRunnableExecution() throws InterruptedException {
      final String uuid = UUID.randomUUID().toString();

      try {
         DistributedExecutorService des = createDES(getCache());
         BoringRunnable runnable = new BoringRunnable(uuid);

         des.execute(runnable);

         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return counterMap.get(uuid) != null && counterMap.get(uuid).get() >= 1;
            }
         });
      } finally {
         counterMap.remove(uuid);
      }
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testNonSerializableRunnableExecution() {
      DistributedExecutorService des = createDES(getCache());
      des.execute(new Runnable() {
         @Override
         public void run() {
            log.trace("Non Serializable Runnable");
         }
      });
   }

   @Test(expectedExceptions = RejectedExecutionException.class)
   public void testRunnableExecutionOnTerminatedExecutor() {
      DistributedExecutorService des = createDES(getCache());
      des.shutdown();

      BoringRunnable runnable = new BoringRunnable();

      des.execute(runnable);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testNullRunnableExecution() {
      DistributedExecutorService des = createDES(getCache());

      BoringRunnable runnable = null;
      des.execute(runnable);
   }

   public void testInvokeAny() throws Exception {

      DistributedExecutorService des = createDES(getCache());
      List<SimpleCallable> tasks = new ArrayList<SimpleCallable>();
      tasks.add(new SimpleCallable());
      Integer result = des.invokeAny(tasks);
      assert result == 1;

      tasks = new ArrayList<SimpleCallable>();
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());
      result = des.invokeAny(tasks);
      assert result == 1;
   }

   public void testInvokeAnyWithTimeout() throws Exception {

      DistributedExecutorService des = createDES(getCache());

      List<SimpleCallable> tasks = new ArrayList<SimpleCallable>();
      tasks.add(new SimpleCallable());
      Integer result = des.invokeAny(tasks, 1000, TimeUnit.MILLISECONDS);

      assert result == 1;

      tasks = new ArrayList<SimpleCallable>();
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());

      result = des.invokeAny(tasks, 1000, TimeUnit.MILLISECONDS);
      assert result == 1;
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testInvokeAnyNoTask() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      des.invokeAny(null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInvokeAnyEmptyTasks() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      des.invokeAny(new ArrayList<SimpleCallable>());
   }

   @Test(expectedExceptions = ExecutionException.class)
   public void testInvokeAnyExceptionTasks() throws Exception {

      DistributedExecutorService des = createDES(getCache());

      List tasks = new ArrayList();
      tasks.add(new ExceptionThrowingCallable());
      tasks.add(new ExceptionThrowingCallable());
      des.invokeAny(tasks);
   }

   public void testInvokeAnySleepingTasks() throws Exception {

      DistributedExecutorService des = createDES(getCache());

      List tasks = new ArrayList();
      tasks.add(new ExceptionThrowingCallable());
      tasks.add(new SleepingSimpleCallable());
      Object result = des.invokeAny(tasks);
      assert ((Integer) result) == 1;
   }
   
   @Test(expectedExceptions = TimeoutException.class)
   public void testInvokeAnyTimedSleepingTasks() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      List<SleepingSimpleCallable> tasks = new ArrayList<SleepingSimpleCallable>();
      tasks.add(new SleepingSimpleCallable());
      des.invokeAny(tasks, 1000, TimeUnit.MILLISECONDS);
   }

   public void testInvokeAll() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      List<SimpleCallable> tasks = new ArrayList<SimpleCallable>();
      tasks.add(new SimpleCallable());
      List<Future<Integer>> list = des.invokeAll(tasks);
      assert list.size() == 1;
      Future<Integer> future = list.get(0);
      assert future.get() == 1;

      tasks = new ArrayList<SimpleCallable>();
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());
      tasks.add(new SimpleCallable());

      list = des.invokeAll(tasks);
      assert list.size() == 3;
      for (Future<Integer> f : list) {
         assert f.get() == 1;
      }
   }

   /**
    * Tests Callable isolation as it gets invoked across the cluster
    * https://issues.jboss.org/browse/ISPN-1041
    * 
    * @throws Exception
    */
   public void testCallableIsolation() throws Exception {
      DistributedExecutorService des = createDES(getCache());

      List<Future<Integer>> list = des.submitEverywhere(new SimpleCallableWithField());
      assert list != null && !list.isEmpty();
      for (Future<Integer> f : list) {
         assert f.get() == 0;
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
      Integer r = future.get(20, TimeUnit.SECONDS);
      assert r == 1;

      //the same using DistributedTask API
      DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new SleepingSimpleCallable());
      DistributedTask<Integer> distributedTask = taskBuilder.build();
      future = des.submit(distributedTask);
      r = future.get(20, TimeUnit.SECONDS);
      assert r == 1;
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testSleepingCallableWithTimeoutExc() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      Future<Integer> future = des.submit(new SleepingSimpleCallable());     
      future.get(2000, TimeUnit.MILLISECONDS);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testSleepingCallableWithTimeoutExcDistApi() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new SleepingSimpleCallable());
      DistributedTask<Integer> distributedTask = taskBuilder.build();
      Future<Integer> future = des.submit(distributedTask);
      future.get(2000, TimeUnit.MILLISECONDS);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testExceptionCallableWithTimedCall() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      Future<Integer> future = des.submit(new ExceptionThrowingCallable(true));

      future.get(10, TimeUnit.MILLISECONDS);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testExceptionCallableWithTimedCallDistApi() throws Exception {
      DistributedExecutorService des = createDES(getCache());

      DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new ExceptionThrowingCallable(true));
      DistributedTask<Integer> distributedTask = taskBuilder.build();
      Future<Integer> future = des.submit(distributedTask);

      future.get(10, TimeUnit.MILLISECONDS);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testBasicTargetDistributedCallableWithNullExecutionPolicy() throws Exception {
      Cache<Object, Object> cache1 = getCache();

      //initiate task from cache1 and select cache2 as target
      DistributedExecutorService des = createDES(cache1);

      //the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des.createDistributedTaskBuilder(new SimpleDistributedCallable(false));
      taskBuilder.executionPolicy(null);

      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      Future<Boolean> future = des.submit(distributedTask);
   }
   
   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicTargetCallableWithNullTarget() {
      Cache<Object, Object> cache1 = getCache();

      DistributedExecutorService des = createDES(cache1);
      des.submit((Address) null, new SimpleCallable());
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testBasicTargetCallableWithIllegalTarget() {
      Cache<Object, Object> cache1 = getCache();

      DistributedExecutorService des = createDES(cache1);
      Address fakeAddress = new Address() {
         @Override
         public int compareTo(Address o) {
            return -1;
         }
      };
      des.submit(fakeAddress, new SimpleCallable());
   }

   public void testBasicDistributedCallableWitkKeys() throws Exception {
      Cache<Object, Object> c1 = getCache();
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DistributedExecutorService des = createDES(getCache());

      Future<Boolean> future = des.submit(new SimpleDistributedCallable(true), new String[] {
               "key1", "key2" });
      Boolean r = future.get();
      assert r;

      // the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des
               .createDistributedTaskBuilder(new SimpleDistributedCallable(true));
      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      future = des.submit(distributedTask, new String[] { "key1", "key2" });
      r = future.get();
      assert r;
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicDistributedCallableWithNullTask() throws Exception {
      Cache<Object, Object> c1 = getCache();
      DistributedExecutorService des = createDES(getCache());

      des.submit((DistributedTask) null, new String[] {"key1", "key2" });
   }

   public void testBasicDistributedCallableWithNullKeys() throws Exception {
      Cache<Object, Object> c1 = getCache();
      c1.put("key1", "value1");
      c1.put("key2", "value2");
      c1.put("key3", "value3");
      c1.put("key4", "value4");

      DistributedExecutorService des = createDES(getCache());

      des.submit(new SimpleDistributedCallable(false), null);
   }

   public void testDistributedCallableEverywhereWithKeys() throws Exception {
      Cache<Object, Object> c1 = getCache();
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      DistributedExecutorService des = createDES(getCache());

      List<Future<Boolean>> list = des.submitEverywhere(new SimpleDistributedCallable(true),
               new String[] { "key1", "key2" });
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }

      // the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des
               .createDistributedTaskBuilder(new SimpleDistributedCallable(true));
      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      list = des.submitEverywhere(distributedTask, new String[] { "key1", "key2" });
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

      List<Future<Boolean>> list = des.submitEverywhere(new SimpleDistributedCallable(false),
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

   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicDistributedCallableEverywhereWithKeysAndNullTask() throws Exception {
      DistributedExecutorService des = createDES(getCache());

      des.submitEverywhere((DistributedTask) null, new String[]{"key1", "key2"});
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicDistributedCallableEverywhereWithNullTask() throws Exception {
      DistributedExecutorService des = createDES(getCache());

      des.submitEverywhere((DistributedTask) null);
   }

   public void testDistributedCallableEverywhere() throws Exception {

      DistributedExecutorService des = createDES(getCache());

      List<Future<Boolean>> list = des.submitEverywhere(new SimpleDistributedCallable(false));
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
            Serializable {

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

   static class SimpleCallable implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;

      @Override
      public Integer call() throws Exception {
         return 1;
      }
   }

   static class SleepingSimpleCallable implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;

      @Override
      public Integer call() throws Exception {
         Thread.sleep(10000);

         return 1;
      }
   }

   static class SimpleCallableWithField implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -6262148927734766558L;
      private int count;

      @Override
      public Integer call() throws Exception {
         return count++;
      }
   }

   static class ExceptionThrowingCallable implements Callable<Integer>, Serializable {

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

         throw new Exception("Intentional Exception from ExceptionThrowingCallable");
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
            AtomicInteger counter = counterMap.get(uuid);
            if(counter == null) {
               counter = new AtomicInteger();
               counterMap.put(uuid, counter);
            }
            counter.incrementAndGet();
         }
      }
   }
}
