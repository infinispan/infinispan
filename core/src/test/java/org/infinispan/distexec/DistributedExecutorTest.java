package org.infinispan.distexec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests org.infinispan.distexec.DistributedExecutorService
 * 
 * @author Vladimir Blagojevic
 * @author Anna Manukyan
 */
@Test(groups = {"functional", "smoke"}, testName = "distexec.DistributedExecutorTest")
public class DistributedExecutorTest extends LocalDistributedExecutorTest {

   public DistributedExecutorTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), false);
      createClusteredCaches(2, cacheName(), builder);
   }

   protected String cacheName() {
      return "DistributedExecutorTest-DIST_SYNC";
   }

   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }

   protected Cache<Object, Object> getCache() {
      return cache(0, cacheName());
   }
   
   @Test(expectedExceptions = ExecutionException.class)
   public void testBasicTargetLocalDistributedCallableWithTimeout() throws Exception {
      Cache<Object, Object> cache1 = getCache();
      Cache<Object, Object> cache2 = cache(1, cacheName());
      CyclicBarrier barrier = new CyclicBarrier(2);
      cache2.getAdvancedCache().getComponentRegistry().registerComponent(barrier, "barrier");

      // initiate task from cache1 and execute on same node
      DistributedExecutorService des = createDES(cache1);
      Address target = address(0);

      DistributedTaskBuilder builder = des.createDistributedTaskBuilder(new SleepingSimpleCallable());
      builder.timeout(1000, TimeUnit.MILLISECONDS);

      Future<Integer> future = des.submit(target, builder.build());
      future.get();
   }

   @Test(expectedExceptions = ExecutionException.class)
   public void testBasicTargetRemoteDistributedCallableWithException() throws Exception {
      Cache<Object, Object> cache1 = cache(0, cacheName());
      Cache<Object, Object> cache2 = cache(1, cacheName());

      // initiate task from cache1 and execute on same node
      DistributedExecutorService des = createDES(cache1);
      Address target = cache2.getAdvancedCache().getRpcManager().getAddress();

      DistributedTaskBuilder builder = des
            .createDistributedTaskBuilder(new ExceptionThrowingCallable());

      Future<Integer> future = des.submit(target, builder.build());
      future.get();
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testBasicTargetLocalDistributedCallableWithHighFutureAndLowTaskTimeout() throws Exception {
      Cache<Object, Object> cache1 = cache(0, cacheName());

      // initiate task from cache1 and execute on same node
      DistributedExecutorService des = createDES(cache1);
      Address target = cache1.getAdvancedCache().getRpcManager().getAddress();

      DistributedTaskBuilder builder = des
            .createDistributedTaskBuilder(new SleepingSimpleCallable());
      builder.timeout(1000, TimeUnit.MILLISECONDS);

      Future<Integer> future = des.submit(target, builder.build());
      future.get(10000, TimeUnit.MILLISECONDS);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testBasicTargetLocalDistributedCallableWithLowFutureAndHighTaskTimeout() throws Exception {
      Cache<Object, Object> cache1 = cache(0, cacheName());

      // initiate task from cache1 and execute on same node
      DistributedExecutorService des = createDES(cache1);
      Address target = cache1.getAdvancedCache().getRpcManager().getAddress();

      DistributedTaskBuilder builder = des
            .createDistributedTaskBuilder(new SleepingSimpleCallable());
      builder.timeout(10000, TimeUnit.MILLISECONDS);

      Future<Integer> future = des.submit(target, builder.build());
      future.get(1000, TimeUnit.MILLISECONDS);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testBasicTargetRemoteDistributedCallableWithHighFutureAndLowTaskTimeout() throws Exception {
      Cache<Object, Object> cache1 = cache(0, cacheName());
      Cache<Object, Object> cache2 = cache(1, cacheName());

      // initiate task from cache1 and execute on same node
      DistributedExecutorService des = createDES(cache1);
      Address target = cache2.getAdvancedCache().getRpcManager().getAddress();

      DistributedTaskBuilder builder = des
            .createDistributedTaskBuilder(new SleepingSimpleCallable());
      builder.timeout(1000, TimeUnit.MILLISECONDS);

      Future<Integer> future = des.submit(target, builder.build());
      future.get(10000, TimeUnit.MILLISECONDS);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testBasicTargetRemoteDistributedCallableWithLowFutureAndHighTaskTimeout() throws Exception {
      Cache<Object, Object> cache1 = cache(0, cacheName());
      Cache<Object, Object> cache2 = cache(1, cacheName());

      // initiate task from cache1 and execute on same node
      DistributedExecutorService des = createDES(cache1);
      Address target = cache2.getAdvancedCache().getRpcManager().getAddress();

      DistributedTaskBuilder builder = des
            .createDistributedTaskBuilder(new SleepingSimpleCallable());
      builder.timeout(10000, TimeUnit.MILLISECONDS);

      Future<Integer> future = des.submit(target, builder.build());
      future.get(1000, TimeUnit.MILLISECONDS);
   }

   public void testBasicTargetLocalDistributedCallableWithoutSpecTimeout() throws Exception {
      Cache<Object, Object> cache1 = cache(0, cacheName());

      // initiate task from cache1 and execute on same node
      DistributedExecutorService des = createDES(cache1);
      Address target = cache1.getAdvancedCache().getRpcManager().getAddress();

      DistributedTaskBuilder builder = des
            .createDistributedTaskBuilder(new SleepingSimpleCallable());

      Future<Integer> future = des.submit(target, builder.build());

      AssertJUnit.assertEquals((Integer) 1, future.get());
   }

   public void testTaskCancellation() throws Exception {
      CyclicBarrier barrier = new CyclicBarrier(2);
      cache(1, cacheName()).getAdvancedCache().getComponentRegistry().registerComponent(barrier, "barrier");

      DistributedExecutorService des = createDES(getCache());
      List<Address> cacheMembers = getCache().getAdvancedCache().getRpcManager().getMembers();
      AssertJUnit.assertEquals(caches(cacheName()).size(), cacheMembers.size());

      DistributedTaskBuilder<Integer> tb = des.createDistributedTaskBuilder(new LongRunningCallable());
      final Future<Integer> future = des.submit(address(1), tb.build());
      // Will unblock when LongRunningCallable starts running
      barrier.await(10, TimeUnit.SECONDS);
      future.cancel(true);
      boolean taskCancelled = false;
      try {
         future.get();
      } catch (Exception e) {
         taskCancelled = e instanceof CancellationException;
      }
      assert taskCancelled : "Dist task not cancelled ";
      // Will unblock when LongRunningCallable is interrupted
      barrier.await(10, TimeUnit.SECONDS);
      assert future.isCancelled();      
      assert future.isDone(); 

      //Testing whether the cancellation already happened.
      boolean isCanceled = future.cancel(true);
      assert !isCanceled;
   }
   
   @Test(expectedExceptions = CancellationException.class)
   public void testCancelAndGet() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      List<Address> cacheMembers = getCache().getAdvancedCache().getRpcManager().getMembers();
      List<Address> members = new ArrayList<Address>(cacheMembers);
      AssertJUnit.assertEquals(caches(cacheName()).size(), members.size());
      members.remove(getCache().getAdvancedCache().getRpcManager().getAddress());
      
      DistributedTaskBuilder<Integer> tb = des.createDistributedTaskBuilder( new LongRunningCallable());
      final Future<Integer> future = des.submit(members.get(0),tb.build());
      
      future.cancel(true);
      future.get();     
   }
   
   @Test(expectedExceptions = TimeoutException.class)
   public void testTimeoutOnLocalNode() throws Exception {
      AdvancedCache<Object, Object> localCache = getCache().getAdvancedCache();      
      DistributedExecutorService des = createDES(localCache);      
      Future<Integer> future = des.submit(localCache.getRpcManager().getAddress(), new SleepingSimpleCallable());     
      future.get(2000, TimeUnit.MILLISECONDS);
   }
   
   public void testBasicTargetDistributedCallableTargetSameNode() throws Exception {
      Cache<Object, Object> cache1 = getCache();

      //initiate task from cache1 and select cache1 as target
      DistributedExecutorService des = createDES(cache1);
      Address target = cache1.getAdvancedCache().getRpcManager().getAddress();
      Future<Boolean> future = des.submit(target, new SimpleDistributedCallable(false));
      Boolean r = future.get();
      assert r;

      //the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des.createDistributedTaskBuilder(new SimpleDistributedCallable(false));
      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      future = des.submit(target, distributedTask);
      r = future.get();
      assert r;
   }

   public void testBasicTargetDistributedCallable() throws Exception {
      Cache<Object, Object> cache1 = cache(0, cacheName());
      Cache<Object, Object> cache2 = cache(1, cacheName());

      // initiate task from cache1 and select cache2 as target
      DistributedExecutorService des = createDES(cache1);
      Address target = cache2.getAdvancedCache().getRpcManager().getAddress();
      Future<Boolean> future = des.submit(target, new SimpleDistributedCallable(false));
      Boolean r = future.get();
      assert r;

      // the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des
               .createDistributedTaskBuilder(new SimpleDistributedCallable(false));
      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      future = des.submit(target, distributedTask);
      r = future.get();
      assert r;
   }

   @Test(expectedExceptions = ExecutionException.class)
   public void testBasicTargetDistributedCallableWithTimeout() throws Exception {
      Cache<Object, Object> cache1 = getCache();

      // initiate task from cache1 and select cache2 as target
      DistributedExecutorService des = createDES(cache1);
      Address target = cache1.getAdvancedCache().getRpcManager().getAddress();

      DistributedTaskBuilder<Integer> builder = des.createDistributedTaskBuilder(new SleepingSimpleCallable());
      builder.timeout(10, TimeUnit.MILLISECONDS);

      Future<Integer> future = des.submit(target, builder.build());
      future.get();
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testBasicTargetCallableWithNullTask() {
      Cache<Object, Object> cache1 = getCache();   

      DistributedExecutorService des = createDES(cache1);
      Address target = cache1.getAdvancedCache().getRpcManager().getAddress();
      des.submit(target, (Callable) null);
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testBasicTargetDistributedTaskWithNullTask() {
      Cache<Object, Object> cache1 = getCache();   

      DistributedExecutorService des = createDES(cache1);
      Address target = cache1.getAdvancedCache().getRpcManager().getAddress();
      des.submit(target, (DistributedTask) null);
   }

   public void testDistributedCallableEverywhereWithKeysOnBothNodes() throws Exception {
      Cache<Object, Object> c1 = getCache();
      c1.put("key1", "Manik");
      c1.put("key2", "Mircea");
      c1.put("key3", "Galder");
      c1.put("key4", "Sanne");

      Cache<Object, Object> c2 = cache(1, cacheName());
      c2.put("key5", "test");
      c2.put("key6", "test1");

      DistributedExecutorService des = createDES(getCache());

      List<Future<Boolean>> list = des.submitEverywhere(new SimpleDistributedCallable(true),
                                                        new String[] { "key1", "key2", "key5", "key6" });
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }

      //the same using DistributedTask API
      DistributedTaskBuilder<Boolean> taskBuilder = des.createDistributedTaskBuilder(new SimpleDistributedCallable(true));
      DistributedTask<Boolean> distributedTask = taskBuilder.build();
      list = des.submitEverywhere(distributedTask,new String[] {"key1", "key2" });
      assert list != null && !list.isEmpty();
      for (Future<Boolean> f : list) {
         assert f.get();
      }
   }
   
   static class LongRunningCallable implements DistributedCallable<Object, Object, Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -6110011263261397071L;
      private Cache<Object, Object> cache;

      @Override
      public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
         this.cache = cache;
      }

      @Override
      public Integer call() throws Exception {
         CyclicBarrier barrier = cache.getAdvancedCache().getComponentRegistry().getComponent("barrier");

         barrier.await(10, TimeUnit.SECONDS);
         try {
            TimeUnit.MILLISECONDS.sleep(5000);
         } catch (InterruptedException e) {
            //interrupted successfully
            barrier.await(10, TimeUnit.SECONDS);
            Thread.currentThread().interrupt();
         }
         return 1;
      }
   }
}