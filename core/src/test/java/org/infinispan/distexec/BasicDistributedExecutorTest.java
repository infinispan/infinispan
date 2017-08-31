package org.infinispan.distexec;

import static org.infinispan.test.Exceptions.expectException;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestClassLocal;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests basic org.infinispan.distexec.DistributedExecutorService functionality
 *
 * @author Vladimir Blagojevic
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.BasicDistributedExecutorTest")
public class BasicDistributedExecutorTest extends AbstractCacheTest {
   private TestClassLocal<ReclosableLatch> latchHolder =
         new TestClassLocal<>("latch", this, ReclosableLatch::new, ReclosableLatch::open);

   public BasicDistributedExecutorTest() {
   }

   public void testImproperMasterCacheForDistributedExecutor() {
      expectException(IllegalArgumentException.class, () -> new DefaultExecutorService(null));
   }

   public void testImproperLocalExecutorServiceForDistributedExecutor() {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(false);
      try {
         Cache<Object, Object> cache = cacheManager.getCache();
         expectException(IllegalArgumentException.class, () -> new DefaultExecutorService(cache, null));
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testStoppedLocalExecutorServiceForDistributedExecutor() throws ExecutionException, InterruptedException {
      ExecutorService service = new WithinThreadExecutor();
      service.shutdown();
      expectException(IllegalArgumentException.class, () -> customExecutorServiceDistributedExecutorTest(service, false));
   }

   public void testDistributedExecutorWithPassedThreadExecutorOwnership() throws ExecutionException, InterruptedException {
      ExecutorService service = new WithinThreadExecutor();
      customExecutorServiceDistributedExecutorTest(service, true);
   }

   public void testDistributedExecutorWithPassedThreadExecutor() throws ExecutionException, InterruptedException {
      ExecutorService service = new WithinThreadExecutor();
      customExecutorServiceDistributedExecutorTest(service, false);
   }

   public void testDistributedExecutorWithManagedExecutorService() throws ExecutionException, InterruptedException {
      ExecutorService service = new ManagedExecutorServicesEmulator();
      customExecutorServiceDistributedExecutorTest(service, false);
   }

   public void testDistributedExecutorWithManagedExecutorServiceOwnership() throws ExecutionException, InterruptedException {
      ExecutorService service = new ManagedExecutorServicesEmulator();
      expectException(IllegalArgumentException.class, () -> customExecutorServiceDistributedExecutorTest(service, true));
   }

   private void customExecutorServiceDistributedExecutorTest(ExecutorService service, boolean overrideOwnership) throws ExecutionException, InterruptedException {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      try {
         Cache<Object, Object> cache = cacheManager.getCache();

         DistributedExecutorService des;
         if (overrideOwnership)
            des = new DefaultExecutorService(cache, service, true);
         else
            des = new DefaultExecutorService(cache, service);

         Future<Integer> future = des.submit(new SimpleCallable());
         Integer r = future.get();
         assert r == 1;
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testStoppedCacheForDistributedExecutor() {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      try {
         Cache<Object, Object> cache = cacheManager.getCache();
         cache.stop();
         expectException(IllegalStateException.class, () -> new DefaultExecutorService(cache));
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testDistributedExecutorShutDown() {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      try {
         Cache<Object, Object> cache = cacheManager.getCache();
         DistributedExecutorService des = new DefaultExecutorService(cache);
         des.shutdown();
         assert des.isShutdown();
         assert des.isTerminated();
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testDistributedExecutorRealShutdown() {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      ExecutorService service = new WithinThreadExecutor();
      try {
         Cache<Object, Object> cache = cacheManager.getCache();

         DistributedExecutorService des = new DefaultExecutorService(cache, service);

         des.shutdown();

         assert des.isShutdown();
         assert des.isTerminated();
         assert !service.isShutdown();
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
         service.shutdown();
      }
   }

   public void testDistributedExecutorRealShutdownWithOwnership() {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      ExecutorService service = new WithinThreadExecutor();
      try {
         Cache<Object, Object> cache = cacheManager.getCache();

         DistributedExecutorService des = new DefaultExecutorService(cache, service, true);

         des.shutdown();

         assert des.isShutdown();
         assert des.isTerminated();
         assert service.isShutdown();
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testDistributedExecutorShutDownNow() {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      try {
         Cache<Object, Object> cache = cacheManager.getCache();
         DistributedExecutorService des = new DefaultExecutorService(cache);

         assert !des.isShutdown();
         assert !des.isTerminated();

         des.shutdownNow();

         assert des.isShutdown();
         assert des.isTerminated();
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   /**
    * Tests that we can invoke DistributedExecutorService on an Infinispan cluster having a single node
    */
   public void testSingleCacheExecution() throws Exception {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      DistributedExecutorService des = null;
      try {
         Cache<Object, Object> cache = cacheManager.getCache();
         des = new DefaultExecutorService(cache);
         Future<Integer> future = des.submit(new SimpleCallable());
         Integer r = future.get();
         assert r == 1;

         List<CompletableFuture<Integer>> list = des.submitEverywhere(new SimpleCallable());
         AssertJUnit.assertEquals(1, list.size());
         for (Future<Integer> f : list) {
            AssertJUnit.assertEquals(new Integer(1), f.get());
         }
      } finally {
         if (des != null) des.shutdownNow();
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   /**
    * Tests that we can invoke DistributedExecutorService task with keys
    * https://issues.jboss.org/browse/ISPN-1886
    */
   public void testSingleCacheWithKeysExecution() throws Exception {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      DistributedExecutorService des = null;
      try {
         Cache<Object, Object> c1 = cacheManager.getCache();
         c1.put("key1", "Manik");
         c1.put("key2", "Mircea");
         c1.put("key3", "Galder");
         c1.put("key4", "Sanne");

         des = new DefaultExecutorService(c1);

         Future<Boolean> future = des.submit(new SimpleDistributedCallable(true), "key1", "key2");
         Boolean r = future.get();
         assert r;
      } finally {
         if (des != null) des.shutdownNow();
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testDistributedCallableCustomFailoverPolicySuccessfullRetry() throws Exception {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      DistributedExecutorService des = null;
      try {
         Cache<Object, Object> cache1 = cacheManager.getCache();
         cache1.put("key1", "value1");
         cache1.put("key2", "value2");

         //initiate task from cache1 and select cache1 as target
         des = new DefaultExecutorService(cache1);

         //the same using DistributedTask API
         DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new FailOnlyOnceCallable());
         taskBuilder.failoverPolicy(new DistributedTaskFailoverPolicy() {

            @Override
            public Address failover(FailoverContext context) {
               return context.executionFailureLocation();
            }

            @Override
            public int maxFailoverAttempts() {
               return 1;
            }
         });

         DistributedTask<Integer> task = taskBuilder.build();
         AssertJUnit.assertEquals(1, task.getTaskFailoverPolicy().maxFailoverAttempts());
         Future<Integer> val = des.submit(task, "key1");
         AssertJUnit.assertEquals(new Integer(1), val.get());
      } finally {
         if (des != null) des.shutdownNow();
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testDistributedCallableWithFailingKeysSuccessfullRetry() throws Exception {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      config.clustering().hash().numOwners(1);
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager(config);
      DistributedExecutorService des = null;
      cacheManager1.defineConfiguration("cache1", config.build());
      cacheManager2.defineConfiguration("cache1", config.build());
      try {
         Cache<Object, Object> cache1 = cacheManager1.getCache("cache1");
         cache1.put("key1", "value1");
         cache1.put("key2", "value2");
         cache1.put("key3", "value3");

         Cache<Object, Object> cache2 = cacheManager2.getCache("cache1");
         cache2.put("key4", "value4");
         cache2.put("key5", "value5");
         cache2.put("key6", "value6");
         cache2.put("key7", "value7");
         cache2.put("key8", "value8");

         //initiate task from cache1 and select cache1 as target
         des = new DefaultExecutorService(cache1);

         //the same using DistributedTask API
         DistributedTaskBuilder<Boolean> taskBuilder = des.createDistributedTaskBuilder(new FailOnlyOnceDistributedCallable());
         taskBuilder.failoverPolicy(new DistributedTaskFailoverPolicy() {

            @Override
            public Address failover(FailoverContext context) {
               List<Address> candidates = context.executionCandidates();
               Address returnAddress = null;
               for (Address candidate : candidates) {
                  if (!candidate.equals(context.executionFailureLocation())) {
                     returnAddress = candidate;
                     break;
                  }
               }
               return returnAddress;
            }

            @Override
            public int maxFailoverAttempts() {
               return 1;
            }
         });
         DistributedTask<Boolean> task = taskBuilder.build();
         AssertJUnit.assertEquals(1, task.getTaskFailoverPolicy().maxFailoverAttempts());
         Future<Boolean> val = des.submit(task, "key1", "key5");
         AssertJUnit.assertEquals(Boolean.TRUE, val.get());
      } finally {
         if (des != null) des.shutdownNow();
         TestingUtil.killCacheManagers(cacheManager1);
         TestingUtil.killCacheManagers(cacheManager2);
      }
   }

   public void testDistributedCallableEmptyFailoverPolicy() throws Exception {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      DistributedExecutorService des = null;
      try {
         Cache<Object, Object> cache1 = cacheManager.getCache();

         //initiate task from cache1 and select cache1 as target
         des = new DefaultExecutorService(cache1);

         //the same using DistributedTask API
         DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new ExceptionThrowingCallable());
         taskBuilder.failoverPolicy(null);
         DistributedTask<Integer> task = taskBuilder.build();

         assert task.getTaskFailoverPolicy().equals(DefaultExecutorService.NO_FAILOVER);

         Future<Integer> f = des.submit(task);

         expectException(ExecutionException.class, TestException.class, f::get);
      } finally {
         if (des != null)
            des.shutdownNow();

         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testDistributedCallableRandomFailoverPolicy() throws Exception {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      DistributedExecutorService des = null;

      try {
         Cache<Object, Object> cache1 = cacheManager.getCache();
         cache1.put("key1", "value1");
         cache1.put("key2", "value2");

         //initiate task from cache1 and select cache1 as target
         des = new DefaultExecutorService(cache1);

         //the same using DistributedTask API
         DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new FailOnlyOnceCallable());
         taskBuilder.failoverPolicy(DefaultExecutorService.RANDOM_NODE_FAILOVER);

         DistributedTask<Integer> task = taskBuilder.build();

         assert task.getTaskFailoverPolicy().equals(DefaultExecutorService.RANDOM_NODE_FAILOVER);

         Future<Integer> val = des.submit(task, "key1");

         val.get();
         throw new IllegalStateException("Should have raised exception");
      } catch (ExecutionException e){
         // The failover policy throws an IllegalStateException because there are no nodes to retry on.
         // Verify that the distributed executor didn't wrap the exception in too many extra exceptions.
         AssertJUnit.assertTrue("Wrong exception: " + e, e.getCause() instanceof IllegalStateException);
      }
      finally {
         if (des != null) des.shutdownNow();
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testDistributedCallableRandomFailoverPolicyWith2Nodes() throws Exception {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      DistributedExecutorService des = null;
      try {
         Cache<Object, Object> cache1 = cacheManager.getCache();
         cache1.put("key1", "value1");
         cache1.put("key2", "value2");

         Cache<Object, Object> cache2 = cacheManager1.getCache();
         cache2.put("key3", "value3");

         //initiate task from cache1 and select cache1 as target
         des = new DefaultExecutorService(cache1);

         //the same using DistributedTask API
         DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new ExceptionThrowingCallable());
         taskBuilder.failoverPolicy(DefaultExecutorService.RANDOM_NODE_FAILOVER);

         DistributedTask<Integer> task = taskBuilder.build();

         assert task.getTaskFailoverPolicy().equals(DefaultExecutorService.RANDOM_NODE_FAILOVER);

         Future<Integer> val = des.submit(task, "key1");
         val.get();
         throw new IllegalStateException("Should have thrown exception");
      }  catch (Exception e){
         assert e instanceof ExecutionException;
         ExecutionException ee = (ExecutionException)e;
         boolean duplicateEEInChain = ee.getCause() instanceof ExecutionException;
         AssertJUnit.assertEquals(false, duplicateEEInChain);
      }
      finally {
         if (des != null)
            des.shutdownNow();

         TestingUtil.killCacheManagers(cacheManager, cacheManager1);
      }
   }

   public void testBasicTargetLocalDistributedCallableWithoutAnyTimeout() throws Exception {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      config.clustering().cacheMode(CacheMode.REPL_SYNC).remoteTimeout(0L);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      DistributedExecutorService des = null;
      try {
         Cache<Object, Object> cache1 = cacheManager.getCache();
         Cache<Object, Object> cache2 = cacheManager1.getCache();

         // initiate task from cache1 and execute on same node
         des = new DefaultExecutorService(cache1);
         Address target = cache1.getAdvancedCache().getRpcManager().getAddress();

         latchHolder.get().close();
         DistributedTaskBuilder<Integer> builder = des
               .createDistributedTaskBuilder(new DistributedExecutorTest.SleepingSimpleCallable(latchHolder));

         Future<Integer> future = des.submit(target, builder.build());

         Thread.sleep(100);
         latchHolder.get().open();
         AssertJUnit.assertEquals((Integer) 1, future.get());
      } finally {
         if (des != null)
            des.shutdownNow();

         TestingUtil.killCacheManagers(cacheManager, cacheManager1);
      }
   }

   public void testBasicTargetRemoteDistributedCallableWithoutAnyTimeout() throws Exception {
      ConfigurationBuilder confBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      confBuilder.clustering().remoteTimeout(0L);

      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(confBuilder);
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(confBuilder);

      Cache<Object, Object> cache1 = cacheManager.getCache();
      Cache<Object, Object> cache2 = cacheManager1.getCache();
      DistributedExecutorService des = null;
      try {
         // initiate task from cache1 and execute on same node
         des = new DefaultExecutorService(cache1);
         Address target = cache2.getAdvancedCache().getRpcManager().getAddress();

         latchHolder.get().close();
         DistributedTaskBuilder<Integer> builder = des
               .createDistributedTaskBuilder(new DistributedExecutorTest.SleepingSimpleCallable(latchHolder));

         Future<Integer> future = des.submit(target, builder.build());

         Thread.sleep(100);
         latchHolder.get().open();

         AssertJUnit.assertEquals((Integer) 1, future.get());
      } finally {
         if (des != null)
            des.shutdown();

         TestingUtil.killCacheManagers(cacheManager, cacheManager1);
      }
   }

   public void testDistributedCallableCustomFailoverPolicy() throws Exception {
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(config);
      DistributedExecutorService des = null;

      try {
         Cache<Object, Object> cache1 = cacheManager.getCache();
         cache1.put("key1", "value1");
         cache1.put("key2", "value2");

         //initiate task from cache1 and select cache1 as target
         des = new DefaultExecutorService(cache1);

         //the same using DistributedTask API
         DistributedTaskBuilder<Integer> taskBuilder = des.createDistributedTaskBuilder(new FailOnlyOnceCallable());

         taskBuilder.failoverPolicy(new DistributedTaskFailoverPolicy() {

            @Override
            public Address failover(FailoverContext context) {
               return context.executionFailureLocation();
            }

            @Override
            public int maxFailoverAttempts() {
               return 0;
            }
         });

         DistributedTask<Integer> task = taskBuilder.build();
         assert task.getTaskFailoverPolicy().maxFailoverAttempts() == 0;

         Future<Integer> val = des.submit(task, "key1");
         val.get();
         throw new IllegalStateException("Should have thrown exception");
      } catch (Exception e) {
         assert e instanceof ExecutionException;
         ExecutionException ee = (ExecutionException) e;
         boolean duplicateEEInChain = ee.getCause() instanceof ExecutionException;
         AssertJUnit.assertEquals(false, duplicateEEInChain);
      }
      finally {
         if (des != null) des.shutdownNow();
         TestingUtil.killCacheManagers(cacheManager);
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

   static class FailOnlyOnceCallable implements Callable<Integer>, Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = 3961940091247573385L;
      boolean throwException = true;

      @Override
      public Integer call() throws Exception {
         if (throwException) {
            // do to not throw the exception 2nd time during retry.
            throwException = false;
            // now throw exception for the first run
            throw new TestException();
         }
         return 1;
      }
   }

   static class FailOnlyOnceDistributedCallable implements DistributedCallable<String, String, Boolean>, Serializable, ExternalPojo {
      /** The serialVersionUID **/
      private static final long serialVersionUID = 5375461422884389555L;
      private static boolean throwException = true;

      @Override
      public void setEnvironment(Cache<String, String> cache, Set<String> inputKeys) {
         //do nothing
      }

      @Override
      public Boolean call() throws Exception {
         if(throwException) {
            throwException = false;

            throw new TestException();
         }

         return true;
      }
   }

   static class ExceptionThrowingCallable implements Callable<Integer>, Serializable, ExternalPojo {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;

      @Override
      public Integer call() throws Exception {
         throw new TestException();
      }
   }
}
