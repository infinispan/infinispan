package org.infinispan.persistence.support;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.ViewChangeListener;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

@Test(groups = "functional", testName = "persistence.decorators.SingletonStoreTest", enabled = false, description = "See ISPN-1123")
public class SingletonStoreTest extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(SingletonStoreTest.class);
   private static final AtomicInteger storeCounter = new AtomicInteger(0);
   private EmbeddedCacheManager cm0, cm1, cm2;

   public SingletonStoreTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() {
      cm0 = addClusterEnabledCacheManager();
      cm1 = addClusterEnabledCacheManager();
      cm2 = addClusterEnabledCacheManager();

      ConfigurationBuilder conf = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      cm0.defineConfiguration("pushing", addDummyStore(conf, true).build());
      cm1.defineConfiguration("pushing", addDummyStore(conf, true).build());
      cm2.defineConfiguration("pushing", addDummyStore(conf, true).build());

      conf = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      // cannot define on ALL cache managers since the same dummy in memory CL bin will be used!
      cm0.defineConfiguration("nonPushing", addDummyStore(conf, false).build());
      cm1.defineConfiguration("nonPushing", addDummyStore(conf, false).build());
      cm2.defineConfiguration("nonPushing", addDummyStore(conf, false).build());
   }

   private ConfigurationBuilder addDummyStore(ConfigurationBuilder config, boolean pushing) {
      config
         .persistence()
            .clearStores()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
               .storeName("Store-" + storeCounter.getAndIncrement())
               .singleton()
                  .enable()
                  .pushStateWhenCoordinator(pushing);
      return config;
   }

   private Cache[] getCaches(String name) {
      return new Cache[]{
            cm0.getCache(name), cm1.getCache(name), cm2.getCache(name)
      };
   }

   private SingletonCacheWriter[] extractStores(Cache[] caches) {
      SingletonCacheWriter[] stores = new SingletonCacheWriter[caches.length];

      int i = 0;
      for (Cache c : caches)
         stores[i++] = (SingletonCacheWriter) TestingUtil.getFirstLoader(c);
      return stores;
   }

   private Object load(SingletonCacheWriter cs, Object key) throws PersistenceException {
      MarshalledEntry se = ((CacheLoader)cs).load(key);
      return se == null ? null : se.getValue();
   }

   public void testPutCacheLoaderWithNoPush() throws Exception {
      Cache[] caches = getCaches("nonPushing");
      for (Cache c : caches) c.start();

      // block until they all see each other!
      TestingUtil.blockUntilViewsReceived(60000, true, caches);

      int i = 1;
      for (Cache c : caches) {
         c.put("key" + i, "value" + i);
         i++;
      }

      // all values should be on all caches since they are sync-repl
      for (Cache c : caches) {
         for (i = 1; i < 4; i++) assert c.get("key" + i).equals("value" + i);
      }

      // now test the stores.  These should *only* be on the store on cache 1.
      SingletonCacheWriter[] stores = extractStores(caches);

      for (i = 1; i < 4; i++) {
         // should ONLY be on the first loader!
         assert load(stores[0], "key" + i).equals("value" + i);
         assert load(stores[1], "key" + i) == null : "stores[1] should not have stored key key" + i;
         assert load(stores[2], "key" + i) == null : "stores[2] should not have stored key key" + i;
      }

      cm0.stop();
      TestingUtil.blockUntilViewsReceived(60000, false, cm1, cm2);

      caches[1].put("key4", "value4");
      caches[2].put("key5", "value5");

      assert load(stores[1], "key4").equals("value4");
      assert load(stores[1], "key5").equals("value5");

      assert load(stores[2], "key4") == null;
      assert load(stores[2], "key5") == null;

      cm1.stop();
      TestingUtil.blockUntilViewsReceived(60000, false, cm2);

      caches[2].put("key6", "value6");
      assert load(stores[2], "key6").equals("value6");
   }

   public void testPutCacheLoaderWithPush() throws Exception {
      Cache[] caches = getCaches("pushing");
      for (Cache c : caches) c.start();
      Map<String, String> expected = new HashMap<String, String>();

      expected.put("a-key", "a-value");
      expected.put("aa-key", "aa-value");
      expected.put("b-key", "b-value");
      expected.put("bb-key", "bb-value");
      expected.put("c-key", "c-value");
      expected.put("d-key", "d-value");
      expected.put("e-key", "e-value");
      expected.put("g-key", "g-value");

      caches[0].putAll(expected);

      SingletonCacheWriter[] stores = extractStores(caches);

      for (String key : expected.keySet()) {
         assert load(stores[0], key).equals(expected.get(key));
         assert load(stores[1], key) == null;
         assert load(stores[2], key) == null;
      }

      ViewChangeListener viewChangeListener = new ViewChangeListener(caches[1]);

      cm0.stop();

      viewChangeListener.waitForViewChange(60, TimeUnit.SECONDS);

      waitForPushStateCompletion(stores[1].pushStateFuture);

      // cache store 1 should have all state now, and store 2 should have nothing

      for (String key : expected.keySet()) {
         assert load(stores[1], key).equals(expected.get(key));
         assert load(stores[2], key) == null;
      }

      caches[1].put("h-key", "h-value");
      caches[2].put("i-key", "i-value");
      expected.put("h-key", "h-value");
      expected.put("i-key", "i-value");

      for (String key : expected.keySet()) {
         assert load(stores[1], key).equals(expected.get(key));
         assert load(stores[2], key) == null;
      }

      viewChangeListener = new ViewChangeListener(caches[2]);
      cm1.stop();
      viewChangeListener.waitForViewChange(60, TimeUnit.SECONDS);

      waitForPushStateCompletion(stores[2].pushStateFuture);

      for (String key : expected.keySet()) assert load(stores[2], key).equals(expected.get(key));

      caches[2].put("aaa-key", "aaa-value");
      expected.put("aaa-key", "aaa-value");

      for (String key : expected.keySet()) assert load(stores[2], key).equals(expected.get(key));
   }

   public void testAvoidConcurrentStatePush() throws Exception {
      final CountDownLatch pushStateCanFinish = new CountDownLatch(1);
      final CountDownLatch secondActiveStatusChangerCanStart = new CountDownLatch(1);
      final TestingSingletonStore mscl = new TestingSingletonStore(pushStateCanFinish, secondActiveStatusChangerCanStart);

      Future f1 = fork(createActiveStatusChanger(mscl));
      assert secondActiveStatusChangerCanStart.await(1000, TimeUnit.MILLISECONDS) : "Failed waiting on latch";

      Future f2 = fork(createActiveStatusChanger(mscl));

      f1.get();
      f2.get();

      assertEquals(1, mscl.getNumberCreatedTasks());
   }

   public void testPushStateTimedOut() throws Throwable {
      final CountDownLatch pushStateCanFinish = new CountDownLatch(1);

      final TestingSingletonStore mscl = new TestingSingletonStore(pushStateCanFinish, null);

      Future f = fork(createActiveStatusChanger(mscl));
      pushStateCanFinish.await(200, TimeUnit.MILLISECONDS);
      pushStateCanFinish.countDown();

      try {
         f.get();
         fail("Should have timed out");
      }
      catch (ExecutionException expected) {
         Throwable e;
         if ((e = expected.getCause().getCause().getCause()) instanceof TimeoutException) {
            assert true : "This is expected";
         } else {
            throw e;
         }
      }
   }

   private void waitForPushStateCompletion(Future pushThreadFuture) throws Exception {
      if (pushThreadFuture != null) pushThreadFuture.get();
   }

   private Callable<?> createActiveStatusChanger(SingletonCacheWriter mscl) {
      return new ActiveStatusModifier(mscl);
   }

   static class TestingSingletonStore extends SingletonCacheWriter {
      private int numberCreatedTasks = 0;
      private CountDownLatch pushStateCanFinish;
      private CountDownLatch secondActiveStatusChangerCanStart;

      public TestingSingletonStore(CountDownLatch pushStateCanFinish, CountDownLatch secondActiveStatusChangerCanStart) {
         super(null, null);
         this.pushStateCanFinish = pushStateCanFinish;
         this.secondActiveStatusChangerCanStart = secondActiveStatusChangerCanStart;
      }

      public int getNumberCreatedTasks() {
         return numberCreatedTasks;
      }

      public void setNumberCreatedTasks(int numberCreatedTasks) {
         this.numberCreatedTasks = numberCreatedTasks;
      }

      @Override
      protected Callable<?> createPushStateTask() {
         return new Callable() {
            @Override
            public Object call() throws Exception {
               numberCreatedTasks++;
               try {
                  if (secondActiveStatusChangerCanStart != null) {
                     secondActiveStatusChangerCanStart.countDown();
                  }
                  pushStateCanFinish.await();
               }
               catch (InterruptedException e) {
                  fail("ActiveStatusModifier interrupted");
               }
               return null;
            }
         };
      }


      @Override
      protected void awaitForPushToFinish(Future future, long timeout, TimeUnit unit) {
         pushStateCanFinish.countDown();
         super.awaitForPushToFinish(future, timeout, unit);
      }
   }

   static class ActiveStatusModifier implements Callable {
      private SingletonCacheWriter scl;

      public ActiveStatusModifier(SingletonCacheWriter singleton) {
         scl = singleton;
      }

      @Override
      public Object call() throws Exception {
         log.debug("active status modifier started");
         scl.activeStatusChanged(true);
         scl.pushStateFuture.get();

         return null;
      }
   }
}
