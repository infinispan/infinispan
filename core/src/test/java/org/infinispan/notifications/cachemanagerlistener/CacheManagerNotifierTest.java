package org.infinispan.notifications.cachemanagerlistener;

import static org.easymock.EasyMock.*;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.xa.TransactionTable.StaleTransactionCleanup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CyclicBarrier;

@Test(groups = "unit", testName = "notifications.cachemanagerlistener.CacheManagerNotifierTest")
public class CacheManagerNotifierTest extends AbstractInfinispanTest {
   EmbeddedCacheManager cm1;
   EmbeddedCacheManager cm2;

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm1, cm2);
   }

   public void testMockViewChange() {
      cm1 = TestCacheManagerFactory.createClusteredCacheManager();
      cm2 = TestCacheManagerFactory.createClusteredCacheManager();
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      c.setFetchInMemoryState(false);
      cm1.defineConfiguration("cache", c);
      cm2.defineConfiguration("cache", c);

      cm1.getCache("cache");

      // this will mean only 1 cache in the cluster so far
      assert cm1.getMembers().size() == 1;
      Address myAddress = cm1.getAddress();
      assert cm1.getMembers().contains(myAddress);

      // now attach a mock notifier
      CacheManagerNotifier mockNotifier = createMock(CacheManagerNotifier.class);
      CacheManagerNotifier origNotifier = TestingUtil.replaceComponent(cm1, CacheManagerNotifier.class, mockNotifier, true);
      try {
         mockNotifier.notifyViewChange(isA(List.class), isA(List.class), eq(myAddress), anyInt(), anyBoolean());
         replay(mockNotifier);
         // start a second cache.
         Cache c2 = cm2.getCache("cache");
         TestingUtil.blockUntilViewsReceived(60000, cm1, cm2);
         verify(mockNotifier);

      } finally {
         TestingUtil.replaceComponent(cm1, CacheManagerNotifier.class, origNotifier, true);
      }
   }

   public void testMockCacheStartedAndStopped() {
      cm1 = TestCacheManagerFactory.createLocalCacheManager();
      cm1.getCache();
      CacheManagerNotifier mockNotifier = createMock(CacheManagerNotifier.class);
      CacheManagerNotifier origNotifier = TestingUtil.replaceComponent(cm1, CacheManagerNotifier.class, mockNotifier, true);
      try {
         cm1.defineConfiguration("testCache", new Configuration());
         mockNotifier.notifyCacheStarted("testCache");
         mockNotifier.addListener(isA(StaleTransactionCleanup.class));
         replay(mockNotifier);
         // start a second cache.
         Cache testCache = cm1.getCache("testCache");
         verify(mockNotifier);

         reset(mockNotifier);
         mockNotifier.removeListener(isA(StaleTransactionCleanup.class));
         mockNotifier.notifyCacheStopped("testCache");
         replay(mockNotifier);
         testCache.stop();
         verify(mockNotifier);
      } finally {
         TestingUtil.replaceComponent(cm1, CacheManagerNotifier.class, origNotifier, true);
      }
   }

   public void testViewChange() throws Exception {
      EmbeddedCacheManager cmA = TestCacheManagerFactory.createClusteredCacheManager();
      cmA.getCache();
      CyclicBarrier barrier = new CyclicBarrier(2);
      GetCacheManagerCheckListener listener = new GetCacheManagerCheckListener(barrier);
      cmA.addListener(listener);
      CacheContainer cmB = TestCacheManagerFactory.createClusteredCacheManager();
      cmB.getCache();
      try {
         barrier.await();
         barrier.await();
         assert listener.cacheContainer != null;
      } finally {
         TestingUtil.killCacheManagers(cmA, cmB);
      }
   }

   @Listener
   public class GetCacheManagerCheckListener {
      CacheContainer cacheContainer;
      CyclicBarrier barrier;
      
      public GetCacheManagerCheckListener(CyclicBarrier barrier) {
         this.barrier = barrier;
      }

      @ViewChanged
      public void onViewChange(ViewChangedEvent e) throws Exception {
         barrier.await();
         cacheContainer = e.getCacheManager();
         barrier.await();
      }

   }

}
