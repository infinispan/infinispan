package org.infinispan.notifications.cachemanagerlistener;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "notifications.cachemanagerlistener.CacheManagerNotifierTest")
public class CacheManagerNotifierTest extends AbstractInfinispanTest {
   private EmbeddedCacheManager cm1;
   private EmbeddedCacheManager cm2;

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm1, cm2);
   }

   public static class CacheManagerNotifierWrapper implements CacheManagerNotifier {

      final CacheManagerNotifier realOne;

      volatile boolean notifyView;

      volatile Address address;

      public CacheManagerNotifierWrapper(CacheManagerNotifier realOne) {
         this.realOne = realOne;
      }

      @Override
      public void notifyViewChange(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId) {
         realOne.notifyViewChange(members, oldMembers, myAddress, viewId);
         notifyView = true;
         this.address = myAddress;
      }

      @Override
      public void notifyCacheStarted(String cacheName) {
         realOne.notifyCacheStarted(cacheName);
      }

      @Override
      public void notifyCacheStopped(String cacheName) {
         realOne.notifyCacheStopped(cacheName);
      }

      @Override
      public void notifyMerge(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId, List<List<Address>> subgroupsMerged) {
         realOne.notifyMerge(members, oldMembers, myAddress, viewId, subgroupsMerged);
      }

      @Override
      public void addListener(Object listener) {
         realOne.addListener(listener);
      }

      @Override
      public void removeListener(Object listener) {
         realOne.removeListener(listener);
      }

      @Override
      public Set<Object> getListeners() {
         return realOne.getListeners();
      }
   }

   public void testViewChange() throws Exception {
      EmbeddedCacheManager cmA = TestCacheManagerFactory.createClusteredCacheManager();
      cmA.getCache();
      CountDownLatch barrier = new CountDownLatch(1);
      GetCacheManagerCheckListener listener = new GetCacheManagerCheckListener(barrier);
      cmA.addListener(listener);
      CacheContainer cmB = TestCacheManagerFactory.createClusteredCacheManager();
      cmB.getCache();
      try {
         barrier.await();
         assert listener.cacheContainer != null;
      } finally {
         TestingUtil.killCacheManagers(cmA, cmB);
      }
   }

   @Listener
   static public class GetCacheManagerCheckListener {
      CacheContainer cacheContainer;
      CountDownLatch barrier;

      public GetCacheManagerCheckListener(CountDownLatch barrier) {
         this.barrier = barrier;
      }

      @ViewChanged
      public void onViewChange(ViewChangedEvent e) throws Exception {
         cacheContainer = e.getCacheManager();
         barrier.countDown();
      }
   }

}
