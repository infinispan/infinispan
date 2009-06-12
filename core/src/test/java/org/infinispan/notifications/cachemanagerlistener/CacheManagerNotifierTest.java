package org.infinispan.notifications.cachemanagerlistener;

import static org.easymock.EasyMock.*;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;

@Test(groups = "unit", testName = "notifications.cachemanagerlistener.CacheManagerNotifierTest")
public class CacheManagerNotifierTest {
   CacheManager cm1;
   CacheManager cm2;

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm1, cm2);
   }

   public void testViewChange() {
      cm1 = TestCacheManagerFactory.createClusteredCacheManager();
      cm2 = TestCacheManagerFactory.createClusteredCacheManager();
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      c.setFetchInMemoryState(false);
      cm1.defineCache("cache", c);
      cm2.defineCache("cache", c);

      cm1.getCache("cache");

      // this will mean only 1 cache in the cluster so far
      assert cm1.getMembers().size() == 1;
      Address myAddress = cm1.getAddress();
      assert cm1.getMembers().contains(myAddress);

      // now attach a mock notifier
      CacheManagerNotifier mockNotifier = createMock(CacheManagerNotifier.class);
      CacheManagerNotifier origNotifier = TestingUtil.replaceComponent(cm1, CacheManagerNotifier.class, mockNotifier, true);
      try {
         mockNotifier.notifyViewChange(isA(List.class), eq(myAddress), anyInt());
         replay(mockNotifier);
         // start a second cache.
         Cache c2 = cm2.getCache("cache");
         TestingUtil.blockUntilViewsReceived(60000, cm1, cm2);
         verify(mockNotifier);

      } finally {
         TestingUtil.replaceComponent(cm1, CacheManagerNotifier.class, origNotifier, true);
      }
   }

   public void testCacheStartedAndStopped() {
      cm1 = TestCacheManagerFactory.createLocalCacheManager();
      cm1.getCache();
      CacheManagerNotifier mockNotifier = createMock(CacheManagerNotifier.class);
      CacheManagerNotifier origNotifier = TestingUtil.replaceComponent(cm1, CacheManagerNotifier.class, mockNotifier, true);
      try {
         cm1.defineCache("testCache", new Configuration());
         mockNotifier.notifyCacheStarted("testCache");
         replay(mockNotifier);
         // start a second cache.
         Cache testCache = cm1.getCache("testCache");
         verify(mockNotifier);

         reset(mockNotifier);
         mockNotifier.notifyCacheStopped("testCache");
         replay(mockNotifier);
         testCache.stop();
         verify(mockNotifier);
      } finally {
         TestingUtil.replaceComponent(cm1, CacheManagerNotifier.class, origNotifier, true);
      }
   }
}
