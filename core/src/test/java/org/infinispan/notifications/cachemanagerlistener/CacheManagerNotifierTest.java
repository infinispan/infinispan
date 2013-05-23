/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.notifications.cachemanagerlistener;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.assertEquals;

@Test(groups = "unit", testName = "notifications.cachemanagerlistener.CacheManagerNotifierTest")
public class CacheManagerNotifierTest extends AbstractInfinispanTest {
   private EmbeddedCacheManager cm1;
   private EmbeddedCacheManager cm2;

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm1, cm2);
   }

   public void testMockViewChange() {
      cm1 = TestCacheManagerFactory.createClusteredCacheManager();
      cm2 = TestCacheManagerFactory.createClusteredCacheManager();
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(false);
      cm1.defineConfiguration("cache", c.build());
      cm2.defineConfiguration("cache", c.build());

      cm1.getCache("cache");

      // this will mean only 1 cache in the cluster so far
      assert cm1.getMembers().size() == 1;
      Address myAddress = cm1.getAddress();
      assert cm1.getMembers().contains(myAddress);

      // now attach a mock notifier
      CacheManagerNotifierWrapper nw = new CacheManagerNotifierWrapper(TestingUtil.extractComponent(cm1.getCache("cache"), CacheManagerNotifier.class));
      CacheManagerNotifier origNotifier = TestingUtil.replaceComponent(cm1, CacheManagerNotifier.class, nw, true);
      try {
         // start a second cache.
         Cache c2 = cm2.getCache("cache");
         TestingUtil.blockUntilViewsReceived(60000, cm1, cm2);
         assert nw.notifyView;
         assertEquals(myAddress, nw.address);
      } finally {
         TestingUtil.replaceComponent(cm1, CacheManagerNotifier.class, origNotifier, true);
      }
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
