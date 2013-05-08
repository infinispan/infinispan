/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.xsite;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.TransactionTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Mircea Markus
 */
@Test (groups = "xsite")
public abstract class AbstractXSiteTest extends AbstractCacheTest {

   List<TestSite> sites = new ArrayList<TestSite>();
   private Map<String, Integer> siteName2index = new HashMap<String, Integer>();

   @BeforeMethod
   public void createBeforeMethod() throws Throwable {
      if (isCleanupAfterMethod()) createSites();
   }

   @BeforeClass
   public void createBeforeClass() throws Throwable {
      if (!isCleanupAfterMethod()) createSites();
   }

   @AfterMethod
   protected void clearContent() throws Throwable {
      if (!isCleanupAfterMethod()) {
         for (TestSite ts : sites) {
            TestingUtil.clearContent(ts.cacheManagers);
         }
      } else {
         killSites();
      }
   }

   @AfterClass
   protected void destroy() {
      if (cleanupAfterTest())  {
         killSites();
      }
   }

   private void killSites() {
      for (TestSite ts : sites) {
         TestingUtil.killCacheManagers(ts.cacheManagers);
      }
      sites.clear();
      siteName2index.clear();
   }

   protected abstract void createSites();

   protected TestSite createSite(String siteName, int numNodes, GlobalConfigurationBuilder gcb, ConfigurationBuilder defaultCacheConfig) {
      TestSite testSite = new TestSite();
      testSite.createClusteredCaches(numNodes, siteName, null, gcb, defaultCacheConfig, new TransportFlags().withSiteIndex(sites.size()).withSiteName(siteName));
      sites.add(testSite);
      siteName2index.put(siteName, sites.size() - 1);
      return testSite;
   }

   protected TestSite site(int index) {
      return sites.get(index);
   }

   protected TestSite site(String name) {
      return sites.get(siteName2index.get(name));
   }

   protected <K,V> Cache<K,V> cache(String site, int index) {
      return site(site).cache(index);
   }

   protected <K,V> Cache<K,V> cache(String site, String cacheName, int index) {
      return site(site).cache(cacheName, index);
   }


   protected void startCache(String siteName, String cacheName, ConfigurationBuilder configurationBuilder) {
      TestSite site = site(siteName);
      for (EmbeddedCacheManager ecm : site.cacheManagers) {
         Configuration config = configurationBuilder.build();
         ecm.defineConfiguration(cacheName, config);
      }
      site.waitForClusterToForm(cacheName);
   }


   public static class TestSite {
      protected List<EmbeddedCacheManager> cacheManagers = new ArrayList<EmbeddedCacheManager>();

      protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster, String siteName, String cacheName,
                                                               GlobalConfigurationBuilder gcb, ConfigurationBuilder builder, TransportFlags flags) {
         List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>(numMembersInCluster);
         for (int i = 0; i < numMembersInCluster; i++) {
            EmbeddedCacheManager cm = addClusterEnabledCacheManager(flags, gcb, builder, siteName);
            if (cacheName != null)
               cm.defineConfiguration(cacheName, builder.build());

            Cache<K, V> cache = (Cache<K, V>) (cacheName == null ? cm.getCache() : cm.getCache(cacheName));

            caches.add(cache);
         }
         waitForClusterToForm(cacheName);
         return caches;
      }

      protected EmbeddedCacheManager addClusterEnabledCacheManager(TransportFlags flags, GlobalConfigurationBuilder gcb, ConfigurationBuilder builder, String siteName) {
         GlobalConfigurationBuilder clone = GlobalConfigurationBuilder.defaultClusteredBuilder();

         //get the transport here as clone.read below would inject the same transport reference into the clone
         // which we don't want
         Transport transport = clone.transport().getTransport();
         clone.read(gcb.build());

         clone.transport().transport(transport);
         clone.transport().clusterName("ISPN(SITE " + siteName + ")");

         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(clone, builder, flags);
         cacheManagers.add(cm);
         return cm;
      }

      protected void waitForClusterToForm(String cacheName) {
         List<Cache> caches = getCaches(cacheName);
         Cache<Object, Object> cache = caches.get(0);
         TestingUtil.blockUntilViewsReceived(10000, caches);
         if (cache.getConfiguration().getCacheMode().isDistributed()) {
            TestingUtil.waitForRehashToComplete(caches);
         }
      }

      private List<Cache> getCaches(String cacheName) {
         List<Cache> caches;
         caches = new ArrayList<Cache>();
         for (EmbeddedCacheManager cm : cacheManagers) {
            caches.add(cacheName == null ? cm.getCache() : cm.getCache(cacheName));
         }
         return caches;
      }

      public <K,V> Cache<K,V> cache(int index) {
         return cacheManagers.get(index).getCache();
      }

      public <K,V> AdvancedCache<K,V> advancedCache(int index) {
         Cache<K, V> cache = cache(index);
         return cache.getAdvancedCache();
      }

      public <K, V> Cache<K, V> cache(String cacheName, int index) {
         return cacheManagers.get(index).getCache(cacheName);
      }
   }

   private boolean isCleanupAfterMethod() {
      return getClass().getAnnotation(CleanupAfterMethod.class) != null;
   }

   protected  TransactionTable txTable(Cache cache) {
      return cache.getAdvancedCache().getComponentRegistry().getComponent(TransactionTable.class);
   }

}
