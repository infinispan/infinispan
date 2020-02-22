package org.infinispan.xsite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.impl.TransactionTable;
import org.jgroups.protocols.relay.RELAY2;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * @author Mircea Markus
 */
public abstract class AbstractXSiteTest extends AbstractCacheTest {

   protected List<TestSite> sites = new ArrayList<>();
   private Map<String, Integer> siteName2index = new HashMap<>();

   @BeforeMethod(alwaysRun = true) // run even for tests in the unstable group
   public void createBeforeMethod() {
      if (cleanupAfterMethod()) createSites();
   }

   @BeforeClass(alwaysRun = true) // run even for tests in the unstable group
   public void createBeforeClass() {
      if (cleanupAfterTest()) createSites();
   }

   @AfterMethod(alwaysRun = true) // run even if the test failed
   protected void clearContent() throws Throwable {
      if (cleanupAfterTest()) {
         clearSites();
      } else {
         killSites();
      }
   }

   private void clearSites() {
      for (TestSite ts : sites) {
         clearSite(ts);
      }
   }

   protected void clearSite(TestSite ts) {
      TestingUtil.clearContent(ts.cacheManagers);
   }

   @AfterClass(alwaysRun = true) // run even if the test failed
   protected void destroy() {
      if (cleanupAfterTest())  {
         killSites();
      }
   }

   private void killSites() {
      for (TestSite ts : sites) {
         killSite(ts);
      }
      sites.clear();
      siteName2index.clear();
   }

   protected void killSite(String siteName) {
      Integer index = siteName2index.remove(siteName);
      if (index == null) {
         return;
      }
      TestSite site = sites.remove(index.intValue());
      killSite(site);
   }

   protected void killSite(TestSite ts) {
      ts.cacheManagers.forEach(Lifecycle::stop);
   }

   protected abstract void createSites();

   protected TestSite createSite(String siteName, int numNodes, GlobalConfigurationBuilder gcb, ConfigurationBuilder defaultCacheConfig) {
      TestSite testSite = new TestSite(siteName, sites.size());
      testSite.createClusteredCaches(numNodes, null, gcb, defaultCacheConfig);
      sites.add(testSite);
      siteName2index.put(siteName, sites.size() - 1);
      return testSite;
   }

   /**
    * Wait for all the site masters to see a specific list of sites.
    */
   public void waitForSites(String... siteNames) {
      waitForSites(10, TimeUnit.SECONDS, siteNames);
   }

   /**
    * Wait for all the site masters to see a specific list of sites.
    */
   public void waitForSites(long timeout, TimeUnit unit, String... siteNames) {
      long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
      Set<String> expectedSites = new HashSet<>(Arrays.asList(siteNames));
      sites.forEach(site -> {
         site.cacheManagers.forEach(manager -> {
            RELAY2 relay2 = ((JGroupsTransport) manager.getTransport()).getChannel().getProtocolStack()
                                                                           .findProtocol(RELAY2.class);
            if (!relay2.isSiteMaster())
               return;

            while (System.nanoTime() - deadlineNanos < 0) {
               if (expectedSites.equals(manager.getTransport().getSitesView()))
                  break;

               LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }

            Set<String> currentSitesView = manager.getTransport().getSitesView();
            if (!expectedSites.equals(currentSitesView)) {
               throw new AssertionError(String.format("Timed out waiting for bridge view %s on %s after %d %s. Current bridge view is %s", expectedSites, manager.getAddress(), timeout, unit, currentSitesView));
            }
         });
      });
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

   protected <K,V> List<Cache<K,V>> caches(String site) {
      return caches(site, null);
   }

   protected <K,V> List<Cache<K,V>> caches(String site, String cacheName) {
      return Collections.unmodifiableList(site(site).getCaches(cacheName));
   }

   protected void startCache(String siteName, String cacheName, ConfigurationBuilder configurationBuilder) {
      TestSite site = site(siteName);
      for (EmbeddedCacheManager ecm : site.cacheManagers) {
         Configuration config = configurationBuilder.build();
         ecm.defineConfiguration(cacheName, getDefaultCacheName(), config);
      }
      site.waitForClusterToForm(cacheName);
   }

   protected final <K, V> void assertInSite(String siteName, AssertCondition<K, V> condition) {
      for (Cache<K, V> cache : this.<K, V>caches(siteName)) {
         condition.assertInCache(cache);
      }
   }

   protected final <K, V> void assertInSite(String siteName, String cacheName, AssertCondition<K, V> condition) {
      for (Cache<K, V> cache : this.<K, V>caches(siteName, cacheName)) {
         condition.assertInCache(cache);
      }
   }

   protected final <K, V> void assertEventuallyInSite(final String siteName, final EventuallyAssertCondition<K, V> condition,
                                                long timeout, TimeUnit timeUnit) {
      eventually(() -> {
         for (Cache<K, V> cache : AbstractXSiteTest.this.<K, V>caches(siteName)) {
            if (!condition.assertInCache(cache)) {
               return false;
            }
         }
         return true;
      }, timeUnit.toMillis(timeout));
   }

   protected final <K, V> void assertEventuallyInSite(final String siteName, final String cacheName, final EventuallyAssertCondition<K, V> condition,
                                                      long timeout, TimeUnit timeUnit) {
      eventually(() -> {
         for (Cache<K, V> cache : AbstractXSiteTest.this.<K, V>caches(siteName, cacheName)) {
            if (!condition.assertInCache(cache)) {
               return false;
            }
         }
         return true;
      }, timeUnit.toMillis(timeout));
   }

   protected RELAY2 getRELAY2(EmbeddedCacheManager cacheManager) {
      JGroupsTransport transport = (JGroupsTransport) cacheManager.getTransport();
      return transport.getChannel().getProtocolStack().findProtocol(RELAY2.class);
   }

   protected interface AssertCondition<K, V> {
      void assertInCache(Cache<K, V> cache);
   }

   protected interface EventuallyAssertCondition<K, V> {
      boolean assertInCache(Cache<K, V> cache);
   }

   public static class TestSite {
      protected List<EmbeddedCacheManager> cacheManagers = new ArrayList<>();
      private final String siteName;
      private final int siteIndex;

      public TestSite(String siteName, int siteIndex) {
         this.siteName = siteName;
         this.siteIndex = siteIndex;
      }

      public String getSiteName() {
         return siteName;
      }

      private TransportFlags transportFlags() {
         return new TransportFlags().withPortRange(siteIndex).withSiteName(siteName).withFD(true);
      }

      protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster, String cacheName,
                                                               GlobalConfigurationBuilder gcb, ConfigurationBuilder builder) {
         List<Cache<K, V>> caches = new ArrayList<>(numMembersInCluster);
         final TransportFlags flags = transportFlags();
         for (int i = 0; i < numMembersInCluster; i++) {
            EmbeddedCacheManager cm = addClusterEnabledCacheManager(flags, gcb, builder);
            if (cacheName != null)
               cm.defineConfiguration(cacheName, builder.build());

            Cache<K, V> cache = cacheName == null ? cm.getCache() : cm.getCache(cacheName);

            caches.add(cache);
         }
         waitForClusterToForm(cacheName);
         return caches;
      }

      protected EmbeddedCacheManager addClusterEnabledCacheManager(TransportFlags flags, GlobalConfigurationBuilder gcb,
                                                                   ConfigurationBuilder builder) {
         GlobalConfigurationBuilder clone = GlobalConfigurationBuilder.defaultClusteredBuilder();

         //get the transport here as clone.read below would inject the same transport reference into the clone
         // which we don't want
         Transport transport = clone.transport().getTransport();
         GlobalConfiguration original = gcb.build();
         clone.read(original);
         clone.transport().transport(transport);
         clone.transport().clusterName("ISPN(SITE " + siteName + ")");
         if (original.jmx().enabled()) {
            clone.jmx().enabled(true).domain(original.jmx().domain() + cacheManagers.size());
         }
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(clone, builder, flags);
         cacheManagers.add(cm);
         return cm;
      }

      public void waitForClusterToForm(String cacheName) {
         List<Cache<Object, Object>> caches = getCaches(cacheName);
         Cache<Object, Object> cache = caches.get(0);
         TestingUtil.blockUntilViewsReceived(10000, caches);
         if (cache.getCacheConfiguration().clustering().cacheMode().isDistributed()) {
            TestingUtil.waitForNoRebalance(caches);
         }
      }

      public void waitForClusterToForm(String cacheName, long timeout, TimeUnit timeUnit) {
         List<Cache<Object, Object>> caches = getCaches(cacheName);
         Cache<Object, Object> cache = caches.get(0);
         TestingUtil.blockUntilViewsReceived((int) timeUnit.toMillis(timeout), false, caches);
         if (cache.getCacheConfiguration().clustering().cacheMode().isDistributed()) {
            TestingUtil.waitForNoRebalance(caches);
         }
      }

      public <K, V> List<Cache<K, V>> getCaches(String cacheName) {
         List<Cache<K,V>> caches = new ArrayList<>(cacheManagers.size());
         for (EmbeddedCacheManager cm : cacheManagers) {
            caches.add(cacheName == null ? cm.getCache() : cm.getCache(cacheName));
         }
         return caches;
      }

      public void addCache(GlobalConfigurationBuilder gBuilder, ConfigurationBuilder builder) {
         addCache(null, gBuilder, builder);
      }

      public void addCache(String cacheName, GlobalConfigurationBuilder gBuilder, ConfigurationBuilder builder) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(transportFlags(), gBuilder, builder);
         if (cacheName != null)
            cm.defineConfiguration(cacheName, builder.build());
      }

      public void kill(int index) {
         TestingUtil.killCacheManagers(cacheManagers.remove(index));
      }

      public <K,V> Cache<K,V> cache(int index) {
         return cacheManagers.get(index).getCache();
      }

      public <K,V> AdvancedCache<K,V> advancedCache(int index) {
         Cache<K, V> cache = cache(index);
         return cache.getAdvancedCache();
      }

      public <K, V> Cache<K, V> cache(String cacheName, int index) {
         return cacheName == null ?
               cache(index) :
               cacheManagers.get(index).getCache(cacheName);
      }

      public List<EmbeddedCacheManager> cacheManagers() {
         return Collections.unmodifiableList(cacheManagers);
      }
   }

   protected  TransactionTable txTable(Cache cache) {
      return cache.getAdvancedCache().getComponentRegistry().getComponent(TransactionTable.class);
   }

   @Override
   public String getDefaultCacheName() {
      return site(0).cacheManagers.get(0).getCacheManagerConfiguration().defaultCacheName().get();
   }
}
