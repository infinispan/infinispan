package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * Base class for tests that operates on clusters of caches. The way tests extending this class operates is:
 * <pre>
 *    1) created cache managers before tests start. The cache managers are only created once
 *    2) after each test method runs, the cache instances are being cleared
 *    3) next test method will run on same cacheManager instance. This way the test is much faster, as CacheManagers
 *       are expensive to create.
 * </pre>
 * If, however, you would like your cache managers destroyed after every <i>test method</i> instead of the </i>test
 * class</i>, you could set the <tt>cleanup</tt> field to {@link MultipleCacheManagersTest.CleanupPhase#AFTER_METHOD} in
 * your test's constructor.  E.g.:
 * <pre>
 * <p/>
 * public void MyTest extends MultipleCacheManagersTest {
 *    public MyTest() {
 *       cleanup =  CleanupPhase.AFTER_METHOD;
 *    }
 * }
 * <p/>
 * </pre>
 * <p/>
 * Note that this will cuse {@link #createCacheManagers()}  to be called befpre each method.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test
public abstract class MultipleCacheManagersTest extends AbstractCacheTest {

   protected List<EmbeddedCacheManager> cacheManagers = new ArrayList<EmbeddedCacheManager>();
   private IdentityHashMap<Cache, ReplListener> listeners = new IdentityHashMap<Cache, ReplListener>();

   @BeforeClass (alwaysRun = true)
   public void createBeforeClass() throws Throwable {
      if (cleanup == CleanupPhase.AFTER_TEST) callCreateCacheManagers();
   }

   private void callCreateCacheManagers() throws Throwable {
      try {
         createCacheManagers();
      } catch (Throwable th) {
         th.printStackTrace();
         log.error("Error in test setup: ", th);
         throw th;
      }
   }

   @BeforeMethod
   public void createBeforeMethod() throws Throwable {
      if (cleanup == CleanupPhase.AFTER_METHOD) callCreateCacheManagers();
   }

   @AfterClass(alwaysRun = true)
   protected void destroy() {
      if (cleanup == CleanupPhase.AFTER_TEST) TestingUtil.killCacheManagers(cacheManagers);
      cacheManagers.clear();
      listeners.clear();
   }

   @AfterMethod(alwaysRun=true)
   protected void clearContent() throws Throwable {
      if (cleanup == CleanupPhase.AFTER_TEST) {
         assertSupportedConfig();
         log.debug("*** Test method complete; clearing contents on all caches.");
         if (cacheManagers.isEmpty())
            throw new IllegalStateException("No caches registered! Use registerCacheManager(Cache... caches) do that!");
         for (EmbeddedCacheManager cacheManager : cacheManagers) {
            TestingUtil.clearContent(cacheManager);
         }
      } else {
         TestingUtil.killCacheManagers(cacheManagers);
         cacheManagers.clear();
      }
   }

   /**
    * Reason: after a tm.commit is run, multiple tests assert that the new value (as within the committing transaction)
    * is present on a remote cache (i.e. not on the cache on which tx originated). If we don't use sync commit,
    * than this (i.e. actual commit of the tx on the remote cache) might happen after the tm.commit() returns,
    * and result in an intermittent failure for the assertion
    */
   protected void assertSupportedConfig() {
      for (EmbeddedCacheManager cm : cacheManagers) {
         for (Cache cache : TestingUtil.getRunningCaches(cm)) {
            Configuration config = cache.getConfiguration();
            try {
               assert config.isSyncCommitPhase();
               assert config.isSyncRollbackPhase();
            } catch (AssertionError e) {
               log.error("Invalid config for cache: " + getClass().getName());
               throw e;
            }
         }
      }
   }

   final protected void registerCacheManager(CacheContainer... cacheContainers) {
      for (CacheContainer ecm : cacheContainers) {
         this.cacheManagers.add((EmbeddedCacheManager) ecm);
      }
   }

   /**
    * Creates a new cache manager, starts it, and adds it to the list of known cache managers on the current thread.
    * Uses a default clustered cache manager global config.
    *
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager();
      cacheManagers.add(cm);
      return cm;
   }

   /**
    * Creates a new cache manager, starts it, and adds it to the list of known cache managers on the current thread.
    * Uses a default clustered cache manager global config.
    *
    * @param defaultConfig default cfg to use
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration defaultConfig) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(defaultConfig);
      cacheManagers.add(cm);
      return cm;
   }

   protected void defineConfigurationOnAllManagers(String cacheName, Configuration c) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(cacheName, c);
      }
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster, String cacheName, Configuration c) {
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager();
         cm.defineConfiguration(cacheName, c);
         Cache<K, V> cache = cm.getCache(cacheName);
         caches.add(cache);
      }
      TestingUtil.blockUntilViewsReceived(10000, caches);
      return caches;
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster, Configuration defaultConfig) {
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultConfig);
         Cache<K, V> cache = cm.getCache();
         caches.add(cache);
      }
      TestingUtil.blockUntilViewsReceived(10000, caches);
      return caches;
   }

   public ReplListener replListener(Cache cache) {
      ReplListener listener = listeners.get(cache);
      if (listener == null) {
         listener = new ReplListener(cache);
         listeners.put(cache, listener);
      }
      return listener;
   }

   public EmbeddedCacheManager manager(int i) {
      return cacheManagers.get(i);
   }

   protected Cache cache(int managerIndex, String cacheName) {
      return manager(managerIndex).getCache(cacheName);
   }

   protected void assertClusterSize(String message, int size) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         assert cm.getMembers() != null && cm.getMembers().size() == size : message;
      }
   }

   protected void removeCacheFromCluster(String cacheName) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         TestingUtil.killCaches(cm.getCache(cacheName));
      }
   }

   /**
    * Returns the default cache from that manager.
    */
   protected Cache cache(int index) {
      return manager(index).getCache();
   }

   /**
    * Create the cache managers you need for your test.  Note that the cache managers you create *must* be created using
    * {@link #addClusterEnabledCacheManager()}
    */
   protected abstract void createCacheManagers() throws Throwable;

   public Address address(int cacheIndex) {
      return cache(cacheIndex).getAdvancedCache().getRpcManager().getAddress();
   }
}
