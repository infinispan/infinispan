package org.infinispan.test;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
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
 * Note that this will cuse {@link #createCacheManagers()}  to be called before each method.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test
public abstract class MultipleCacheManagersTest extends AbstractCacheTest {

   protected List<EmbeddedCacheManager> cacheManagers = new ArrayList<EmbeddedCacheManager>();
   private IdentityHashMap<Cache, ReplListener> listeners = new IdentityHashMap<Cache, ReplListener>();

   @BeforeClass (alwaysRun = true)
   public void createBeforeClass() throws Throwable {
      if (cleanupAfterTest()) callCreateCacheManagers();
   }

   private void callCreateCacheManagers() throws Throwable {
      try {
         createCacheManagers();
      } catch (Throwable th) {
         th.printStackTrace();
         log.error("Error in test setup: ", th);
      }
   }

   @BeforeMethod
   public void createBeforeMethod() throws Throwable {
      if (cleanupAfterMethod()) callCreateCacheManagers();
   }

   @AfterClass(alwaysRun = true)
   protected void destroy() {
      if (cleanupAfterTest()) TestingUtil.killCacheManagers(cacheManagers);
      cacheManagers.clear();
      listeners.clear();
   }

   @AfterMethod(alwaysRun=true)
   protected void clearContent() throws Throwable {
      if (cleanupAfterTest()) {
//         assertSupportedConfig();
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
               assert config.isSyncCommitPhase() : "Must use a sync commit phase!";
               assert config.isSyncRollbackPhase(): "Must use a sync rollback phase!";
            } catch (AssertionError e) {
               log.error("Invalid config for cache in test: " + getClass().getName());
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
    * Creates a new non-transactional cache manager, starts it, and adds it to the list of known cache managers on the
    * current thread.  Uses a default clustered cache manager global config.
    *
    * @param defaultConfig default cfg to use
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration defaultConfig) {
      return addClusterEnabledCacheManager(defaultConfig, false);
   }

   /**
    * Creates a new optionally transactional cache manager, starts it, and adds it to the list of known cache managers on
    * the current thread.  Uses a default clustered cache manager global config.
    *
    * @param defaultConfig default cfg to use
    * @param transactional if true, the configuration will be decorated with necessary transactional settings
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration defaultConfig, boolean transactional) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(defaultConfig, transactional);
      cacheManagers.add(cm);
      return cm;
   }

   /**
    * Creates a new cache manager, starts it, and adds it to the list of known cache managers on the current thread.
    * @param mode cache mode to use
    * @param transactional if true, the configuration will be decorated with necessary transactional settings
    * @return an embedded cache manager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration.CacheMode mode, boolean transactional) {
      Configuration configuration = getDefaultClusteredConfig(mode, transactional);
      configuration.setCacheMode(mode);
      return addClusterEnabledCacheManager(configuration);
   }

   protected void createCluster(Configuration.CacheMode mode, boolean transactional, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(mode, transactional);
   }
   
   protected void createCluster(Configuration config, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(config);
   }

   protected void createCluster(Configuration config, boolean transactional, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(config, transactional);
   }

   protected void createCluster(Configuration.CacheMode mode, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(mode, true);
   }

   protected void defineConfigurationOnAllManagers(String cacheName, Configuration c) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(cacheName, c);
      }
   }

   protected void waitForClusterToForm(String cacheName) {
      List<Cache> caches;
      caches = getCaches(cacheName);
      Cache<Object, Object> cache = caches.get(0);
      TestingUtil.blockUntilViewsReceived(10000, caches);
      if (cache.getConfiguration().getCacheMode().isDistributed()) {
         BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(caches);
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

   protected void waitForClusterToForm() {
      waitForClusterToForm(null);
   }

   protected TransactionManager tm(Cache<?, ?> c) {
      return c.getAdvancedCache().getTransactionManager();
   }

   protected TransactionManager tm(int i, String cacheName) {
      return cache(i, cacheName ).getAdvancedCache().getTransactionManager();
   }


   protected TransactionManager tm(int i) {
      return cache(i).getAdvancedCache().getTransactionManager();
   }

   protected Transaction tx(int i) {
      try {
         return cache(i).getAdvancedCache().getTransactionManager().getTransaction();
      } catch (SystemException e) {
         throw new RuntimeException(e);
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
      TestingUtil.blockUntilViewsReceived(30000, caches);
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
   protected <A, B> Cache<A, B> cache(int index) {
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

   public AdvancedCache advancedCache(int i) {
      return cache(i).getAdvancedCache();
   }

   public AdvancedCache advancedCache(int i, String cacheName) {
      return cache(i, cacheName).getAdvancedCache();
   }

   public <K, V> List<Cache<K, V>> caches(String name) {
      List<Cache<K, V>> result = new ArrayList<Cache<K, V>>();
      for (EmbeddedCacheManager ecm : cacheManagers) {
         Cache<K, V> c;
         if (name == null)
            c = ecm.getCache();
         else
            c = ecm.getCache(name);
         result.add(c);
      }
      return result;
   }

   public <K, V> List<Cache<K, V>> caches() {
      return caches(null);
   }

   protected Address address(Cache c) {
      return c.getAdvancedCache().getRpcManager().getAddress();
   }

   protected LockManager lockManager(int i) {
      return TestingUtil.extractLockManager(cache(i));
   }
   
   protected LockManager lockManager(int i, String cacheName) {
      return TestingUtil.extractLockManager(cache(i, cacheName));
   }
}
