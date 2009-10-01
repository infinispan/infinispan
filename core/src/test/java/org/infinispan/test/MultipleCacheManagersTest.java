package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
@Test(groups = {"functional", "unit"})
public abstract class MultipleCacheManagersTest extends AbstractCacheTest {

   protected List<CacheManager> cacheManagers = new ArrayList<CacheManager>();
   private IdentityHashMap<Cache, ReplListener> listeners = new IdentityHashMap<Cache, ReplListener>();

   @BeforeClass
   public void createBeforeClass() throws Throwable {
      if (cleanup == CleanupPhase.AFTER_TEST) callCreateCacheManagers();
   }

   private void callCreateCacheManagers() throws Throwable {
      try {
         createCacheManagers();
      } catch (Throwable th) {
         th.printStackTrace();
         log.error("Error in test setup: " + th);
         throw th;
      }
   }

   @BeforeMethod
   public void createBeforeMethod() throws Throwable {
      if (cleanup == CleanupPhase.AFTER_METHOD) callCreateCacheManagers();
   }

   @AfterClass(alwaysRun=true)
   protected void destroy() {
      if (cleanup == CleanupPhase.AFTER_TEST) TestingUtil.killCacheManagers(cacheManagers);
   }

   @AfterMethod(alwaysRun=true)
   protected void clearContent() throws Throwable {
      if (cleanup == CleanupPhase.AFTER_TEST) {
         assertSupportedConfig();
         log.debug("*** Test method complete; clearing contents on all caches.");
         if (cacheManagers.isEmpty())
            throw new IllegalStateException("No caches registered! Use registerCacheManager(Cache... caches) do that!");
         for (CacheManager cacheManager : cacheManagers) {
            super.clearContent(cacheManager);
         }
      } else {
         TestingUtil.killCacheManagers(cacheManagers);
         cacheManagers.clear();
      }
   }

   protected void assertSupportedConfig() {
      for (CacheManager cm : cacheManagers) {
         for (Cache cache : getRunningCaches(cm)) {
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

   final protected void registerCacheManager(CacheManager... cacheManagers) {
      this.cacheManagers.addAll(Arrays.asList(cacheManagers));
   }

   /**
    * Creates a new cache manager, starts it, and adds it to the list of known cache managers on the current thread.
    * Uses a default clustered cache manager global config.
    *
    * @return the new CacheManager
    */
   protected CacheManager addClusterEnabledCacheManager() {
      CacheManager cm = TestCacheManagerFactory.createClusteredCacheManager();
      cacheManagers.add(cm);
      return cm;
   }

   protected void defineConfigurationOnAllManagers(String cacheName, Configuration c) {
      for (CacheManager cm : cacheManagers) {
         cm.defineConfiguration(cacheName, c);
      }
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster, String cacheName, Configuration c) {
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         CacheManager cm = addClusterEnabledCacheManager();
         cm.defineConfiguration(cacheName, c);
         Cache<K, V> cache = cm.getCache(cacheName);
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

   public CacheManager manager(int i) {
      return cacheManagers.get(i);
   }

   protected Cache cache(int managerIndex, String cacheName) {
      return manager(managerIndex).getCache(cacheName);
   }

   protected void assertClusterSize(String message, int size) {
      for (CacheManager cm : cacheManagers) {
         assert cm.getMembers() != null && cm.getMembers().size() == size : message;
      }
   }

   protected void removeCacheFromCluster(String cacheName) {
      for (CacheManager cm : cacheManagers) {
         TestingUtil.killCaches(cm.getCache(cacheName));
      }
   }

   /**
    * Create the cache managers you need for your test.  Note that the cache managers you create *must* be created using
    * {@link #addClusterEnabledCacheManager()}
    */
   protected abstract void createCacheManagers() throws Throwable;


   protected void assertNotLocked(Cache cache, Object key) {
      LockManager lockManager = TestingUtil.extractLockManager(cache);
      assert !lockManager.isLocked(key) : "expected key '" + key + "' not to be locked, and it is by: " + lockManager.getOwner(key);
   }

   protected void assertLocked(Cache cache, Object key) {
      LockManager lockManager = TestingUtil.extractLockManager(cache);
      assert lockManager.isLocked(key) : "expected key '" + key + "' to be locked, but it is not";
   }
}
