package org.horizon.test;

import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.manager.CacheManager;
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
 * class</i>, you could set the <tt>cleanup</tt> field to {@link org.horizon.test.MultipleCacheManagersTest.CleanupPhase#AFTER_METHOD}
 * in your test's constructor.  E.g.:
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

   protected static enum CleanupPhase {
      AFTER_METHOD, AFTER_TEST
   }

   private List<CacheManager> cacheManagers = new ArrayList<CacheManager>();
   private IdentityHashMap<Cache, ReplListener> listeners = new IdentityHashMap<Cache, ReplListener>();
   protected CleanupPhase cleanup = CleanupPhase.AFTER_TEST;

   @BeforeClass
   public void createBeforeClass() throws Throwable {
      if (cleanup == CleanupPhase.AFTER_TEST) callCreateCacheManagers();
   }

   private void callCreateCacheManagers() {
      try {
         createCacheManagers();
      } catch (Throwable th) {
         th.printStackTrace();
         log.error("Error in test setup: " + th);
      }
   }

   @BeforeMethod
   public void createBeforeMethod() throws Throwable {
      if (cleanup == CleanupPhase.AFTER_METHOD) callCreateCacheManagers();
   }

   @AfterClass
   protected void destroy() {
      if (cleanup == CleanupPhase.AFTER_TEST) TestingUtil.killCacheManagers(cacheManagers);
   }

   @AfterMethod
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
      }
   }

   /**
    * @see #getDefaultReplicatedConfig()
    */
   private void assertSupportedConfig() {
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
      CacheManager cm = TestingUtil.createClusteredCacheManager();
      cacheManagers.add(cm);
      return cm;
   }

   protected void defineCacheOnAllManagers(String cacheName, Configuration c) {
      for (CacheManager cm : cacheManagers) {
         cm.defineCache(cacheName, c);
      }
   }

   protected List<Cache> createClusteredCaches(int numMembersInCluster, String cacheName, Configuration c) {
      List<Cache> caches = new ArrayList<Cache>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         CacheManager cm = addClusterEnabledCacheManager();
         cm.defineCache(cacheName, c);
         caches.add(cm.getCache(cacheName));
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
}
