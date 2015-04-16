package org.infinispan.test;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Collections;
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
 * Note that this will cause {@link #createCacheManagers()}  to be called before each method.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class MultipleCacheManagersTest extends AbstractCacheTest {

   protected List<EmbeddedCacheManager> cacheManagers = Collections.synchronizedList(new ArrayList<EmbeddedCacheManager>());
   protected IdentityHashMap<Cache<?, ?>, ReplListener> listeners = new IdentityHashMap<Cache<?, ?>, ReplListener>();

   @BeforeClass(alwaysRun = true)
   public void createBeforeClass() throws Throwable {
      if (cleanupAfterTest()) callCreateCacheManagers();
   }

   private void callCreateCacheManagers() throws Throwable {
      try {
         log.debug("Creating cache managers");
         createCacheManagers();
         log.debug("Cache managers created, ready to start the test");
      } catch (Throwable th) {
         log.error("Error in test setup: ", th);
         throw th;
      }
   }

   @BeforeMethod(alwaysRun = true)
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
            throw new IllegalStateException("No caches registered! Use registerCacheManager(Cache... caches) to do that!");
         TestingUtil.clearContent(cacheManagers);
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
         for (Cache<?, ?> cache : TestingUtil.getRunningCaches(cm)) {
            Configuration config = cache.getCacheConfiguration();
            try {
               assert config.transaction().syncCommitPhase() : "Must use a sync commit phase!";
               assert config.transaction().syncRollbackPhase(): "Must use a sync rollback phase!";
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
      return addClusterEnabledCacheManager(new TransportFlags());
   }

   /**
    * Creates a new cache manager, starts it, and adds it to the list of known
    * cache managers on the current thread. Uses a default clustered cache
    * manager global config.
    *
    * @param flags properties that allow transport stack to be tweaked
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(TransportFlags flags) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(new ConfigurationBuilder(), flags);
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
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder defaultConfig) {
      return addClusterEnabledCacheManager(defaultConfig, new TransportFlags());
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder defaultConfig) {
      return addClusterEnabledCacheManager(globalBuilder, defaultConfig, new TransportFlags());
   }

   /**
    * Creates a new optionally transactional cache manager, starts it, and adds it to the list of known cache managers on
    * the current thread.  Uses a default clustered cache manager global config.
    *
    * @param builder default cfg to use
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder, TransportFlags flags) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(builder, flags);
      cacheManagers.add(cm);
      return cm;
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder, TransportFlags flags) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, builder, flags);
      cacheManagers.add(cm);
      return cm;
   }

   protected void createCluster(ConfigurationBuilder builder, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(builder);
   }

   protected void createCluster(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(new GlobalConfigurationBuilder().read(globalBuilder.build()), builder);
   }

   protected void defineConfigurationOnAllManagers(String cacheName, ConfigurationBuilder b) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(cacheName, b.build());
      }
   }

   protected  <K, V> List<Cache<K, V>> getCaches(String cacheName) {
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>();
      List<EmbeddedCacheManager> managers = new ArrayList<>(cacheManagers);
      for (EmbeddedCacheManager cm : managers) {
         Cache<K, V> c;
         if (cacheName == null)
            c = cm.getCache();
         else
            c = cm.getCache(cacheName);
         caches.add(c);
      }
      return caches;
   }

   protected void waitForClusterToForm(String cacheName) {
      List<Cache<Object, Object>> caches = getCaches(cacheName);
      Cache<Object, Object> cache = caches.get(0);
      TestingUtil.blockUntilViewsReceived(30000, caches);
      if (cache.getCacheConfiguration().clustering().cacheMode().isClustered()) {
         TestingUtil.waitForRehashToComplete(caches);
      }
   }

   protected void waitForClusterToForm() {
      waitForClusterToForm((String) null);
   }

   protected void waitForClusterToForm(String... names) {
      for (String name : names) {
         waitForClusterToForm(name);
      }
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

   protected <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, ConfigurationBuilder builder) {
      return createClusteredCaches(numMembersInCluster, cacheName, builder, new TransportFlags());
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, ConfigurationBuilder builder, TransportFlags flags) {
      List<Cache<K, V>> caches = new ArrayList<>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(flags);
         cm.defineConfiguration(cacheName, builder.build());
         Cache<K, V> cache = cm.getCache(cacheName);
         caches.add(cache);
      }
      waitForClusterToForm(cacheName);
      return caches;
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster,
                                                            ConfigurationBuilder defaultConfigBuilder) {
      List<Cache<K, V>> caches = new ArrayList<>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultConfigBuilder);
         Cache<K, V> cache = cm.getCache();
         caches.add(cache);

      }
      waitForClusterToForm();
      return caches;
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster,
                                                            ConfigurationBuilder defaultConfig,
                                                            TransportFlags flags) {
      List<Cache<K, V>> caches = new ArrayList<>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultConfig, flags);
         Cache<K, V> cache = cm.getCache();
         caches.add(cache);
      }
      waitForClusterToForm();
      return caches;
   }

   /**
    * Create cacheNames.length in each CacheManager (numMembersInCluster cacheManagers).
    *
    * @param numMembersInCluster
    * @param defaultConfigBuilder
    * @param cacheNames
    * @return A list with size numMembersInCluster containing a list of cacheNames.length caches
    */
   protected <K, V> List<List<Cache<K, V>>> createClusteredCaches(int numMembersInCluster,
         ConfigurationBuilder defaultConfigBuilder, String[] cacheNames) {
      List<List<Cache<K, V>>> allCaches = new ArrayList<>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultConfigBuilder);
         List<Cache<K, V>> currentCacheManagerCaches = new ArrayList<>(cacheNames.length);

         for (String cacheName : cacheNames) {
            Cache<K, V> cache = cm.getCache(cacheName);
            currentCacheManagerCaches.add(cache);
         }
         allCaches.add(currentCacheManagerCaches);
      }
      waitForClusterToForm(cacheNames);
      return allCaches;
   }

   protected ReplListener replListener(Cache<?, ?> cache) {
      ReplListener listener = listeners.get(cache);
      if (listener == null) {
         listener = new ReplListener(cache);
         listeners.put(cache, listener);
      }
      return listener;
   }

   protected EmbeddedCacheManager manager(int i) {
      return cacheManagers.get(i);
   }

   public EmbeddedCacheManager manager(Address a) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         if (cm.getAddress().equals(a)) {
            return cm;
         }
      }
      throw new IllegalArgumentException(a + " is not a valid cache manager address!");
   }

   public int managerIndex(Address a) {
      for (int i = 0; i < cacheManagers.size(); i++) {
         EmbeddedCacheManager cm = cacheManagers.get(i);
         if (cm.getAddress().equals(a)) {
            return i;
         }
      }
      throw new IllegalArgumentException(a + " is not a valid cache manager address!");
   }

   protected <K, V> Cache<K, V> cache(int managerIndex, String cacheName) {
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

   protected DataContainer dataContainer(int index) {
      return advancedCache(index).getDataContainer();
   }


   /**
    * Create the cache managers you need for your test.  Note that the cache managers you create *must* be created using
    * {@link #addClusterEnabledCacheManager()}
    */
   protected abstract void createCacheManagers() throws Throwable;

   protected Address address(int cacheIndex) {
      return manager(cacheIndex).getAddress();
   }

   protected <A, B> AdvancedCache<A, B> advancedCache(int i) {
      return this.<A,B>cache(i).getAdvancedCache();
   }

   protected <A, B> AdvancedCache<A, B> advancedCache(int i, String cacheName) {
      return this.<A, B>cache(i, cacheName).getAdvancedCache();
   }

   protected <K, V> List<Cache<K, V>> caches(String name) {
      return getCaches(name);
   }

   protected <K, V> List<Cache<K, V>> caches() {
      return caches(null);
   }

   protected Address address(Cache<?, ?> c) {
      return c.getAdvancedCache().getRpcManager().getAddress();
   }

   protected LockManager lockManager(int i) {
      return TestingUtil.extractLockManager(cache(i));
   }

   protected LockManager lockManager(int i, String cacheName) {
      return TestingUtil.extractLockManager(getCache(i, cacheName));
   }

   public List<EmbeddedCacheManager> getCacheManagers() {
      return cacheManagers;
   }

   /**
    * Kills the cache manager with the given index and waits for the new cluster to form.
    */
   protected void killMember(int cacheIndex) {
      killMember(cacheIndex, null);
   }

   /**
    * Kills the cache manager with the given index and waits for the new cluster to form using the provided cache
    */
   protected void killMember(int cacheIndex, String cacheName) {
      List<Cache<Object, Object>> caches = caches(cacheName);
      caches.remove(cacheIndex);
      manager(cacheIndex).stop();
      cacheManagers.remove(cacheIndex);
      if (caches.size() > 0) {
         TestingUtil.blockUntilViewsReceived(60000, false, caches);
         TestingUtil.waitForRehashToComplete(caches);
      }
   }

   /**
    * Creates a {@link org.infinispan.affinity.KeyAffinityService} and uses it for generating a key that maps to the given address.
    * @param nodeIndex the index of tha cache where to be the main data owner of the returned key
    */
   protected Object getKeyForCache(int nodeIndex) {
      final Cache<Object, Object> cache = cache(nodeIndex);
      return getKeyForCache(cache);
   }

   protected Object getKeyForCache(int nodeIndex, String cacheName) {
      final Cache<Object, Object> cache = cache(nodeIndex, cacheName);
      return getKeyForCache(cache);
   }

   protected Object getKeyForCache(Cache<?, ?> cache) {
      return new MagicKey(cache);
   }

   protected void assertNotLocked(final String cacheName, final Object key) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            boolean aNodeIsLocked = false;
            for (int i = 0; i < caches(cacheName).size(); i++) {
               final boolean isLocked = lockManager(i, cacheName).isLocked(key);
               if (isLocked) log.trace(key + " is locked on cache index " + i + " by " + lockManager(i, cacheName).getOwner(key));
               aNodeIsLocked = aNodeIsLocked || isLocked;
            }
            return !aNodeIsLocked;
         }
      });
   }

   protected void assertNotLocked(final Object key) {
      assertNotLocked((String)null, key);
   }

   protected boolean checkTxCount(int cacheIndex, int localTx, int remoteTx) {
      final int localTxCount = TestingUtil.getTransactionTable(cache(cacheIndex)).getLocalTxCount();
      final int remoteTxCount = TestingUtil.getTransactionTable(cache(cacheIndex)).getRemoteTxCount();
      log.tracef("Cache index %s, local tx %4s, remote tx %4s \n", cacheIndex, localTxCount, remoteTxCount);
      return localTxCount == localTx && remoteTxCount == remoteTx;
   }

   protected void assertNotLocked(int cacheIndex, Object key) {
      assertEventuallyNotLocked(cache(cacheIndex), key);
   }

   protected void assertLocked(int cacheIndex, Object key) {
      assertLocked(cache(cacheIndex), key);
   }

   protected boolean checkLocked(int index, Object key) {
      return checkLocked(cache(index), key);
   }

   protected <K, V> Cache<K, V> getLockOwner(Object key) {
      return getLockOwner(key, null);
   }

   protected <K, V> Cache<K, V> getLockOwner(Object key, String cacheName) {
      Configuration c = getCache(0, cacheName).getCacheConfiguration();
      if (c.clustering().cacheMode().isReplicated() || c.clustering().cacheMode().isInvalidation()) {
         return getCache(0, cacheName); //for replicated caches only the coordinator acquires lock
      }  else {
         if (!c.clustering().cacheMode().isDistributed()) throw new IllegalStateException("This is not a clustered cache!");
         final Address address = getCache(0, cacheName).getAdvancedCache().getDistributionManager().locate(key).get(0);
         for (Cache<K, V> cache : this.<K, V>caches(cacheName)) {
            if (cache.getAdvancedCache().getRpcManager().getTransport().getAddress().equals(address)) {
               return cache;
            }
         }
         throw new IllegalStateException();
      }
   }

   protected void assertKeyLockedCorrectly(Object key) {
      assertKeyLockedCorrectly(key, null);
   }

   protected void assertKeyLockedCorrectly(Object key, String cacheName) {
      final Cache<?, ?> lockOwner = getLockOwner(key, cacheName);
      assert checkLocked(lockOwner, key);
      for (Cache<?, ?> c : caches(cacheName)) {
         if (c != lockOwner)
            assert !checkLocked(c, key) : "Key " + key + " is locked on cache " + c + " (" + cacheName
                  + ") and it shouldn't";
      }
   }

   private <K, V> Cache<K, V> getCache(int index, String name) {
      return name == null ? this.<K, V>cache(index) : this.<K, V>cache(index, name);
   }

   protected void forceTwoPhase(int cacheIndex) throws SystemException, RollbackException {
      TransactionManager tm = tm(cacheIndex);
      Transaction tx = tm.getTransaction();
      tx.enlistResource(new XAResourceAdapter());
   }

   protected void assertNoTransactions() {
      assertNoTransactions(null);
   }

   protected void assertNoTransactions(final String cacheName) {
      eventually("There are pending transactions!", new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (Cache<?, ?> cache : caches(cacheName)) {
               final TransactionTable transactionTable = TestingUtil.extractComponent(cache, TransactionTable.class);
               int localTxCount = transactionTable.getLocalTxCount();
               int remoteTxCount = transactionTable.getRemoteTxCount();
               if (localTxCount != 0 || remoteTxCount != 0) {
                  log.tracef("Local tx=%s, remote tx=%s, for cache %s ", transactionTable.getLocalGlobalTransaction(),
                             transactionTable.getRemoteGlobalTransaction(), address(cache));
                  return false;
               }
            }
            return true;
         }
      });
   }

   protected TransactionTable transactionTable(int cacheIndex) {
      return advancedCache(cacheIndex).getComponentRegistry()
            .getComponent(TransactionTable.class);
   }

   protected void assertEventuallyEquals(
         final int cacheIndex, final Object key, final Object value) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value == null
                  ? null == cache(cacheIndex).get(key)
                  : value.equals(cache(cacheIndex).get(key));
         }
      });
   }

}
