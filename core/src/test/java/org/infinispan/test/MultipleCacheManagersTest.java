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
package org.infinispan.test;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.TransactionTable;
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
public abstract class MultipleCacheManagersTest extends AbstractCacheTest {

   protected List<EmbeddedCacheManager> cacheManagers = new ArrayList<EmbeddedCacheManager>();
   protected IdentityHashMap<Cache, ReplListener> listeners = new IdentityHashMap<Cache, ReplListener>();

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
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(flags);
      cacheManagers.add(cm);
      return cm;
   }

   /**
    * Creates a new non-transactional cache manager, starts it, and adds it to the list of known cache managers on the
    * current thread.  Uses a default clustered cache manager global config.
    *
    * @param defaultConfig default cfg to use
    * @return the new CacheManager
    * @deprecated Use {@link #addClusterEnabledCacheManager(
    *    org.infinispan.configuration.cache.ConfigurationBuilder)} instead
    */
   @Deprecated
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration defaultConfig) {
      return addClusterEnabledCacheManager(defaultConfig, new TransportFlags());
   }

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
    * @param defaultConfig default cfg to use
    * @return the new CacheManager
    * @deprecated Use {@link #addClusterEnabledCacheManager(
    *    org.infinispan.configuration.cache.ConfigurationBuilder, org.infinispan.test.fwk.TransportFlags)} instead
    */
   @Deprecated
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration defaultConfig, TransportFlags flags) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(defaultConfig, flags);
      cacheManagers.add(cm);
      return cm;
   }

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

   /**
    * Creates a new cache manager, starts it, and adds it to the list of known cache managers on the current thread.
    * @param mode cache mode to use
    * @param transactional if true, the configuration will be decorated with necessary transactional settings
    * @return an embedded cache manager
    */
   @Deprecated
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration.CacheMode mode, boolean transactional) {
      return addClusterEnabledCacheManager(mode, transactional, new TransportFlags());
   }

   @Deprecated
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration.CacheMode mode, boolean transactional, TransportFlags flags) {
      Configuration configuration = getDefaultClusteredConfig(mode, transactional);
      return addClusterEnabledCacheManager(configuration, flags);
   }

   @Deprecated
   protected void createCluster(Configuration.CacheMode mode, boolean transactional, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(mode, transactional);
   }

   protected void createCluster(ConfigurationBuilder builder, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(builder);
   }

   protected void createCluster(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(globalBuilder, builder);
   }

   @Deprecated
   protected void createCluster(Configuration config, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(config);
   }

   @Deprecated
   protected void createCluster(Configuration.CacheMode mode, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(mode, true);
   }

   protected void defineConfigurationOnAllManagers(String cacheName, Configuration c) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(cacheName, c);
      }
   }

   protected void defineConfigurationOnAllManagers(String cacheName, ConfigurationBuilder b) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(cacheName, b.build());
      }
   }

   private <K, V> List<Cache<K, V>> getCaches(String cacheName) {
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>();
      for (EmbeddedCacheManager cm : cacheManagers) {
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
      TestingUtil.blockUntilViewsReceived(10000, caches);
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

   /**
    * @deprecated Use {@link #createClusteredCaches(
    *    int, String, org.infinispan.configuration.cache.ConfigurationBuilder)} instead
    */
   @Deprecated
   protected <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, Configuration c) {
      return createClusteredCaches(numMembersInCluster, cacheName, c, new TransportFlags());
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, ConfigurationBuilder builder) {
      return createClusteredCaches(numMembersInCluster, cacheName, builder, new TransportFlags());
   }

   /**
    * @deprecated Use {@link #createClusteredCaches(
    *    int, String, org.infinispan.configuration.cache.ConfigurationBuilder, org.infinispan.test.fwk.TransportFlags)} instead
    */
   @Deprecated
   protected <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, Configuration c, TransportFlags flags) {
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(flags);
         cm.defineConfiguration(cacheName, c);
         Cache<K, V> cache = cm.getCache(cacheName);
         caches.add(cache);
      }
      waitForClusterToForm(cacheName);
      return caches;
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, ConfigurationBuilder builder, TransportFlags flags) {
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(flags);
         cm.defineConfiguration(cacheName, builder.build());
         Cache<K, V> cache = cm.getCache(cacheName);
         caches.add(cache);
      }
      waitForClusterToForm(cacheName);
      return caches;
   }

   @Deprecated
   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster, Configuration defaultConfig) {
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultConfig);
         Cache<K, V> cache = cm.getCache();
         caches.add(cache);

      }
      waitForClusterToForm();
      return caches;
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster,
                                                            ConfigurationBuilder defaultConfigBuilder) {
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>(numMembersInCluster);
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
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultConfig, flags);
         Cache<K, V> cache = cm.getCache();
         caches.add(cache);
      }
      waitForClusterToForm();
      return caches;
   }
   
   /**
    * Create cacheNames.lenght in each CacheManager (numMembersInCluster cacheManagers).
    * 
    * @param numMembersInCluster
    * @param defaultConfigBuilder
    * @param cacheNames
    * @return A list with size numMembersInCluster containing a list of cacheNames.length caches
    */
   protected <K, V> List<List<Cache<K, V>>> createClusteredCaches(int numMembersInCluster,
         ConfigurationBuilder defaultConfigBuilder, String[] cacheNames) {
      List<List<Cache<K, V>>> allCaches = new ArrayList<List<Cache<K, V>>>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultConfigBuilder);
         List<Cache<K, V>> currentCacheManagerCaches = new ArrayList<Cache<K, V>>(cacheNames.length);

         for (String cacheName : cacheNames) {
            Cache<K, V> cache = cm.getCache(cacheName);
            currentCacheManagerCaches.add(cache);
         }
         allCaches.add(currentCacheManagerCaches);
      }
      waitForClusterToForm(cacheNames);
      return allCaches;
   }

   protected ReplListener replListener(Cache cache) {
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

   protected Address address(Cache c) {
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
      List<Cache<Object, Object>> caches = caches();
      caches.remove(cacheIndex);
      manager(cacheIndex).stop();
      cacheManagers.remove(cacheIndex);
      TestingUtil.blockUntilViewsReceived(60000, false, caches);
      TestingUtil.waitForRehashToComplete(caches);
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

   protected Object getKeyForCache(Cache cache) {
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
      assertNotLocked(cache(cacheIndex), key);
   }

   protected void assertLocked(int cacheIndex, Object key) {
      assertLocked(cache(cacheIndex), key);
   }

   protected boolean checkLocked(int index, Object key) {
      return checkLocked(cache(index), key);
   }

   protected Cache getLockOwner(Object key) {
      return getLockOwner(key, null);
   }

   protected Cache getLockOwner(Object key, String cacheName) {
      Configuration c = getCache(0, cacheName).getConfiguration();
      if (c.getCacheMode().isReplicated() || c.getCacheMode().isInvalidation()) {
         return getCache(0, cacheName); //for replicated caches only the coordinator acquires lock
      }  else {
         if (!c.getCacheMode().isDistributed()) throw new IllegalStateException("This is not a clustered cache!");
         final Address address = getCache(0, cacheName).getAdvancedCache().getDistributionManager().locate(key).get(0);
         for (Cache cache : caches(cacheName)) {
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
      final Cache lockOwner = getLockOwner(key, cacheName);
      assert checkLocked(lockOwner, key);
      for (Cache c : caches(cacheName)) {
         if (c != lockOwner)
            assert !checkLocked(c, key) : "Key " + key + " is locked on cache " + c + " (" + cacheName
                  + ") and it shouldn't";
      }
   }

   private Cache getCache(int index, String name) {
      return name == null ? cache(index) : cache(index, name);
   }

   protected void forceTwoPhase(int cacheIndex) throws SystemException, RollbackException {
      TransactionManager tm = tm(cacheIndex);
      Transaction tx = tm.getTransaction();
      tx.enlistResource(new XAResourceAdapter());
   }

   protected void assertNoTransactions() {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (int i = 0; i < caches().size(); i++) {
               int localTxCount = transactionTable(i).getLocalTxCount();
               int remoteTxCount = transactionTable(i).getRemoteTxCount();
               if (localTxCount != 0 || remoteTxCount != 0) {
                  log.tracef("Local tx=%s, remote tx=%s, for cache %s ",
                        localTxCount, remoteTxCount, i);
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
                  ? value == cache(cacheIndex).get(key)
                  : value.equals(cache(cacheIndex).get(key));
         }
      });
   }

}
