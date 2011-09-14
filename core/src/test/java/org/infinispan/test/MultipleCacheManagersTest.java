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
import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.RndKeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
   private IdentityHashMap<Cache, ReplListener> listeners = new IdentityHashMap<Cache, ReplListener>();

   @BeforeClass (alwaysRun = true)
   public void createBeforeClass() throws Throwable {
      if (cleanupAfterTest()) callCreateCacheManagers();
   }

   private void callCreateCacheManagers() throws Throwable {
      try {
         log.debug("Creating cache managers");
         createCacheManagers();
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
            throw new IllegalStateException("No caches registered! Use registerCacheManager(Cache... caches) do that!");
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
      return addClusterEnabledCacheManager(false);
   }

   /**
    * Creates a new cache manager, starts it, and adds it to the list of known
    * cache managers on the current thread. Uses a default clustered cache
    * manager global config.
    *
    * @param withFD boolean indicating whether the JGroups stack should be
    *               configured with failure detection.
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(boolean withFD) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(withFD);
      cacheManagers.add(cm);
      return cm;
   }

   /**
    * Creates a new optionally transactional cache manager, starts it, and adds it to the list of known cache managers on
    * the current thread.  Uses a default clustered cache manager global config.
    *
    * @param defaultConfig default cfg to use
    * @param transactional if true, the configuration will be decorated with necessary transactional settings
    * @return the new CacheManager
    */
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration defaultConfig) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(defaultConfig);
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

   protected void createCluster(Configuration.CacheMode mode, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(mode, true);
   }

   protected void defineConfigurationOnAllManagers(String cacheName, Configuration c) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(cacheName, c);
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

   protected void waitForClusterToForm(String cacheName) {
      List<Cache> caches;
      caches = getCaches(cacheName);
      Cache<Object, Object> cache = caches.get(0);
      TestingUtil.blockUntilViewsReceived(10000, caches);
      if (cache.getConfiguration().getCacheMode().isDistributed()) {
         TestingUtil.waitForInitRehashToComplete(caches);
      }
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

   protected <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, Configuration c) {
      return createClusteredCaches(numMembersInCluster, cacheName, c, false);
   }

   protected <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, Configuration c, boolean withFD) {
      List<Cache<K, V>> caches = new ArrayList<Cache<K, V>>(numMembersInCluster);
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(withFD);
         cm.defineConfiguration(cacheName, c);
         Cache<K, V> cache = cm.getCache(cacheName);
         caches.add(cache);
      }
      waitForClusterToForm(cacheName);
      return caches;
   }


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

   protected Address address(int cacheIndex) {
      return cache(cacheIndex).getAdvancedCache().getRpcManager().getAddress();
   }

   protected AdvancedCache advancedCache(int i) {
      return cache(i).getAdvancedCache();
   }

   protected AdvancedCache advancedCache(int i, String cacheName) {
      return cache(i, cacheName).getAdvancedCache();
   }

   protected <K, V> List<Cache<K, V>> caches(String name) {
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
      return TestingUtil.extractLockManager(cache(i, cacheName));
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
      TestingUtil.blockUntilViewsReceived(60000, false, caches.toArray(new Cache[0]));
   }

   /**
    * Creates a {@link org.infinispan.affinity.KeyAffinityService} and uses it for generating a key that maps to the given address.
    * @param nodeIndex the index of tha cache where to be the main data owner of the returned key
    */
   protected Object getKeyForNode(int nodeIndex) {
      final Cache<Object, Object> cache = cache(nodeIndex);
      return getKeyForCache(cache);
   }

   protected Object getKeyForCache(Cache cache) {
      ExecutorService ex = Executors.newSingleThreadExecutor();
      KeyAffinityService<Object> kas = KeyAffinityServiceFactory.newKeyAffinityService(cache, ex,
                                                                                       new RndKeyGenerator(), 5, true);
      try {
         return kas.getKeyForAddress(cache.getAdvancedCache().getAdvancedCache().getRpcManager().getAddress());
      } finally {
         ex.shutdown();
      }
   }
}
