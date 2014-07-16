package org.infinispan.test.arquillian;

import java.util.List;
import java.util.concurrent.Future;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * An adapter class that enables users to call all important methods from
 * {@link MultipleCacheManagersTest}, {@link AbstractCacheTest} and
 * {@link AbstractInfinispanTest}, changing their visibility to public.
 * Usage of this class is in infinispan-arquillian-container project which
 * enables injecting of this class into a test case and forming a cluster
 * of cache managers/caches.
 *
 * A few methods from super-classes changed their names, mostly because
 * they cannot be overridden. All such methods have comments on them which
 * say "name change".
 *
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 *
 */
public class DatagridManager extends MultipleCacheManagersTest
{

   @Override
   public void destroy() {
      TestingUtil.killCacheManagers(cacheManagers);
      cacheManagers.clear();
      listeners.clear();
      killSpawnedThreads();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      //empty implementation
   }

   /* ========================= AbstractInfinispanTest methods ================== */

   //name change
   public void waitForCondition(Condition ec, long timeout) {
      eventually(ec, timeout);
   }

   //name change
   public Future<?> forkThread(Runnable r) {
      return fork(r);
   }

   //name change
   public void waitForCondition(Condition ec) {
      eventually(ec);
   }

   /* =========================== AbstractCacheTest methods ====================== */

   //name change
   public boolean xorOp(boolean b1, boolean b2) {
      return xor(b1, b2);
   }

   //name change
   public void assertKeyNotLocked(Cache cache, Object key) {
      assertNotLocked(cache, key);
   }

   //name change
   public void assertKeyLocked(Cache cache, Object key) {
      assertLocked(cache, key);
   }

   //name change
   public boolean checkKeyLocked(Cache cache, Object key) {
      return checkLocked(cache, key);
   }

   /* ===================== MultipleCacheManagersTest methods ==================== */

   @Override
   public void assertSupportedConfig() {
      super.assertSupportedConfig();
   }

   //name change
   public void registerCacheManagers(CacheContainer... cacheContainers) {
      registerCacheManager(cacheContainers);
   }

   @Override
   public EmbeddedCacheManager addClusterEnabledCacheManager() {
      return super.addClusterEnabledCacheManager();
   }

   @Override
   public EmbeddedCacheManager addClusterEnabledCacheManager(TransportFlags flags) {
      return super.addClusterEnabledCacheManager(flags);
   }

   @Override
   public EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder defaultConfig) {
      return super.addClusterEnabledCacheManager(defaultConfig);
   }

   @Override
   public EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder, TransportFlags flags) {
      return super.addClusterEnabledCacheManager(builder, flags);
   }

   @Override
   public void defineConfigurationOnAllManagers(String cacheName, ConfigurationBuilder b) {
      super.defineConfigurationOnAllManagers(cacheName, b);
   }

   @Override
   public void waitForClusterToForm(String cacheName) {
      super.waitForClusterToForm(cacheName);
   }

   @Override
   public void waitForClusterToForm() {
      super.waitForClusterToForm();
   }

   @Override
   public void waitForClusterToForm(String... names) {
      super.waitForClusterToForm(names);
   }

   @Override
   public TransactionManager tm(Cache<?, ?> c) {
      return super.tm(c);
   }

   @Override
   public TransactionManager tm(int i, String cacheName) {
      return super.tm(i, cacheName);
   }

   @Override
   public TransactionManager tm(int i) {
      return super.tm(i);
   }

   @Override
   public Transaction tx(int i) {
      return super.tx(i);
   }

   @Override
   public <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, ConfigurationBuilder builder) {
      return super.createClusteredCaches(numMembersInCluster, cacheName, builder);
   }

   @Override
   public <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, ConfigurationBuilder builder, TransportFlags flags) {
      return super.createClusteredCaches(numMembersInCluster, cacheName, builder, flags);
   }

   @Override
   public ReplListener replListener(Cache cache) {
      return super.replListener(cache);
   }

   @Override
   public EmbeddedCacheManager manager(int i) {
      return super.manager(i);
   }

   @Override
   public Cache cache(int managerIndex, String cacheName) {
      return super.cache(managerIndex, cacheName);
   }

   @Override
   public void assertClusterSize(String message, int size) {
      super.assertClusterSize(message, size);
   }

   @Override
   public void removeCacheFromCluster(String cacheName) {
      super.removeCacheFromCluster(cacheName);
   }

   @Override
   public <A, B> Cache<A, B> cache(int index) {
      return super.cache(index);
   }

   @Override
   public Address address(int cacheIndex) {
      return super.address(cacheIndex);
   }

   @Override
   public AdvancedCache advancedCache(int i) {
      return super.advancedCache(i);
   }

   @Override
   public AdvancedCache advancedCache(int i, String cacheName) {
      return super.advancedCache(i, cacheName);
   }

   @Override
   public <K, V> List<Cache<K, V>> caches(String name) {
      return super.caches(name);
   }

   @Override
   public <K, V> List<Cache<K, V>> caches() {
      return super.caches();
   }

   @Override
   public Address address(Cache c) {
      return super.address(c);
   }

   @Override
   public LockManager lockManager(int i) {
      return super.lockManager(i);
   }

   @Override
   public LockManager lockManager(int i, String cacheName) {
      return super.lockManager(i, cacheName);
   }

   @Override
   public List<EmbeddedCacheManager> getCacheManagers() {
      return super.getCacheManagers();
   }

   @Override
   public void killMember(int cacheIndex) {
      super.killMember(cacheIndex);
   }

   @Override
   public Object getKeyForCache(int nodeIndex) {
      return super.getKeyForCache(nodeIndex);
   }

   @Override
   public Object getKeyForCache(int nodeIndex, String cacheName) {
      return super.getKeyForCache(nodeIndex, cacheName);
   }

   @Override
   public Object getKeyForCache(Cache cache) {
      return super.getKeyForCache(cache);
   }

   @Override
   public void assertNotLocked(final String cacheName, final Object key) {
      super.assertNotLocked(cacheName, key);
   }

   @Override
   public void assertNotLocked(final Object key) {
      super.assertNotLocked(key);
   }

   @Override
   public boolean checkTxCount(int cacheIndex, int localTx, int remoteTx) {
      return super.checkTxCount(cacheIndex, localTx, remoteTx);
   }

   @Override
   public void assertNotLocked(int cacheIndex, Object key) {
      super.assertNotLocked(cacheIndex, key);
   }

   @Override
   public void assertLocked(int cacheIndex, Object key) {
      super.assertLocked(cacheIndex, key);
   }

   @Override
   public boolean checkLocked(int index, Object key) {
      return super.checkLocked(index, key);
   }

   @Override
   public Cache getLockOwner(Object key) {
      return super.getLockOwner(key);
   }

   @Override
   public Cache getLockOwner(Object key, String cacheName) {
      return super.getLockOwner(key, cacheName);
   }

   @Override
   public void assertKeyLockedCorrectly(Object key) {
      super.assertKeyLockedCorrectly(key);
   }

   @Override
   public void assertKeyLockedCorrectly(Object key, String cacheName) {
      super.assertKeyLockedCorrectly(key, cacheName);
   }

   @Override
   public void forceTwoPhase(int cacheIndex) throws SystemException, RollbackException {
      super.forceTwoPhase(cacheIndex);
   }

   /* ========== methods simulating those from SingleCacheManagerTest ========== */

   public EmbeddedCacheManager manager() {
      return super.manager(0);
   }

   public <A, B> Cache<A, B> cache() {
      return super.cache(0);
   }

   public TransactionManager tm() {
      return super.cache(0).getAdvancedCache().getTransactionManager();
   }

   public Transaction tx() {
      try {
         return super.cache(0).getAdvancedCache().getTransactionManager().getTransaction();
      } catch (SystemException e) {
         throw new RuntimeException(e);
      }
   }

   public LockManager lockManager(String cacheName) {
      return super.lockManager(0, cacheName);
   }

   public LockManager lockManager() {
      return super.lockManager(0);
   }
}
