package org.infinispan.jupiter;

import static org.infinispan.commons.test.TestResourceTracker.getCurrentTestShortName;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

// TODO how to replace InCacheMode? Most uses only have a single cache type, so should be easy
// Just use @ParameterizedTest and @ArgumentsSource?

// @InTransactionMode should be simpler as it always seems to be set to TRANSACTIONAL
public abstract class MultipleCacheManagersTest extends AbstractCacheTest {

   protected List<EmbeddedCacheManager> cacheManagers = Collections.synchronizedList(new ArrayList<>());
   protected IdentityHashMap<Cache<?, ?>, ReplListener> listeners = new IdentityHashMap<>();
   // the cache mode set in configuration is shared in many tests, therefore we'll place the field,
   // fluent setter cacheMode(...) and parameters() to this class.
   protected CacheMode cacheMode;
   protected Boolean transactional;
   protected LockingMode lockingMode;
   protected IsolationLevel isolationLevel;
   // Disables the triangle algorithm if set to Boolean.FALSE
   protected Boolean useTriangle;
   protected StorageType storageType;

   @BeforeAll
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

   @BeforeEach
   public void createBeforeMethod() throws Throwable {
      if (cleanupAfterMethod()) callCreateCacheManagers();
   }

   @AfterAll
   protected void destroy() {
      if (cleanupAfterTest()) {
         TestingUtil.clearContent(cacheManagers);
         TestingUtil.killCacheManagers(cacheManagers);
      }

      if (cacheManagers != null) {
         for (EmbeddedCacheManager cm : cacheManagers) {
            String nodeName = SecurityActions.getCacheManagerConfiguration(cm).transport().nodeName();
            assertTrue(nodeName != null && nodeName.contains(getCurrentTestShortName()),
                  () -> "Invalid node name for test " + getCurrentTestShortName() + ": " + nodeName);
         }

         cacheManagers.clear();
      }

      listeners.clear();
   }

   @AfterEach
   protected void clearContent() throws Throwable {
      if (cleanupAfterTest()) {
         log.debug("*** Test method complete; clearing contents on all caches.");
         TestingUtil.clearContent(cacheManagers);
      } else {
         TestingUtil.clearContent(cacheManagers);
         TestingUtil.killCacheManagers(cacheManagers);
         threadExt.cleanUpResources();
         cacheManagers.clear();
      }
   }

   final protected void registerCacheManager(CacheContainer... cacheContainers) {
      for (CacheContainer ecm : cacheContainers) {
         this.cacheManagers.add((EmbeddedCacheManager) ecm);
      }
   }

   final protected void registerCacheManager(List<? extends EmbeddedCacheManager> cacheContainers) {
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
      EmbeddedCacheManager cm = createClusteredCacheManager(false, defaultGlobalConfigurationBuilder(),
            null, flags);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);
      cm.start();
      return cm;
   }

   /**
    * Allows a test to manipulate a cache manager before it is started. Does nothing by default
    */
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      // Do nothing
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

   protected EmbeddedCacheManager addClusterEnabledCacheManager(SerializationContextInitializer sci) {
      return addClusterEnabledCacheManager(sci, null, new TransportFlags());
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(SerializationContextInitializer sci, ConfigurationBuilder defaultConfig) {
      return addClusterEnabledCacheManager(sci, defaultConfig, new TransportFlags());
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(SerializationContextInitializer sci,
                                                                ConfigurationBuilder defaultConfig, TransportFlags flags) {
      GlobalConfigurationBuilder globalBuilder = defaultGlobalConfigurationBuilder();
      if (sci != null) globalBuilder.serialization().addContextInitializer(sci);
      return addClusterEnabledCacheManager(globalBuilder, defaultConfig, flags);
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
      EmbeddedCacheManager cm = createClusteredCacheManager(false, defaultGlobalConfigurationBuilder(),
            builder, flags);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);
      cm.start();
      return cm;
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilderHolder builderHolder) {
      EmbeddedCacheManager cm = createClusteredCacheManager(false, builderHolder);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);
      cm.start();
      return cm;
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(GlobalConfigurationBuilder globalBuilder,
                                                                ConfigurationBuilder builder, TransportFlags flags) {
      EmbeddedCacheManager cm = createClusteredCacheManager(false, globalBuilder, builder, flags);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);
      cm.start();
      return cm;
   }

   protected GlobalConfigurationBuilder defaultGlobalConfigurationBuilder() {
      return GlobalConfigurationBuilder.defaultClusteredBuilder();
   }

   protected void createCluster(int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager();
   }

   protected void createCluster(ConfigurationBuilder builder, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(builder);
   }

   protected void createCluster(SerializationContextInitializer sci, ConfigurationBuilder builder, int count) {
      for (int i = 0; i < count; i++) addClusterEnabledCacheManager(sci, builder);
   }

   /**
    * Allows multiple configurations to be defined for a cache manager before it is started, using the supplied
    * {@link ConfigurationBuilderHolder}.  These cannot be shared per node so this method doesn't allow the user to make
    * the mistake and instead will give you one instance per node.
    * <p>
    * This method will wait until all nodes are up before returning
    * @param consumer consumer to configure the caches
    * @param count how many nodes to bring up
    */
   protected void createCluster(Consumer<ConfigurationBuilderHolder> consumer, int count) {
      for (int i = 0; i < count; ++i) {
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
         holder.getGlobalConfigurationBuilder().clusteredDefault();
         consumer.accept(holder);
         addClusterEnabledCacheManager(holder);
      }
      waitForClusterToForm();
   }

   protected void createCluster(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder, int count) {
      for (int i = 0; i < count; i++)
         addClusterEnabledCacheManager(new GlobalConfigurationBuilder().read(globalBuilder.build()), builder);
   }

   protected void defineConfigurationOnAllManagers(String cacheName, ConfigurationBuilder b) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration(cacheName, b.build());
      }
   }

   protected <K, V> List<Cache<K, V>> getCaches(String cacheName) {
      List<Cache<K, V>> caches = new ArrayList<>();
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
         TestingUtil.waitForNoRebalance(caches);
      }
   }

   protected void waitForClusterToForm() {
      waitForClusterToForm((String) null);
   }

   protected void waitForClusterToForm(String... names) {
      if (names != null && names.length != 0) {
         for (String name : names) {
            waitForClusterToForm(name);
         }
      } else {
         waitForClusterToForm();
      }
   }

   protected TransactionManager tm(Cache<?, ?> c) {
      return c.getAdvancedCache().getTransactionManager();
   }

   protected TransactionManager tm(int i, String cacheName) {
      return cache(i, cacheName).getAdvancedCache().getTransactionManager();
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

   protected void createClusteredCaches(int numMembersInCluster, String cacheName, ConfigurationBuilder builder) {
      createClusteredCaches(numMembersInCluster, cacheName, null, builder);
   }

   protected void createClusteredCaches(int numMembersInCluster, String cacheName, SerializationContextInitializer sci,
                                        ConfigurationBuilder builder) {
      createClusteredCaches(numMembersInCluster, cacheName, sci, builder, new TransportFlags());
   }

   protected void createClusteredCaches(int numMembersInCluster, String cacheName, ConfigurationBuilder builder, TransportFlags flags) {
      createClusteredCaches(numMembersInCluster, null, builder, flags, cacheName);
   }

   protected void createClusteredCaches(
         int numMembersInCluster, String cacheName, SerializationContextInitializer sci, ConfigurationBuilder builder, TransportFlags flags) {
      createClusteredCaches(numMembersInCluster, sci, builder, flags, cacheName);
   }

   protected void createClusteredCaches(int numMembersInCluster,
                                        SerializationContextInitializer sci,
                                        ConfigurationBuilder defaultConfigBuilder) {
      createClusteredCaches(numMembersInCluster, sci, defaultConfigBuilder, new TransportFlags());
   }

   protected void createClusteredCaches(int numMembersInCluster,
                                        SerializationContextInitializer sci,
                                        ConfigurationBuilder configBuilder,
                                        TransportFlags flags, String... cacheNames) {
      GlobalConfigurationBuilder globalBuilder = defaultGlobalConfigurationBuilder();
      if (sci != null) globalBuilder.serialization().addContextInitializer(sci);
      createClusteredCaches(numMembersInCluster, globalBuilder, configBuilder, false, flags, cacheNames);
   }

   protected void createClusteredCaches(int numMembersInCluster,
                                        GlobalConfigurationBuilder globalConfigurationBuilder,
                                        ConfigurationBuilder defaultConfigBuilder,
                                        boolean serverMode, String... cacheNames) {
      createClusteredCaches(numMembersInCluster, globalConfigurationBuilder, defaultConfigBuilder, serverMode,
            new TransportFlags(), cacheNames);
   }

   protected void createClusteredCaches(int numMembersInCluster,
                                        GlobalConfigurationBuilder globalConfigurationBuilder,
                                        ConfigurationBuilder configBuilder,
                                        boolean serverMode, TransportFlags flags,
                                        String... cacheNames) {
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm;
         GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
         global.read(globalConfigurationBuilder.build());
         if (serverMode) {
            global.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
            global.transport().defaultTransport();
         }
         ConfigurationBuilder defaultBuilder = null;
         if (cacheNames.length == 0 || global.defaultCacheName().isPresent()) {
            defaultBuilder = configBuilder;
         }

         cm = addClusterEnabledCacheManager(global, defaultBuilder, flags);

         if (cacheNames.length == 0) {
            cm.getCache();
         } else {
            for (String cacheName : cacheNames) {
               // The default cache was already defined
               if (!global.defaultCacheName().orElse("").equals(cacheName)) {
                  cm.defineConfiguration(cacheName, configBuilder.build());
               }
            }
         }
      }
      waitForClusterToForm(cacheNames);
   }

   protected void createClusteredCaches(int numMembersInCluster,
                                        ConfigurationBuilder defaultConfigBuilder,
                                        boolean serverMode, String... cacheNames) {
      createClusteredCaches(numMembersInCluster, defaultGlobalConfigurationBuilder(),
            defaultConfigBuilder, serverMode, cacheNames);
   }

   protected void createClusteredCaches(int numMembersInCluster,
                                        ConfigurationBuilder defaultConfigBuilder) {
      createClusteredCaches(numMembersInCluster, defaultConfigBuilder, false);
   }

   protected void createClusteredCaches(int numMembersInCluster,
                                        ConfigurationBuilder defaultConfig,
                                        TransportFlags flags) {
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(defaultConfig, flags);
         cm.getCache();
      }
      waitForClusterToForm();
   }

   /**
    * Create cacheNames.length in each CacheManager (numMembersInCluster cacheManagers).
    */
   protected void createClusteredCaches(int numMembersInCluster,
                                        ConfigurationBuilder defaultConfigBuilder, String... cacheNames) {
      createClusteredCaches(numMembersInCluster, defaultConfigBuilder, new TransportFlags(), cacheNames);
   }

   protected void createClusteredCaches(int numMembersInCluster, ConfigurationBuilder configBuilder,
                                        TransportFlags transportFlags, String... cacheNames) {
      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(null, transportFlags);

         for (String cacheName : cacheNames) {
            cm.defineConfiguration(cacheName, configBuilder.build());
            cm.getCache(cacheName);
         }
      }
      waitForClusterToForm(cacheNames);
   }

   protected ReplListener replListener(Cache<?, ?> cache) {
      return listeners.computeIfAbsent(cache, k -> new ReplListener(cache));
   }

   protected EmbeddedCacheManager[] managers() {
      return cacheManagers.toArray(new EmbeddedCacheManager[0]);
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

   protected <K, V> DataContainer<K, V> dataContainer(int index) {
      return this.<K, V>advancedCache(index).getDataContainer();
   }

   public MultipleCacheManagersTest cacheMode(CacheMode cacheMode) {
      this.cacheMode = cacheMode;
      return this;
   }

   public MultipleCacheManagersTest transactional(boolean transactional) {
      this.transactional = transactional;
      return this;
   }

   public MultipleCacheManagersTest lockingMode(LockingMode lockingMode) {
      this.lockingMode = lockingMode;
      return this;
   }

   public MultipleCacheManagersTest isolationLevel(IsolationLevel isolationLevel) {
      this.isolationLevel = isolationLevel;
      return this;
   }

   public TransactionMode transactionMode() {
      return transactional ? TransactionMode.TRANSACTIONAL : TransactionMode.NON_TRANSACTIONAL;
   }

   public MultipleCacheManagersTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
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
      return this.<A, B>cache(i).getAdvancedCache();
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

   protected LocalizedCacheTopology cacheTopology(int i) {
      return TestingUtil.extractCacheTopology(cache(i));
   }

   protected LocalizedCacheTopology cacheTopology(int i, String cacheName) {
      return TestingUtil.extractCacheTopology(cache(i, cacheName));
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
      killMember(cacheIndex, cacheName, true);
   }

   protected void killMember(int cacheIndex, String cacheName, boolean awaitRehash) {
      List<Cache<Object, Object>> caches = caches(cacheName);
      caches.remove(cacheIndex);
      manager(cacheIndex).stop();
      cacheManagers.remove(cacheIndex);
      if (awaitRehash && caches.size() > 0) {
         TestingUtil.blockUntilViewsReceived(60000, false, caches);
         TestingUtil.waitForNoRebalance(caches);
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

   @SuppressWarnings("unchecked")
   protected <K> Supplier<K> supplyKeyForCache(int nodeIndex, String cacheName) {
      return () -> (K) getKeyForCache(nodeIndex, cacheName);
   }

   protected MagicKey getKeyForCache(Cache<?, ?> cache) {
      return new MagicKey(cache);
   }

   protected MagicKey getKeyForCache(Cache<?, ?> primary, Cache<?, ?>... backup) {
      return new MagicKey(primary, backup);
   }

   protected void assertNotLocked(final String cacheName, final Object key) {
      eventually(() -> {
         boolean aNodeIsLocked = false;
         for (int i = 0; i < caches(cacheName).size(); i++) {
            final boolean isLocked = lockManager(i, cacheName).isLocked(key);
            if (isLocked)
               log.trace(key + " is locked on cache index " + i + " by " + lockManager(i, cacheName).getOwner(key));
            aNodeIsLocked = aNodeIsLocked || isLocked;
         }
         return !aNodeIsLocked;
      });
   }

   protected void assertNotLocked(final Object key) {
      assertNotLocked((String)null, key);
   }

   protected boolean checkTxCount(int cacheIndex, int localTx, int remoteTx) {
      final int localTxCount = getLocalTxCount(cacheIndex);
      final int remoteTxCount = getRemoteTxCount(cacheIndex);
      log.tracef("Cache index %s, local tx %4s, remote tx %4s \n", cacheIndex, localTxCount, remoteTxCount);
      return localTxCount == localTx && remoteTxCount == remoteTx;
   }

   protected int getRemoteTxCount(int cacheIndex) {
      return TestingUtil.getTransactionTable(cache(cacheIndex)).getRemoteTxCount();
   }

   protected int getLocalTxCount(int cacheIndex) {
      return TestingUtil.getTransactionTable(cache(cacheIndex)).getLocalTxCount();
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
      if (c.clustering().cacheMode().isInvalidation()) {
         return getCache(0, cacheName); //for replicated caches only the coordinator acquires lock
      } else if (!c.clustering().cacheMode().isClustered()) {
         throw new IllegalStateException("This is not a clustered cache!");
      } else {
         Address address = getCache(0, cacheName).getAdvancedCache().getDistributionManager().getCacheTopology()
               .getDistribution(key).primary();
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
      for (Cache<?, ?> c : caches(cacheName)) {
         if (c != lockOwner) {
            assertNotLocked(c, key);
         } else {
            assertLocked(c, key);
         }
      }
   }

   private <K, V> Cache<K, V> getCache(int index, String name) {
      return name == null ? this.cache(index) : this.cache(index, name);
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
      eventually("There are pending transactions!", () -> {
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
      });
   }

   protected TransactionTable transactionTable(int cacheIndex) {
      return TestingUtil.extractComponent(cache(cacheIndex), TransactionTable.class);
   }

   protected void assertEventuallyEquals(
         final int cacheIndex, final Object key, final Object value) {
      eventually(() -> value == null
            ? null == cache(cacheIndex).get(key)
            : value.equals(cache(cacheIndex).get(key)));
   }

   public MultipleCacheManagersTest useTriangle(boolean useTriangle) {
      this.useTriangle = useTriangle;
      return this;
   }
}
