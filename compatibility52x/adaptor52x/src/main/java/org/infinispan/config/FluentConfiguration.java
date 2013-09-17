
package org.infinispan.config;

import org.infinispan.commons.hash.Hash;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.group.Group;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;
import org.infinispan.util.concurrent.IsolationLevel;

import java.util.List;
import java.util.Properties;

/**
 * Fluent configuration base class.
 *
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Deprecated
public class FluentConfiguration extends AbstractFluentConfigurationBean {

   public FluentConfiguration(Configuration config) {
      setConfiguration(config);
   }

   /**
    * Defines the local, in-VM locking and concurrency characteristics of the cache.
    */
   @Deprecated public static interface LockingConfig extends FluentTypes {
      /**
       * Maximum time to attempt a particular lock acquisition
       *
       * @param lockAcquisitionTimeout
       */
      LockingConfig lockAcquisitionTimeout(Long lockAcquisitionTimeout);

      /**
       * Cache isolation level. Infinispan only supports READ_COMMITTED or REPEATABLE_READ isolation
       * levels. See <a href=
       * 'http://en.wikipedia.org/wiki/Isolation_level'>http://en.wikipedia.org/wiki/Isolation_level</a>
       * for a discussion on isolation levels.
       *
       * @param isolationLevel
       */
      LockingConfig isolationLevel(IsolationLevel isolationLevel);

      /**
       * This setting is only applicable in the case of REPEATABLE_READ. When write skew check is
       * set to false, if the writer at commit time discovers that the working entry and the
       * underlying entry have different versions, the working entry will overwrite the underlying
       * entry. If true, such version conflict - known as a write-skew - will throw an Exception.
       *
       * @param writeSkewCheck
       */
      LockingConfig writeSkewCheck(Boolean writeSkewCheck);

      /**
       * If true, a pool of shared locks is maintained for all entries that need to be locked.
       * Otherwise, a lock is created per entry in the cache. Lock striping helps control memory
       * footprint but may reduce concurrency in the system.
       *
       * @param useLockStriping
       */
      LockingConfig useLockStriping(Boolean useLockStriping);

      /**
       * Concurrency level for lock containers. Adjust this value according to the number of
       * concurrent threads interacting with Infinispan. Similar to the concurrencyLevel tuning
       * parameter seen in the JDK's ConcurrentHashMap.
       *
       * @param concurrencyLevel
       */
      LockingConfig concurrencyLevel(Integer concurrencyLevel);
   }

   /**
    * Holds the configuration for cache loaders and stores.
    */
   @Deprecated public static interface LoadersConfig extends FluentTypes {
      /**
       * If true, when the cache starts, data stored in the cache store will be pre-loaded into memory.
       * This is particularly useful when data in the cache store will be needed immediately after
       * startup and you want to avoid cache operations being delayed as a result of loading this data
       * lazily. Can be used to provide a 'warm-cache' on startup, however there is a performance
       * penalty as startup time is affected by this process.
       *
       * @param preload
       */
      LoadersConfig preload(Boolean preload);

      /**
       * If true, data is only written to the cache store when it is evicted from memory, a phenomenon
       * known as 'passivation'. Next time the data is requested, it will be 'activated' which means
       * that data will be brought back to memory and removed from the persistent store. This gives you
       * the ability to 'overflow' to disk, similar to swapping in an operating system. <br />
       * <br />
       * If false, the cache store contains a copy of the contents in memory, so writes to cache result
       * in cache store writes. This essentially gives you a 'write-through' configuration.
       *
       * @param passivation
       */
      LoadersConfig passivation(Boolean passivation);

      /**
       * This setting should be set to true when multiple cache instances share the same cache store
       * (e.g., multiple nodes in a cluster using a JDBC-based CacheStore pointing to the same, shared
       * database.) Setting this to true avoids multiple cache instances writing the same modification
       * multiple times. If enabled, only the node where the modification originated will write to the
       * cache store. <br />
       * <br />
       * If disabled, each individual cache reacts to a potential remote update by storing the data to
       * the cache store. Note that this could be useful if each individual node has its own cache
       * store - perhaps local on-disk.
       *
       * @param shared
       */
      LoadersConfig shared(Boolean shared);

      LoadersConfig addCacheLoader(CacheLoaderConfig... configs);
   }

   /**
    * Defines transactional (JTA) characteristics of the cache.
    */
   @Deprecated public static interface TransactionConfig extends FluentTypes {
      /**
       * Fully qualified class name of a class that looks up a reference to a
       * {@link javax.transaction.TransactionManager}. The default provided is capable of locating
       * the default TransactionManager in most popular Java EE systems, using a JNDI lookup.
       *
       * @param transactionManagerLookupClass
       */
      TransactionConfig transactionManagerLookupClass(Class<? extends TransactionManagerLookup> transactionManagerLookupClass);

      /**
       * Configure Transaction manager lookup directly using an instance of TransactionManagerLookup. Calling this
       * method marks the cache as transactional.
       *
       * @param transactionManagerLookup instance to use as lookup
       * @return this TransactionConfig
       */
      TransactionConfig transactionManagerLookup(TransactionManagerLookup transactionManagerLookup);

      /**
       * Configure Transaction Synchronization Registry lookup directly using an instance of TransactionManagerLookup.
       * Calling this method marks the cache as transactional.
       *
       * @param transactionSynchronizationRegistryLookup instance to use as lookup
       * @return this TransactionConfig
       */
      TransactionConfig transactionSynchronizationRegistryLookup(TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup);

      /**
       * If true, the cluster-wide commit phase in two-phase commit (2PC) transactions will be
       * synchronous, so Infinispan will wait for responses from all nodes to which the commit was
       * sent. Otherwise, the commit phase will be asynchronous. Keeping it as false improves
       * performance of 2PC transactions, since any remote failures are trapped during the prepare
       * phase anyway and appropriate rollbacks are issued.
       *
       * @param syncCommitPhase
       */
      TransactionConfig syncCommitPhase(Boolean syncCommitPhase);

      /**
       * If true, the cluster-wide rollback phase in two-phase commit (2PC) transactions will be
       * synchronous, so Infinispan will wait for responses from all nodes to which the rollback was
       * sent. Otherwise, the rollback phase will be asynchronous. Keeping it as false improves
       * performance of 2PC transactions.
       *
       * @param syncRollbackPhase
       */
      TransactionConfig syncRollbackPhase(Boolean syncRollbackPhase);

      /**
       * Only has effect for DIST mode and when useEagerLocking is set to true. When this is
       * enabled, then only one node is locked in the cluster, disregarding numOwners config. On the
       * opposite, if this is false, then on all cache.lock() calls numOwners RPCs are being
       * performed. The node that gets locked is the main data owner, i.e. the node where data would
       * reside if numOwners==1. If the node where the lock resides crashes, then the transaction is
       * marked for rollback - data is in a consistent state, no fault tolerance.
       *
       * Note: Starting with infinispan 5.1 eager locking is replaced with pessimistic locking and can
       * be enforced by setting transaction's locking mode to PESSIMISTIC.
       *
       * @param useEagerLocking
       * @deprecated
       * @see  Configuration#getTransactionLockingMode()
       */
      @Deprecated
      TransactionConfig useEagerLocking(Boolean useEagerLocking);

      /**
       * Only has effect for DIST mode and when useEagerLocking is set to true. When this is
       * enabled, then only one node is locked in the cluster, disregarding numOwners config. On the
       * opposite, if this is false, then on all cache.lock() calls numOwners RPCs are being
       * performed. The node that gets locked is the main data owner, i.e. the node where data would
       * reside if numOwners==1. If the node where the lock resides crashes, then the transaction is
       * marked for rollback - data is in a consistent state, no fault tolerance.
       *
       * @param eagerLockSingleNode
       * @deprecated starting with Infinispan 5.1 single node locking is used by default
       */
      @Deprecated
      TransactionConfig eagerLockSingleNode(Boolean eagerLockSingleNode);

      /**
       * If there are any ongoing transactions when a cache is stopped, Infinispan waits for ongoing
       * remote and local transactions to finish. The amount of time to wait for is defined by the
       * cache stop timeout. It is recommended that this value does not exceed the transaction
       * timeout because even if a new transaction was started just before the cache was stopped,
       * this could only last as long as the transaction timeout allows it.
       */
      TransactionConfig cacheStopTimeout(Integer cacheStopTimeout);

      /**
       * This method allows configuration of the transaction recovery cache.
       * When this method is called, it automatically enables recovery. So,
       * if you want it to be disabled, make sure you call
       * {@link org.infinispan.config.FluentConfiguration.RecoveryConfig#disable()}
       */
      RecoveryConfig recovery();

      TransactionConfig useSynchronization(Boolean useSynchronization);

      /**
       * Configures whether the cache uses optimistic or pessimistic locking. If the cache is not transactional then
       * the locking mode is ignored.
       * @see org.infinispan.config.Configuration#isTransactionalCache()
       */
      TransactionConfig lockingMode(LockingMode lockingMode);

      /**
       * Configures whether the cache is transactional or not.
       * @see TransactionMode
       */
      TransactionConfig transactionMode(TransactionMode transactionMode);

      /**
       * @see org.infinispan.config.Configuration#isTransactionAutoCommit().
       */
      TransactionConfig autoCommit(boolean enabled);

      /**
       * This configuration option was added for the following situation:
       * - pre 5.1 code is using the cache
       */
      Configuration.TransactionType use1PcForAutoCommitTransactions(boolean b);
   }

   /**
    * Defines recovery configuration for the cache.
    */
   @Deprecated public static interface RecoveryConfig extends TransactionConfig {

      RecoveryConfig disable();

      /**
       * Sets the name of the cache where recovery related information is held. If not specified defaults to
       * a cache named {@link Configuration.RecoveryType#DEFAULT_RECOVERY_INFO_CACHE}
       */
      RecoveryConfig recoveryInfoCacheName(String cacheName);
   }

   /**
    * Configures deadlock detection.
    */
   @Deprecated public static interface DeadlockDetectionConfig extends FluentTypes {

      DeadlockDetectionConfig disable();

      /**
       * Time period that determines how often is lock acquisition attempted within maximum time
       * allowed to acquire a particular lock
       *
       * @param duration
       */
      DeadlockDetectionConfig spinDuration(Long duration);
   }

   /**
    * Configures custom interceptors to be added to the cache.
    */
   @Deprecated public static interface CustomInterceptorsConfig extends FluentTypes, CustomInterceptorCumulator {
   }

   /**
    * Enables addition of several customer interceptors
    */
   @Deprecated public static interface CustomInterceptorCumulator {
      CustomInterceptorPosition add(CommandInterceptor interceptor);
   }

   /**
    * Configures the location of a specific custom interceptor
    */
   @Deprecated public static interface CustomInterceptorPosition {
      CustomInterceptorsConfig first();

      CustomInterceptorsConfig last();

      CustomInterceptorsConfig atIndex(int index);

      CustomInterceptorsConfig after(Class<? extends CommandInterceptor> interceptorClass);

      CustomInterceptorsConfig before(Class<? extends CommandInterceptor> interceptorClass);
   }

   /**
    * Controls the eviction settings for the cache.
    */
@Deprecated public interface EvictionConfig extends FluentTypes {
      /**
       * Eviction strategy. Available options are 'UNORDERED', 'FIFO', 'LRU', 'LIRS' and 'NONE' (to disable
       * eviction).
       *
       * @param strategy
       */
      EvictionConfig strategy(EvictionStrategy strategy);

      /**
       * Threading policy for eviction.
       *
       * @param threadPolicy
       */
      EvictionConfig threadPolicy(EvictionThreadPolicy threadPolicy);

      /**
       * Maximum number of entries in a cache instance. If selected value is not a power of two the
       * actual value will default to the least power of two larger than selected value. -1 means no
       * limit.
       *
       * @param maxEntries
       */
      EvictionConfig maxEntries(Integer maxEntries);
   }

   /**
    * Controls the default expiration settings for entries in the cache.
    */
   @Deprecated public static interface ExpirationConfig extends FluentTypes {
      /**
       * Maximum lifespan of a cache entry, after which the entry is expired cluster-wide, in
       * milliseconds. -1 means the entries never expire. 
       * 
       * Note that this can be overridden on a per-entry basis by using the Cache API.
       *
       * @param lifespan
       */
      ExpirationConfig lifespan(Long lifespan);

      /**
       * Maximum idle time a cache entry will be maintained in the cache, in milliseconds. If the
       * idle time is exceeded, the entry will be expired cluster-wide. -1 means the entries never
       * expire. 
       * 
       * Note that this can be overridden on a per-entry basis by using the Cache API.
       *
       * @param maxIdle
       */
      ExpirationConfig maxIdle(Long maxIdle);

      /**
       * Interval (in milliseconds) between subsequent runs to purge expired
       * entries from memory and any cache stores. If you wish to disable the
       * periodic eviction process altogether, set wakeupInterval to -1.
       *
       * @param wakeUpInterval
       */
      ExpirationConfig wakeUpInterval(Long wakeUpInterval);

      /**
       * Sets whether the background reaper thread is enabled to test entries for expiration.  Regardless of whether
       * a reaper is used, entries are tested for expiration lazily when they are touched.
       * @param enabled whether a reaper thread is used or not
       */
      ExpirationConfig reaperEnabled(Boolean enabled);
   }

   /**
    * Defines clustered characteristics of the cache.
    */
   @Deprecated public static interface ClusteringConfig extends FluentTypes {
      /**
       * Cache mode. For distribution, set mode to either 'd', 'dist' or 'distribution'. For
       * replication, use either 'r', 'repl' or 'replication'. Finally, for invalidation, 'i', 'inv'
       * or 'invalidation'. If the cache mode is set to 'l' or 'local', the cache in question will
       * not support clustering even if its cache manager does.
       */
      ClusteringConfig mode(Configuration.CacheMode mode);

      /**
       * Configure async sub element. Once this method is invoked users cannot subsequently invoke
       * <code>configureSync()</code> as two are mutually exclusive
       *
       * @return AsyncConfig element
       */
      AsyncConfig async();

      /**
       * Configure sync sub element. Once this method is invoked users cannot subsequently invoke
       * <code>configureAsync()</code> as two are mutually exclusive
       *
       * @return SyncConfig element
       */
      SyncConfig sync();

      /**
       * Configure stateRetrieval sub element
       *
       * @return StateRetrievalConfig element
       */
      StateRetrievalConfig stateRetrieval();

      /**
       * This method allows configuration of the L1 cache for distributed
       * caches. When this method is called, it automatically enables L1. So,
       * if you want it to be disabled, make sure you call
       * {@link org.infinispan.config.FluentConfiguration.L1Config#disable()}
       */
      L1Config l1();

      /**
       * * Configure hash sub element
       *
       * @return HashConfig element
       */
      HashConfig hash();
   }

   /**
    * If configured all communications are asynchronous, in that whenever a thread sends a message
    * sent over the wire, it does not wait for an acknowledgment before returning. AsyncConfig is
    * mutually exclusive with the SyncConfig
    */
@Deprecated public interface AsyncConfig extends ClusteringConfig {
      /**
       * If true, this forces all async communications to be queued up and sent out periodically as
       * a batch.
       *
       * @param useReplQueue
       */
      AsyncConfig useReplQueue(Boolean useReplQueue);

      /**
       * If useReplQueue is set to true, this attribute can be used to trigger flushing of the queue
       * when it reaches a specific threshold.
       *
       * @param replQueueMaxElements
       */
      AsyncConfig replQueueMaxElements(Integer replQueueMaxElements);

      /**
       * If useReplQueue is set to true, this attribute controls how often the asynchronous thread
       * used to flush the replication queue runs. This should be a positive integer which
       * represents thread wakeup time in milliseconds.
       *
       * @param replQueueInterval
       */
      AsyncConfig replQueueInterval(Long replQueueInterval);

      /**
       * If true, asynchronous marshalling is enabled which means that caller can return even
       * quicker, but it can suffer from reordering of operations. You can find more information <a
       * href=&quot;http://community.jboss.org/docs/DOC-15725&quot;>here</a>
       *
       * @param asyncMarshalling
       */
      AsyncConfig asyncMarshalling(Boolean asyncMarshalling);

      /**
       * This overrides the replication queue implementation class. Overriding the default allows
       * you to add behavior to the queue, typically by subclassing the default implementation.
       *
       * @param replQueueClass
       */
      AsyncConfig replQueueClass(Class<? extends ReplicationQueue> replQueueClass);
   }

   /**
    * If configured all communications are synchronous, in that whenever a thread sends a message
    * sent over the wire, it blocks until it receives an acknowledgment from the recipient.
    * SyncConfig is mutually exclusive with the AsyncConfig.
    */
@Deprecated public interface SyncConfig extends ClusteringConfig {
      /**
       * This is the timeout used to wait for an acknowledgment when making a remote call, after
       * which the call is aborted and an exception is thrown.
       *
       * @param replTimeout
       */
      SyncConfig replTimeout(Long replTimeout);
   }

   /**
    * Configures how state is retrieved when a new cache joins the cluster.
    * Used with invalidation and replication clustered modes.
    */
@Deprecated public interface StateRetrievalConfig extends ClusteringConfig {
      /**
       * If true, this will cause the cache to ask neighboring caches for state when it starts up,
       * so the cache starts 'warm', although it will impact startup time.
       *
       * @param fetchInMemoryState
       */
      StateRetrievalConfig fetchInMemoryState(Boolean fetchInMemoryState);

      /**
       * If true, this will allow the cache to provide in-memory state to a neighbor, even if the
       * cache is not configured to fetch state from its neighbors (fetchInMemoryState is false)
       *
       * @param alwaysProvideInMemoryState
       */
      StateRetrievalConfig alwaysProvideInMemoryState(Boolean alwaysProvideInMemoryState);

      /**
       * Initial wait time when backing off before retrying state transfer retrieval
       *
       * @param initialRetryWaitTime
       */
      StateRetrievalConfig initialRetryWaitTime(Long initialRetryWaitTime);

      /**
       * Wait time increase factor over successive state retrieval backoffs
       *
       * @param retryWaitTimeIncreaseFactor
       */
      StateRetrievalConfig retryWaitTimeIncreaseFactor(Integer retryWaitTimeIncreaseFactor);

      /**
       * Number of state retrieval retries before giving up and aborting startup.
       *
       * @param numRetries
       */
      StateRetrievalConfig numRetries(Integer numRetries);

      /**
       * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
       * caches, before throwing an exception and aborting startup.
       *
       * @param timeout
       */
      StateRetrievalConfig timeout(Long timeout);

      /**
       * This is the maximum amount of time to run a cluster-wide flush, to allow for syncing of
       * transaction logs.
       *
       * @param logFlushTimeout
       */
      StateRetrievalConfig logFlushTimeout(Long logFlushTimeout);

      /**
       * This is the maximum number of non-progressing transaction log writes after which a
       * brute-force flush approach is resorted to, to synchronize transaction logs.
       *
       * @param maxNonProgressingLogWrites
       */
      StateRetrievalConfig maxNonProgressingLogWrites(Integer maxNonProgressingLogWrites);

      /**
       * Size of a state transfer chunk, in cache entries.
       */
      StateRetrievalConfig chunkSize(Integer chunkSize);
   }

   /**
    * Configures the L1 cache behavior in 'distributed' caches instances.
    * In any other cache modes, this element is ignored.
    */
@Deprecated public interface L1Config extends ClusteringConfig {
      /**
       * Maximum lifespan of an entry placed in the L1 cache.
       *
       * @param lifespan
       */
      L1Config lifespan(Long lifespan);

      /**
       * If true, entries removed due to a rehash will be moved to L1 rather than being removed
       * altogether.
       *
       * @param onRehash
       */
      L1Config onRehash(Boolean onRehash);

      L1Config disable();
      
      /**
       * <p>
       * Determines whether a multicast or a web of unicasts are used when performing L1 invalidations.
       * </p>
       * 
       * <p>
       * By default multicast will be used.
       * </p>
       * 
       * <p>
       * If the threshold is set to -1, then unicasts will always be used. If the threshold is set to 0, then multicast 
       * will be always be used.
       * </p>
       * 
       * @param threshold the threshold over which to use a multicast
       * 
       */
      L1Config invalidationThreshold(Integer threshold);

      /**
       * Determines how often a cleanup thread runs to clean up an internal log of requestors for a specific key
       * @param frequencyMillis frequency in milliseconds
       */
      L1Config cleanupTaskFrequency(Long frequencyMillis);
   }

   /**
    * Allows fine-tuning of rehashing characteristics. Only used with 'distributed' cache mode, and otherwise ignored.
    */
@Deprecated public interface HashConfig extends FluentTypes {
      /**
       * @deprecated No longer used since 5.2, use {@link org.infinispan.configuration.cache.HashConfigurationBuilder#consistentHashFactory(org.infinispan.distribution.ch.ConsistentHashFactory)} instead.
       */
      @Deprecated
      HashConfig consistentHashClass(Class<? extends ConsistentHash> consistentHashClass);

      /**
       * A fully qualified name of the class providing a hash function, used as a bit spreader and a
       * general hash code generator. Typically used in conjunction with the many default
       * {@link org.infinispan.distribution.ch.ConsistentHash} implementations shipped.
       *
       * @param hashFunctionClass
       */
      HashConfig hashFunctionClass(Class<? extends Hash> hashFunctionClass);

      /**
       * Number of cluster-wide replicas for each cache entry.
       *
       * @param numOwners
       */
      HashConfig numOwners(Integer numOwners);

      HashConfig rehashWait(Long rehashWaitTime);

      /**
       *
       * @param rehashRpcTimeout
       */
      HashConfig rehashRpcTimeout(Long rehashRpcTimeout);

      /**
       * If false, no rebalancing or rehashing will take place when a new node joins the cluster or
       * a node leaves
       *
       * @param rehashEnabled
       */
      HashConfig rehashEnabled(Boolean rehashEnabled);
      
      /**
       * @deprecated No longer used since 5.2, use {@link org.infinispan.configuration.cache.HashConfigurationBuilder#numSegments(int)} instead.
       */
      HashConfig numVirtualNodes(Integer numVirtualNodes);
      
      GroupsConfig groups();

      @Override // Override definition so that Scala classes can see it.
      Configuration build();
   }
   
@Deprecated public interface GroupsConfig extends FluentTypes {
      /**
       * Enable grouping support, such that {@link Group} annotations are honoured and any configured
       * groupers will be invoked
       * 
       * @param enabled
       * @return
       */
      GroupsConfig enabled(Boolean enabled);
      
      Boolean isEnabled();
      
      /**
       * Set the groupers currently in use
       */
      GroupsConfig groupers(List<Grouper<?>> groupers);
      
      /**
       * Get's the current groupers in use
       */
      List<Grouper<?>> getGroupers();
      
      @Override // Override definition so that Scala classes can see it.
      Configuration build();
   }

   /**
    * Configures indexing of entries in the cache for searching.
    */
@Deprecated public interface IndexingConfig extends FluentTypes {
      /**
       * If true, only index changes made locally, ignoring remote changes. This is useful if
       * indexes are shared across a cluster to prevent redundant indexing of updates.
       *
       * @param indexLocalOnly
       * @return <code>this</code>, for method chaining
       */
      IndexingConfig indexLocalOnly(Boolean indexLocalOnly);

      /**
       * Indexing is disabled by default, but using the fluent API entering the {@link FluentTypes#indexing()}
       * method enables Indexing implicitly. If needed, this method can be used to disable it.
       * @return <code>this</code>, for method chaining
       */
      IndexingConfig disable();
      
      /**
       * <p>The Query engine relies on properties for configuration.</p>
       * <p>These properties are passed directly to the embedded Hibernate Search engine, so
       * for the complete and up to date documentation about available properties
       * refer to the Hibernate Search reference of the version you're using with Infinispan Query.</p>
       * @see <a href="http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/">Hibernate Search</a>
       * @param properties
       * @return <code>this</code>, for method chaining
       */
      IndexingConfig withProperties(Properties properties);
      
      /**
       * <p>Defines a single property. Can be used multiple times to define all needed properties,
       * but the full set is overridden by {@link #withProperties(Properties)}.</p>
       * <p>These properties are passed directly to the embedded Hibernate Search engine, so
       * for the complete and up to date documentation about available properties
       * refer to the Hibernate Search reference of the version you're using with Infinispan Query.</p>
       * @see <a href="http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/">Hibernate Search</a>
       * @param key Property key
       * @param value Property value
       * @return <code>this</code>, for method chaining
       */
      IndexingConfig addProperty(String key, String value);
   }

   @Deprecated
   public static interface VersioningConfig extends FluentTypes {
      VersioningConfig enable();
      VersioningConfig disable();
      VersioningConfig versioningScheme(VersioningScheme scheme);
   }

   @Deprecated public static interface DataContainerConfig extends FluentTypes {

      DataContainerConfig dataContainerClass(Class<? extends DataContainer> dataContainerClass);

      DataContainerConfig dataContainer(DataContainer dataContainer);

      DataContainerConfig withProperties(Properties properties);

      DataContainerConfig addProperty(String key, String value);
   }

   @Deprecated public static interface UnsafeConfig extends FluentTypes {

      UnsafeConfig unreliableReturnValues(Boolean unreliableReturnValues);

   }

   @Deprecated public static interface StoreAsBinaryConfig extends FluentTypes {

      StoreAsBinaryConfig storeKeysAsBinary(Boolean storeKeysAsBinary);

      StoreAsBinaryConfig storeValuesAsBinary(Boolean storeValuesAsBinary);

      StoreAsBinaryConfig disable();

      @Override // Override definition so that Scala classes can see it.
      Configuration build();
   }

   @Deprecated public static interface JmxStatisticsConfig extends FluentTypes {}

   @Deprecated public static interface InvocationBatchingConfig extends FluentTypes {

      InvocationBatchingConfig disable();

   }
}

@Deprecated
interface FluentTypes {

   FluentConfiguration.LockingConfig locking();

   FluentConfiguration.LoadersConfig loaders();

   FluentConfiguration.TransactionConfig transaction();

   /**
    * This method allows configuration of the deadlock detection. When this
    * method is called, it automatically enables deadlock detection. So, if
    * you want it to be disabled, make sure you call
    * {@link org.infinispan.config.FluentConfiguration.DeadlockDetectionConfig#disable()}
    */
   FluentConfiguration.DeadlockDetectionConfig deadlockDetection();

   FluentConfiguration.CustomInterceptorsConfig customInterceptors();

   FluentConfiguration.EvictionConfig eviction();

   FluentConfiguration.ExpirationConfig expiration();

   FluentConfiguration.ClusteringConfig clustering();

   /**
    * This method allows configuration of the indexing subsystem. When
    * this method is called, it automatically enables indexing. So, if you
    * want it to be disabled, make sure you call
    * {@link org.infinispan.config.FluentConfiguration.IndexingConfig#disable()}
    */
   FluentConfiguration.IndexingConfig indexing();

   FluentConfiguration.DataContainerConfig dataContainer();

   FluentConfiguration.UnsafeConfig unsafe();

   /**
    * This method allows configuration of jmx statistics. When this method is
    * called, it automatically enables jmx statistics.
    */
   FluentConfiguration.JmxStatisticsConfig jmxStatistics();

   /**
    * This method allows configuration of lazy deserialization. When this
    * method is called, it automatically enables lazy deserialization.
    */
   FluentConfiguration.StoreAsBinaryConfig storeAsBinary();

   /**
    * This method allows configuration of invocation batching. When
    * this method is called, it automatically enables invocation batching.
    */
   FluentConfiguration.InvocationBatchingConfig invocationBatching();

   FluentConfiguration.VersioningConfig versioning();

   Configuration build();
}

@Deprecated
abstract class AbstractFluentConfigurationBean extends AbstractNamedCacheConfigurationBean implements FluentTypes {

   Configuration config;

   @Override
   public FluentConfiguration.LockingConfig locking() {
      return config.locking;
   }

   @Override
   public FluentConfiguration.LoadersConfig loaders() {
      return config.loaders;
   }

   @Override
   public FluentConfiguration.TransactionConfig transaction() {
      return config.transaction;
   }

   @Override
   public FluentConfiguration.DeadlockDetectionConfig deadlockDetection() {
      config.deadlockDetection.setEnabled(true);
      return config.deadlockDetection;
   }

   @Override
   public FluentConfiguration.CustomInterceptorsConfig customInterceptors() {
      return config.customInterceptors;
   }

   @Override
   public FluentConfiguration.EvictionConfig eviction() {
      return config.eviction;
   }

   @Override
   public FluentConfiguration.ExpirationConfig expiration() {
      return config.expiration;
   }

   @Override
   public FluentConfiguration.ClusteringConfig clustering() {
      return config.clustering;
   }

   @Override
   public FluentConfiguration.IndexingConfig indexing() {
      config.indexing.setEnabled(true);
      return config.indexing;
   }

   @Override
   public FluentConfiguration.DataContainerConfig dataContainer() {
      return config.dataContainer;
   }

   @Override
   public FluentConfiguration.UnsafeConfig unsafe() {
      return config.unsafe;
   }

   @Override
   public FluentConfiguration.JmxStatisticsConfig jmxStatistics() {
      return config.jmxStatistics.enabled(true);
   }

   @Override
   public FluentConfiguration.StoreAsBinaryConfig storeAsBinary() {
      return config.storeAsBinary.enabled(true);
   }

   @Override
   public FluentConfiguration.VersioningConfig versioning() {
      return config.versioning.enable();
   }

   @Override
   public FluentConfiguration.InvocationBatchingConfig invocationBatching() {
      return config.invocationBatching.enabled(true);
   }

   public FluentConfiguration.AsyncConfig async() {
      return clustering().async();
   }

   public FluentConfiguration.SyncConfig sync() {
      return clustering().sync();
   }

   public FluentConfiguration.StateRetrievalConfig stateRetrieval() {
      return clustering().stateRetrieval();
   }

   public FluentConfiguration.L1Config l1() {
      return clustering().l1();
   }

   public FluentConfiguration.HashConfig hash() {
      return clustering().hash();
   }

   public FluentConfiguration.ClusteringConfig mode(Configuration.CacheMode mode) {
      return clustering().mode(mode);
   }

   public FluentConfiguration.TransactionConfig transactionManagerLookupClass(Class<? extends TransactionManagerLookup> transactionManagerLookupClass) {
      return transaction().transactionManagerLookupClass(transactionManagerLookupClass);
   }

   public FluentConfiguration.TransactionConfig transactionManagerLookup(TransactionManagerLookup transactionManagerLookup) {
      return transaction().transactionManagerLookup(transactionManagerLookup);
   }

   public FluentConfiguration.TransactionConfig transactionSynchronizationRegistryLookup(TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup) {
      return transaction().transactionSynchronizationRegistryLookup(transactionSynchronizationRegistryLookup);
   }

   public FluentConfiguration.TransactionConfig syncCommitPhase(Boolean syncCommitPhase) {
      return transaction().syncCommitPhase(syncCommitPhase);
   }

   public FluentConfiguration.TransactionConfig syncRollbackPhase(Boolean syncRollbackPhase) {
      return transaction().syncRollbackPhase(syncRollbackPhase);
   }

   public FluentConfiguration.TransactionConfig useEagerLocking(Boolean useEagerLocking) {
      return transaction().useEagerLocking(useEagerLocking);
   }

   public FluentConfiguration.TransactionConfig eagerLockSingleNode(Boolean eagerLockSingleNode) {
      return transaction().eagerLockSingleNode(eagerLockSingleNode);
   }

   public FluentConfiguration.TransactionConfig cacheStopTimeout(Integer cacheStopTimeout) {
      return transaction().cacheStopTimeout(cacheStopTimeout);
   }

   public FluentConfiguration.TransactionConfig useSynchronization(Boolean useSynchronization) {
      return transaction().useSynchronization(useSynchronization);
   }

   public FluentConfiguration.RecoveryConfig recovery() {
      return transaction().recovery();
   }

   @Override
   public Configuration build() {
      return config;
   }

   protected AbstractFluentConfigurationBean setConfiguration(Configuration config) {
      this.config = config;
      return this;
   }

}


