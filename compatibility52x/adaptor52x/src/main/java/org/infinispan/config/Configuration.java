package org.infinispan.config;

import org.infinispan.CacheException;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.config.FluentConfiguration.*;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.ReplicationQueueImpl;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.infinispan.config.Configuration.CacheMode.*;

/**
 * Encapsulates the configuration of a Cache. Configures the default cache which can be retrieved via
 * CacheManager.getCache(). These default settings are also used as a starting point when configuring namedCaches, since
 * the default settings are inherited by any named cache.
 * <p />
 * @deprecated This class is deprecated.  Use {@link org.infinispan.configuration.cache.Configuration} instead.
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @see <a href="../../../config.html#ce_infinispan_default">Configuration reference</a>
 * @since 4.0
 */
@SurvivesRestarts
@XmlAccessorType(XmlAccessType.NONE)
@XmlType(propOrder = {})
@XmlRootElement(name = "namedCacheConfiguration")
@ConfigurationDoc(name = "default")
@Deprecated
@SuppressWarnings("boxing")
public class Configuration extends AbstractNamedCacheConfigurationBean {

   private static final long serialVersionUID = 5553791890144997466L;
   private static final Log log = LogFactory.getLog(Configuration.class);

   // reference to a global configuration
   @XmlTransient
   private GlobalConfiguration globalConfiguration;

   @XmlAttribute
   @ConfigurationDoc(desc = "Only used with the namedCache element, this attribute specifies the name of the cache.  Can be any String, but must be unique in a given configuration.")
   protected String name;


   // ------------------------------------------------------------------------------------------------------------
   //   CONFIGURATION OPTIONS
   // ------------------------------------------------------------------------------------------------------------

   @XmlTransient
   FluentConfiguration fluentConfig = new FluentConfiguration(this);

   @XmlTransient
   private ClassLoader cl;

   @XmlElement
   LockingType locking = new LockingType().setConfiguration(this);

   @XmlElement
   CacheLoaderManagerConfig loaders = new CacheLoaderManagerConfig().setConfiguration(this);

   @XmlElement
   TransactionType transaction = new TransactionType(null).setConfiguration(this);

   @XmlElement
   CustomInterceptorsType customInterceptors = new CustomInterceptorsType().setConfiguration(this);

   @XmlElement
   DataContainerType dataContainer = new DataContainerType().setConfiguration(this);

   @XmlElement
   EvictionType eviction = new EvictionType().setConfiguration(this);

   @XmlElement
   ExpirationType expiration = new ExpirationType().setConfiguration(this);

   @XmlElement
   UnsafeType unsafe = new UnsafeType().setConfiguration(this);

   @XmlElement
   ClusteringType clustering = new ClusteringType(LOCAL).setConfiguration(this);

   @XmlElement
   JmxStatistics jmxStatistics = new JmxStatistics().setConfiguration(this);

   @XmlElement
   StoreAsBinary storeAsBinary = new StoreAsBinary().setConfiguration(this);

   @Deprecated
   @XmlElement
   LazyDeserialization lazyDeserialization = new LazyDeserialization().setConfiguration(this);

   @XmlTransient
   InvocationBatching invocationBatching = new InvocationBatching().setConfiguration(this);

   @XmlElement
   DeadlockDetectionType deadlockDetection = new DeadlockDetectionType().setConfiguration(this);

   @XmlElement
   QueryConfigurationBean indexing = new QueryConfigurationBean().setConfiguration(this);

   @XmlElement
   VersioningConfigurationBean versioning = new VersioningConfigurationBean().setConfiguration(this);

   @SuppressWarnings("unused")
   @Start(priority = 1)
   private void correctIsolationLevels() {
      // ensure the correct isolation level upgrades and/or downgrades are performed.
      switch (locking.isolationLevel) {
         case NONE:
            if (clustering.mode.isClustered())
               locking.isolationLevel = IsolationLevel.READ_COMMITTED;
            break;
         case READ_UNCOMMITTED:
            locking.isolationLevel = IsolationLevel.READ_COMMITTED;
            break;
         case SERIALIZABLE:
            locking.isolationLevel = IsolationLevel.REPEATABLE_READ;
            break;
      }
   }

   public void applyOverrides(Configuration overrides) {
      OverrideConfigurationVisitor v1 = new OverrideConfigurationVisitor();
      this.accept(v1);
      OverrideConfigurationVisitor v2 = new OverrideConfigurationVisitor();
      overrides.accept(v2);
      v1.override(v2);
   }

   @Override
   public void inject(ComponentRegistry cr) {
      this.accept(new InjectComponentRegistryVisitor(cr));
   }

   /**
    * Use the {@link org.infinispan.configuration.cache.ConfigurationBuilder}
    * hierarchy to configure Infinispan caches fluently.
    */
   @Deprecated
   public FluentConfiguration fluent() {
      return fluentConfig;
   }

   private void setInvocationBatching(InvocationBatching invocationBatching) {
      this.invocationBatching = invocationBatching;
      this.invocationBatching.setConfiguration(this);
   }

   @XmlElement
   private InvocationBatching getInvocationBatching() {
      return invocationBatching;
   }

   // ------------------------------------------------------------------------------------------------------------
   //   SETTERS - MAKE SURE ALL SETTERS PERFORM testImmutability()!!!
   // ------------------------------------------------------------------------------------------------------------


   public GlobalConfiguration getGlobalConfiguration() {
      return globalConfiguration;
   }

   public void setGlobalConfiguration(GlobalConfiguration gc) {
      this.globalConfiguration = gc;
   }

   /**
     * Returns the name of the cache associated with this configuration.
   */
   public final String getName() {
      return name;
   }

   public ClassLoader getClassLoader() {
      if (cl != null)
         // The classloader has been set for this configuration
         return cl;
      else if (cl == null && globalConfiguration != null)
         // The classloader is not set for this configuration, and we have a global config
         return globalConfiguration.getClassLoader();
      else
         // Return the default CL
         return Thread.currentThread().getContextClassLoader();
   }

   public void setClassLoader(ClassLoader cl) {
      this.cl = cl;
   }

   public boolean isStateTransferEnabled() {
      return clustering.stateRetrieval.fetchInMemoryState || (loaders != null && loaders.isFetchPersistentState());
   }

   public long getDeadlockDetectionSpinDuration() {
      return deadlockDetection.spinDuration;
   }


   /**
    * Time period that determines how often is lock acquisition attempted within maximum time allowed to acquire a
    * particular lock
    *
    * @param eagerDeadlockSpinDuration
    * @deprecated Use {@link FluentConfiguration.DeadlockDetectionConfig#spinDuration(Long)} instead
    */
   @Deprecated
   public void setDeadlockDetectionSpinDuration(long eagerDeadlockSpinDuration) {
      this.deadlockDetection.setSpinDuration(eagerDeadlockSpinDuration);
   }

   /**
    * @deprecated Use {@link #isDeadlockDetectionEnabled()} instead.
    */
   @Deprecated
   public boolean isEnableDeadlockDetection() {
      return deadlockDetection.enabled;
   }

   public boolean isDeadlockDetectionEnabled() {
      return deadlockDetection.enabled;
   }

   /**
    * Toggle to enable/disable deadlock detection
    *
    * @param useEagerDeadlockDetection
    * @deprecated Use {@link FluentConfiguration#deadlockDetection()} instead
    */
   @Deprecated
   public void setEnableDeadlockDetection(boolean useEagerDeadlockDetection) {
      this.deadlockDetection.setEnabled(useEagerDeadlockDetection);
   }

   /**
    * If true, a pool of shared locks is maintained for all entries that need to be locked. Otherwise, a lock is created
    * per entry in the cache. Lock striping helps control memory footprint but may reduce concurrency in the system.
    *
    * @param useLockStriping
    * @deprecated Use {@link FluentConfiguration.LockingConfig#useLockStriping(Boolean)} instead
    */
   @Deprecated
   public void setUseLockStriping(boolean useLockStriping) {
      locking.setUseLockStriping(useLockStriping);
   }

   public boolean isUseLockStriping() {
      return locking.useLockStriping;
   }

   public boolean isUnsafeUnreliableReturnValues() {
      return unsafe.unreliableReturnValues;
   }


   /**
    * Toggle to enable/disable return value fetching
    *
    * @param unsafeUnreliableReturnValues
    * @deprecated Use {@link FluentConfiguration.UnsafeConfig#unreliableReturnValues(Boolean)} instead
    */
   @Deprecated
   public void setUnsafeUnreliableReturnValues(boolean unsafeUnreliableReturnValues) {
      this.unsafe.setUnreliableReturnValues(unsafeUnreliableReturnValues);
   }

   /**
    * Rehashing timeout
    *
    * @param rehashRpcTimeout
    * @deprecated Use {@link FluentConfiguration.HashConfig#rehashRpcTimeout(Long)} instead
    */
   @Deprecated
   public void setRehashRpcTimeout(long rehashRpcTimeout) {
      this.clustering.hash.setRehashRpcTimeout(rehashRpcTimeout);
   }

   public long getRehashRpcTimeout() {
      return clustering.hash.rehashRpcTimeout;
   }

   public boolean isWriteSkewCheck() {
      return locking.writeSkewCheck;
   }

   /**
    * This setting is only applicable in the case of REPEATABLE_READ. When write skew check is set to false, if the
    * writer at commit time discovers that the working entry and the underlying entry have different versions, the
    * working entry will overwrite the underlying entry. If true, such version conflict - known as a write-skew - will
    * throw an Exception.
    *
    * @param writeSkewCheck
    * @deprecated Use {@link FluentConfiguration.LockingConfig#writeSkewCheck(Boolean)} instead
    */
   @Deprecated
   public void setWriteSkewCheck(boolean writeSkewCheck) {
      locking.setWriteSkewCheck(writeSkewCheck);
   }

   public int getConcurrencyLevel() {
      return locking.concurrencyLevel;
   }

   /**
    * Concurrency level for lock containers. Adjust this value according to the number of concurrent threads interating
    * with Infinispan. Similar to the concurrencyLevel tuning parameter seen in the JDK's ConcurrentHashMap.
    *
    * @param concurrencyLevel
    * @deprecated Use {@link FluentConfiguration.LockingConfig#concurrencyLevel(Integer)} instead
    */
   @Deprecated
   public void setConcurrencyLevel(int concurrencyLevel) {
      locking.setConcurrencyLevel(concurrencyLevel);
   }

   /**
    * If useReplQueue is set to true, this attribute can be used to trigger flushing of the queue when it reaches a
    * specific threshold.
    *
    * @param replQueueMaxElements
    * @deprecated Use {@link FluentConfiguration.AsyncConfig#replQueueMaxElements(Integer)} instead
    */
   @Deprecated
   public void setReplQueueMaxElements(int replQueueMaxElements) {
      this.clustering.async.setReplQueueMaxElements(replQueueMaxElements);
   }

   /**
    * If useReplQueue is set to true, this attribute controls how often the asynchronous thread used to flush the
    * replication queue runs. This should be a positive integer which represents thread wakeup time in milliseconds.
    *
    * @param replQueueInterval
    * @deprecated Use {@link FluentConfiguration.AsyncConfig#replQueueInterval(Long)} instead
    */
   @Deprecated
   public void setReplQueueInterval(long replQueueInterval) {
      this.clustering.async.setReplQueueInterval(replQueueInterval);
   }

   /**
    * @deprecated Use {@link FluentConfiguration.AsyncConfig#replQueueInterval(Long)} instead
    */
   @Deprecated
   public void setReplQueueInterval(long replQueueInterval, TimeUnit timeUnit) {
      setReplQueueInterval(timeUnit.toMillis(replQueueInterval));
   }

   /**
    * This overrides the replication queue implementation class. Overriding the default allows you to add behavior to
    * the queue, typically by subclassing the default implementation.
    *
    * @param classname
    * @deprecated Use {@link FluentConfiguration.AsyncConfig#replQueueClass(Class)} instead
    */
   @Deprecated
   public void setReplQueueClass(String classname) {
      this.clustering.async.setReplQueueClass(classname);
   }

   /**
    * @deprecated Use {@link FluentConfiguration#jmxStatistics()} instead
    */
   @Deprecated
   public void setExposeJmxStatistics(boolean useMbean) {
      jmxStatistics.setEnabled(useMbean);
   }

   /**
    * Enables invocation batching if set to <tt>true</tt>.  You still need to use {@link
    * org.infinispan.Cache#startBatch()} and {@link org.infinispan.Cache#endBatch(boolean)} to demarcate the start and
    * end of batches.
    *
    * @param enabled if true, batching is enabled.
    * @since 4.0
    * @deprecated Use {@link FluentConfiguration#invocationBatching()} instead
    */
   @Deprecated
   public void setInvocationBatchingEnabled(boolean enabled) {
      invocationBatching.setEnabled(enabled);
   }

   /**
    * If true, this will cause the cache to ask neighboring caches for state when it starts up, so the cache starts
    * 'warm', although it will impact startup time.
    *
    * @param fetchInMemoryState
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#fetchInMemoryState(Boolean)} instead
    */
   @Deprecated
   public void setFetchInMemoryState(boolean fetchInMemoryState) {
      this.clustering.stateRetrieval.setFetchInMemoryState(fetchInMemoryState);
   }

   /**
    * If true, this will allow the cache to provide in-memory state to a neighbor, even if the cache is not configured
    * to fetch state from its neighbors (fetchInMemoryState is false)
    *
    * @param alwaysProvideInMemoryState
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#alwaysProvideInMemoryState(Boolean)} instead
    */
   @Deprecated
   public void setAlwaysProvideInMemoryState(boolean alwaysProvideInMemoryState) {
      this.clustering.stateRetrieval.setAlwaysProvideInMemoryState(alwaysProvideInMemoryState);
   }

   /**
    * Maximum time to attempt a particular lock acquisition
    *
    * @param lockAcquisitionTimeout
    * @deprecated Use {@link FluentConfiguration.LockingConfig#lockAcquisitionTimeout(Long)} instead
    */
   @Deprecated
   public void setLockAcquisitionTimeout(long lockAcquisitionTimeout) {
      locking.setLockAcquisitionTimeout(lockAcquisitionTimeout);
   }

   /**
    * Maximum time to attempt a particular lock acquisition
    *
    * @param lockAcquisitionTimeout
    * @param timeUnit
    * @deprecated Use {@link FluentConfiguration.LockingConfig#lockAcquisitionTimeout(Long)} instead
    */
   @Deprecated
   public void setLockAcquisitionTimeout(long lockAcquisitionTimeout, TimeUnit timeUnit) {
      setLockAcquisitionTimeout(timeUnit.toMillis(lockAcquisitionTimeout));
   }


   /**
    * This is the timeout (in ms) used to wait for an acknowledgment when making a remote call, after which the call is
    * aborted and an exception is thrown.
    *
    * @param syncReplTimeout
    * @deprecated Use {@link FluentConfiguration.SyncConfig#replTimeout(Long)} instead
    */
   @Deprecated
   public void setSyncReplTimeout(long syncReplTimeout) {
      this.clustering.sync.setReplTimeout(syncReplTimeout);
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which the call is aborted
    * and an exception is thrown.
    *
    * @param syncReplTimeout
    * @param timeUnit
    * @deprecated Use {@link FluentConfiguration.SyncConfig#replTimeout(Long)} instead
    */
   @Deprecated
   public void setSyncReplTimeout(long syncReplTimeout, TimeUnit timeUnit) {
      setSyncReplTimeout(timeUnit.toMillis(syncReplTimeout));
   }

   /**
    * Cache mode. For distribution, set mode to either 'd', 'dist' or 'distribution'. For replication, use either 'r',
    * 'repl' or 'replication'. Finally, for invalidation, 'i', 'inv' or 'invalidation'.  If the cache mode is set to
    * 'l' or 'local', the cache in question will not support clustering even if its cache manager does.
    * When no transport is enabled, the default is 'local' (instead of 'dist').
    *
    * @deprecated Use {@link FluentConfiguration.ClusteringConfig#mode(org.infinispan.config.Configuration.CacheMode)} instead
    */
   @Deprecated
   public void setCacheMode(CacheMode cacheModeInt) {
      clustering.setMode(cacheModeInt);
   }

   /**
    * Cache mode. For distribution, set mode to either 'd', 'dist' or 'distribution'. For replication, use either 'r',
    * 'repl' or 'replication'. Finally, for invalidation, 'i', 'inv' or 'invalidation'.  If the cache mode is set to
    * 'l' or 'local', the cache in question will not support clustering even if its cache manager does.
    * When no transport is enabled, the default is 'local' (instead of 'dist').
    *
    * @deprecated Use {@link FluentConfiguration.ClusteringConfig#mode(org.infinispan.config.Configuration.CacheMode)} instead
    */
   @Deprecated
   public void setCacheMode(String cacheMode) {
      if (cacheMode == null) throw new ConfigurationException("Cache mode cannot be null", "CacheMode");
      clustering.setMode(CacheMode.valueOf(uc(cacheMode)));
      if (clustering.mode == null) {
         log.warn("Unknown cache mode '" + cacheMode + "', using defaults.");
         clustering.setMode(LOCAL);
      }
   }

   public String getCacheModeString() {
      return clustering.mode == null ? "none" : clustering.mode.toString();
   }

   /**
    * @deprecated Use {@link FluentConfiguration.ClusteringConfig#mode(org.infinispan.config.Configuration.CacheMode)} instead
    */
   @Deprecated
   public void setCacheModeString(String cacheMode) {
      setCacheMode(cacheMode);
   }

   /**
    * Pluggable data container class which must implement
    * {@link org.infinispan.container.DataContainer}
    */
   public String getDataContainerClass() {
      return dataContainer.dataContainerClass;
   }

   public DataContainer getDataContainer() {
      return dataContainer.dataContainer;
   }

   public TypedProperties getDataContainerProperties() {
      return dataContainer.properties;
   }

   /**
    * @deprecated Use {@link #getExpirationWakeUpInterval()}
    */
   @Deprecated
   public long getEvictionWakeUpInterval() {
      return getExpirationWakeUpInterval();
   }

   /**
    * @deprecated Use {@link FluentConfiguration.ExpirationConfig#wakeUpInterval(Long)} instead
    */
   @Deprecated
   public void setEvictionWakeUpInterval(long evictionWakeUpInterval) {
      this.eviction.setWakeUpInterval(evictionWakeUpInterval);
   }

   public EvictionStrategy getEvictionStrategy() {
      return eviction.strategy;
   }

   /**
    * Eviction strategy. Available options are 'UNORDERED', 'FIFO', 'LRU', 'LIRS' and 'NONE' (to disable eviction).
    *
    * @param evictionStrategy
    * @deprecated Use {@link FluentConfiguration.EvictionConfig#strategy(org.infinispan.eviction.EvictionStrategy)} instead
    */
   @Deprecated
   public void setEvictionStrategy(EvictionStrategy evictionStrategy) {
      this.eviction.setStrategy(evictionStrategy);
   }

   /**
    * Eviction strategy. Available options are 'UNORDERED', 'FIFO', 'LRU', 'LIRS' and 'NONE' (to disable eviction).
    *
    * @param eStrategy
    * @deprecated Use {@link FluentConfiguration.EvictionConfig#strategy(org.infinispan.eviction.EvictionStrategy)} instead
    */
   @Deprecated
   public void setEvictionStrategy(String eStrategy) {
      this.eviction.strategy = EvictionStrategy.valueOf(uc(eStrategy));
      if (this.eviction.strategy == null) {
         log.warn("Unknown evictionStrategy  '" + eStrategy + "'!  Using EvictionStrategy.NONE.");
         this.eviction.setStrategy(EvictionStrategy.NONE);
      }
   }

   public EvictionThreadPolicy getEvictionThreadPolicy() {
      return eviction.threadPolicy;
   }

   /**
    * Threading policy for eviction.
    *
    * @param policy
    * @deprecated Use {@link FluentConfiguration.EvictionConfig#threadPolicy(org.infinispan.eviction.EvictionThreadPolicy)} instead
    */
   @Deprecated
   public void setEvictionThreadPolicy(EvictionThreadPolicy policy) {
      this.eviction.setThreadPolicy(policy);
   }

   /**
    * Threading policy for eviction.
    *
    * @param policy
    * @deprecated Use {@link FluentConfiguration.EvictionConfig#threadPolicy(org.infinispan.eviction.EvictionThreadPolicy)} instead
    */
   @Deprecated
   public void setEvictionThreadPolicy(String policy) {
      this.eviction.threadPolicy = EvictionThreadPolicy.valueOf(uc(policy));
      if (this.eviction.threadPolicy == null) {
         log.warn("Unknown thread eviction policy  '" + policy + "'!  Using EvictionThreadPolicy.DEFAULT");
         this.eviction.setThreadPolicy(EvictionThreadPolicy.DEFAULT);
      }
   }

   public int getEvictionMaxEntries() {
      return eviction.maxEntries;
   }

   /**
    * Maximum number of entries in a cache instance. If selected value is not a power of two the actual value will
    * default to the least power of two larger than selected value. -1 means no limit.
    *
    * @param evictionMaxEntries
    * @deprecated Use {@link FluentConfiguration.EvictionConfig#maxEntries(Integer)} instead
    */
   @Deprecated
   public void setEvictionMaxEntries(int evictionMaxEntries) {
      this.eviction.setMaxEntries(evictionMaxEntries);
   }

   @Deprecated
   public void setVersioningScheme(VersioningScheme versioningScheme) {
      this.versioning.setVersioningScheme(versioningScheme);
   }

   @Deprecated
   public void setEnableVersioning(boolean enabled) {
      this.versioning.setEnabled(enabled);
   }

   /**
    * Expiration lifespan, in milliseconds
    */
   public long getExpirationLifespan() {
      return expiration.lifespan;
   }

   @Deprecated
   public VersioningScheme getVersioningScheme() {
      return this.versioning.versioningScheme;
   }

   @Deprecated
   public boolean isEnableVersioning() {
      return this.versioning.enabled;
   }



   /**
    * Maximum lifespan of a cache entry, after which the entry is expired cluster-wide, in milliseconds. -1 means the
    * entries never expire. <br /> <br /> Note that this can be overriden on a per-entry basis by using the Cache API.
    *
    * @param expirationLifespan
    * @deprecated Use {@link FluentConfiguration.ExpirationConfig#lifespan(Long)} instead
    */
   @Deprecated
   public void setExpirationLifespan(long expirationLifespan) {
      this.expiration.setLifespan(expirationLifespan);
   }

   /**
    * Expiration max idle time, in milliseconds
    */
   public long getExpirationMaxIdle() {
      return expiration.maxIdle;
   }


   /**
    * Maximum idle time a cache entry will be maintained in the cache, in milliseconds. If the idle time is exceeded,
    * the entry will be expired cluster-wide. -1 means the entries never expire. <br /> <br /> Note that this can be
    * overriden on a per-entry basis by using the Cache API.
    *
    * @param expirationMaxIdle
    * @deprecated Use {@link FluentConfiguration.ExpirationConfig#maxIdle(Long)} instead
    */
   @Deprecated
   public void setExpirationMaxIdle(long expirationMaxIdle) {
      this.expiration.setMaxIdle(expirationMaxIdle);
   }

   /**
    * Eviction thread wake up interval, in milliseconds.
    */
   public long getExpirationWakeUpInterval() {
      return expiration.wakeUpInterval;
   }

   /**
    * Fully qualified class name of a class that looks up a reference to a {@link javax.transaction.TransactionManager}.
    * The default provided is capable of locating the default TransactionManager in most popular Java EE systems, using
    * a JNDI lookup. Calling this method marks the cache as transactional.
    *
    * @param transactionManagerLookupClass
    * @deprecated Use {@link FluentConfiguration.TransactionConfig#transactionManagerLookupClass(Class)} instead
    */
   @Deprecated
   public void setTransactionManagerLookupClass(String transactionManagerLookupClass) {
      this.transaction.setTransactionManagerLookupClass(transactionManagerLookupClass);
   }

   /**
    * @deprecated Use {@link FluentConfiguration.TransactionConfig#transactionManagerLookup(TransactionManagerLookup)} instead
    */
   @Deprecated
   public void setTransactionManagerLookup(TransactionManagerLookup transactionManagerLookup) {
      this.transaction.transactionManagerLookup(transactionManagerLookup);
   }

   /**
    * @deprecated Use {@link FluentConfiguration.LoadersConfig#addCacheLoader(org.infinispan.loaders.CacheLoaderConfig...)} instead
    */
   @Deprecated
   public void setCacheLoaderManagerConfig(CacheLoaderManagerConfig cacheLoaderManagerConfig) {
      this.loaders = cacheLoaderManagerConfig;
   }

   /**
    * If true, the cluster-wide commit phase in two-phase commit (2PC) transactions will be synchronous, so Infinispan
    * will wait for responses from all nodes to which the commit was sent. Otherwise, the commit phase will be
    * asynchronous. Keeping it as false improves performance of 2PC transactions, since any remote failures are trapped
    * during the prepare phase anyway and appropriate rollbacks are issued.
    *
    * @param syncCommitPhase
    * @deprecated Use {@link FluentConfiguration.TransactionConfig#syncCommitPhase(Boolean)} instead
    */
   @Deprecated
   public void setSyncCommitPhase(boolean syncCommitPhase) {
      this.transaction.setSyncCommitPhase(syncCommitPhase);
   }

   /**
    * If true, the cluster-wide rollback phase in two-phase commit (2PC) transactions will be synchronous, so Infinispan
    * will wait for responses from all nodes to which the rollback was sent. Otherwise, the rollback phase will be
    * asynchronous. Keeping it as false improves performance of 2PC transactions.
    *
    * @param syncRollbackPhase
    * @deprecated Use {@link FluentConfiguration.TransactionConfig#syncRollbackPhase(Boolean)} instead
    */
   @Deprecated
   public void setSyncRollbackPhase(boolean syncRollbackPhase) {
      this.transaction.setSyncRollbackPhase(syncRollbackPhase);
   }

   /**
    * Only has effect for DIST mode and when useEagerLocking is set to true. When this is enabled, then only one node is
    * locked in the cluster, disregarding numOwners config. On the opposite, if this is false, then on all cache.lock()
    * calls numOwners RPCs are being performed. The node that gets locked is the main data owner, i.e. the node where
    * data would reside if numOwners==1. If the node where the lock resides crashes, then the transaction is marked for
    * rollback - data is in a consistent state, no fault tolerance.
    *
    * @param useEagerLocking
    * @deprecated Use {@link FluentConfiguration.TransactionConfig#useEagerLocking(Boolean)} instead
    */
   @Deprecated
   public void setUseEagerLocking(boolean useEagerLocking) {
      this.transaction.setUseEagerLocking(useEagerLocking);
   }

   /**
    * Only has effect for DIST mode and when useEagerLocking is set to true. When this is enabled, then only one node is
    * locked in the cluster, disregarding numOwners config. On the opposite, if this is false, then on all cache.lock()
    * calls numOwners RPCs are being performed. The node that gets locked is the main data owner, i.e. the node where
    * data would reside if numOwners==1. If the node where the lock resides crashes, then the transaction is marked for
    * rollback - data is in a consistent state, no fault tolerance.
    *
    * @param eagerLockSingleNode
    * @deprecated Use {@link FluentConfiguration.TransactionConfig#eagerLockSingleNode(Boolean)} instead
    */
   @Deprecated
   public void setEagerLockSingleNode(boolean eagerLockSingleNode) {
      this.transaction.setEagerLockSingleNode(eagerLockSingleNode);
   }

   /**
    * If there are any ongoing transactions when a cache is stopped,
    * Infinispan waits for ongoing remote and local transactions to finish.
    * The amount of time to wait for is defined by the cache stop timeout.
    * It is recommended that this value does not exceed the transaction
    * timeout because even if a new transaction was started just before the
    * cache was stopped, this could only last as long as the transaction
    * timeout allows it.
    *
    * @deprecated Use {@link FluentConfiguration.TransactionConfig#cacheStopTimeout(Integer)} instead
    */
   @Deprecated
   public Configuration setCacheStopTimeout(int cacheStopTimeout) {
      this.transaction.setCacheStopTimeout(cacheStopTimeout);
      return this;
   }

   /**
    * If true, this forces all async communications to be queued up and sent out periodically as a batch.
    *
    * @param useReplQueue
    * @deprecated Use {@link FluentConfiguration.AsyncConfig#useReplQueue(Boolean)} instead
    */
   @Deprecated
   public void setUseReplQueue(boolean useReplQueue) {
      this.clustering.async.setUseReplQueue(useReplQueue);
   }

   /**
    * Cache isolation level. Infinispan only supports READ_COMMITTED or REPEATABLE_READ isolation levels. See <a
    * href='http://en.wikipedia.org/wiki/Isolation_level'>http://en.wikipedia.org/wiki/Isolation_level</a> for a
    * discussion on isolation levels.
    *
    * @param isolationLevel
    * @deprecated Use {@link FluentConfiguration.LockingConfig#isolationLevel(org.infinispan.util.concurrent.IsolationLevel)} instead
    */
   @Deprecated
   public void setIsolationLevel(IsolationLevel isolationLevel) {
      locking.setIsolationLevel(isolationLevel);
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring caches, before throwing
    * an exception and aborting startup.
    *
    * @param stateRetrievalTimeout
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#timeout(Long)} instead
    */
   @Deprecated
   public void setStateRetrievalTimeout(long stateRetrievalTimeout) {
      this.clustering.stateRetrieval.setTimeout(stateRetrievalTimeout);
   }

   /**
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#timeout(Long)} instead
    */
   @Deprecated
   public void setStateRetrievalTimeout(long stateRetrievalTimeout, TimeUnit timeUnit) {
      setStateRetrievalTimeout(timeUnit.toMillis(stateRetrievalTimeout));
   }

   /**
    * This is the maximum amount of time to run a cluster-wide flush, to allow for syncing of transaction logs.
    *
    * @param logFlushTimeout
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#logFlushTimeout(Long)} instead
    */
   @Deprecated
   public void setStateRetrievalLogFlushTimeout(long logFlushTimeout) {
      this.clustering.stateRetrieval.setLogFlushTimeout(logFlushTimeout);
   }

   /**
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#logFlushTimeout(Long)} instead
    */
   @Deprecated
   public void setStateRetrievalLogFlushTimeout(long logFlushTimeout, TimeUnit timeUnit) {
      this.clustering.stateRetrieval.setLogFlushTimeout(timeUnit.toMillis(logFlushTimeout));
   }


   /**
    * This is the maximum number of non-progressing transaction log writes after which a brute-force flush approach is
    * resorted to, to synchronize transaction logs.
    *
    * @param maxNonProgressingLogWrites
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#maxNonProgressingLogWrites(Integer)} instead
    */
   @Deprecated
   public void setStateRetrievalMaxNonProgressingLogWrites(int maxNonProgressingLogWrites) {
      this.clustering.stateRetrieval.setMaxNonProgressingLogWrites(maxNonProgressingLogWrites);
   }

   /**
    * The size of a state transfer "chunk", in cache entries.
    *
    * @param chunkSize
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#chunkSize(Integer)} instead
    */
   @Deprecated
   public void setStateRetrievalChunkSize(int chunkSize) {
      this.clustering.stateRetrieval.setChunkSize(chunkSize);
   }

   /**
    * Initial wait time when backing off before retrying state transfer retrieval
    *
    * @param initialRetryWaitTime
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#initialRetryWaitTime(Long)} instead
    */
   @Deprecated
   public void setStateRetrievalInitialRetryWaitTime(long initialRetryWaitTime) {
      clustering.stateRetrieval.setInitialRetryWaitTime(initialRetryWaitTime);
   }

   /**
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#initialRetryWaitTime(Long)} instead
    */
   @Deprecated
   public void setStateRetrievalInitialRetryWaitTime(long initialRetryWaitTime, TimeUnit timeUnit) {
      setStateRetrievalInitialRetryWaitTime(timeUnit.toMillis(initialRetryWaitTime));
   }


   /**
    * Wait time increase factor over successive state retrieval backoffs
    *
    * @param retryWaitTimeIncreaseFactor
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#retryWaitTimeIncreaseFactor(Integer)} instead
    */
   @Deprecated
   public void setStateRetrievalRetryWaitTimeIncreaseFactor(int retryWaitTimeIncreaseFactor) {
      clustering.stateRetrieval.setRetryWaitTimeIncreaseFactor(retryWaitTimeIncreaseFactor);
   }

   /**
    * Number of state retrieval retries before giving up and aborting startup.
    *
    * @param numRetries
    * @deprecated Use {@link FluentConfiguration.StateRetrievalConfig#numRetries(Integer)} instead
    */
   @Deprecated
   public void setStateRetrievalNumRetries(int numRetries) {
      clustering.stateRetrieval.setNumRetries(numRetries);
   }

   /**
    * @deprecated Use {@link FluentConfiguration.LockingConfig#isolationLevel(org.infinispan.util.concurrent.IsolationLevel)} instead
    */
   @Deprecated
   public void setIsolationLevel(String isolationLevel) {
      if (isolationLevel == null) throw new ConfigurationException("Isolation level cannot be null", "IsolationLevel");
      locking.setIsolationLevel(IsolationLevel.valueOf(uc(isolationLevel)));
      if (locking.isolationLevel == null) {
         log.warn("Unknown isolation level '" + isolationLevel + "', using defaults.");
         locking.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      }
   }

   /**
    * @deprecated Use {@link FluentConfiguration#storeAsBinary()} instead
    */
   @Deprecated
   public void setUseLazyDeserialization(boolean useLazyDeserialization) {
      storeAsBinary.setEnabled(useLazyDeserialization);
   }

   /**
    * Toggle to enable/disable L1 cache.
    *
    * @param l1CacheEnabled
    * @deprecated Use {@link FluentConfiguration#l1()} instead
    */
   @Deprecated
   public void setL1CacheEnabled(boolean l1CacheEnabled) {
      this.clustering.l1.setEnabled(l1CacheEnabled);
   }

   /**
    * Maximum lifespan of an entry placed in the L1 cache.
    *
    * @param l1Lifespan
    * @deprecated Use {@link FluentConfiguration.L1Config#lifespan(Long)} instead
    */
   @Deprecated
   public void setL1Lifespan(long l1Lifespan) {
      this.clustering.l1.setLifespan(l1Lifespan);
   }

   /**
    * If true, entries removed due to a rehash will be moved to L1 rather than being removed altogether.
    *
    * @param l1OnRehash
    * @deprecated Use {@link FluentConfiguration.L1Config#onRehash(Boolean)} instead
    */
   @Deprecated
   public void setL1OnRehash(boolean l1OnRehash) {
      this.clustering.l1.setOnRehash(l1OnRehash);
   }

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
    * @deprecated Use {@link FluentConfiguration.L1Config#invalidationThreshold(Integer)} instead
    */
   @Deprecated
   public void setL1InvalidationThreshold(int threshold) {
      this.clustering.l1.setInvalidationThreshold(threshold);
   }

   public int getL1InvalidationThreshold() {
   	return this.clustering.l1.invalidationThreshold;
   }

   /**
    * @deprecated No longer used since 5.2, use {@link org.infinispan.configuration.cache.HashConfigurationBuilder#consistentHashFactory(org.infinispan.distribution.ch.ConsistentHashFactory)} instead.
    */
   @Deprecated
   public void setConsistentHashClass(String consistentHashClass) {
      this.clustering.hash.setConsistentHashClass(consistentHashClass);
   }

   /**
    * A fully qualified name of the class providing a hash function, used as a bit spreader and a general hash code
    * generator.  Typically used in conjunction with the many default {@link org.infinispan.distribution.ch.ConsistentHash}
    * implementations shipped.
    *
    * @param hashFunctionClass
    * @deprecated Use {@link FluentConfiguration.HashConfig#hashFunctionClass(Class)} instead
    */
   @Deprecated
   public void setHashFunctionClass(String hashFunctionClass) {
      clustering.hash.hashFunctionClass = hashFunctionClass;
   }

   /**
    * Number of cluster-wide replicas for each cache entry.
    *
    * @param numOwners
    * @deprecated Use {@link FluentConfiguration.HashConfig#numOwners(Integer)} instead
    */
   @Deprecated
   public void setNumOwners(int numOwners) {
      this.clustering.hash.setNumOwners(numOwners);
   }

   /**
    * If false, no rebalancing or rehashing will take place when a new node joins the cluster or a node leaves
    *
    * @param rehashEnabled
    * @deprecated Use {@link FluentConfiguration.HashConfig#rehashEnabled(Boolean)} instead
    */
   @Deprecated
   public void setRehashEnabled(boolean rehashEnabled) {
      this.clustering.hash.setRehashEnabled(rehashEnabled);
   }

   /**
    * @deprecated Use {@link FluentConfiguration.HashConfig#rehashWait(Long)} instead
    */
   @Deprecated
   public void setRehashWaitTime(long rehashWaitTime) {
      this.clustering.hash.setRehashWait(rehashWaitTime);
   }

   /**
    * If true, asynchronous marshalling is enabled which means that caller can return even quicker, but it can suffer
    * from reordering of operations. You can find more information <a href=&quot;http://community.jboss.org/docs/DOC-15725&quot;>here</a>
    *
    * @param useAsyncMarshalling
    * @deprecated Use {@link FluentConfiguration.AsyncConfig#asyncMarshalling(Boolean)} instead
    */
   @Deprecated
   public void setUseAsyncMarshalling(boolean useAsyncMarshalling) {
      this.clustering.async.setAsyncMarshalling(useAsyncMarshalling);
   }

   /**
    * If enabled, entries will be indexed when they are added to the cache. Indexes will be updated as entries change or
    * are removed.
    *
    * @param enabled
    * @deprecated Use {@link FluentConfiguration#indexing()} instead
    */
   @Deprecated
   public void setIndexingEnabled(boolean enabled) {
      this.indexing.setEnabled(enabled);
   }

   /**
    * If true, only index changes made locally, ignoring remote changes. This is useful if indexes are shared across a
    * cluster to prevent redundant indexing of updates.
    *
    * @param indexLocalOnly
    * @deprecated Use {@link FluentConfiguration.IndexingConfig#indexLocalOnly(Boolean)} instead
    */
   @Deprecated
   public void setIndexLocalOnly(boolean indexLocalOnly) {
      this.indexing.setIndexLocalOnly(indexLocalOnly);
   }

   // ------------------------------------------------------------------------------------------------------------
   //   GETTERS
   // ------------------------------------------------------------------------------------------------------------

   public boolean isUseAsyncMarshalling() {
      return clustering.async.asyncMarshalling;
   }

   public boolean isUseReplQueue() {
      return clustering.async.useReplQueue;
   }

   public int getReplQueueMaxElements() {
      return clustering.async.replQueueMaxElements;
   }

   public long getReplQueueInterval() {
      return clustering.async.replQueueInterval;
   }

   public String getReplQueueClass() {
      return this.clustering.async.replQueueClass;
   }

   public boolean isExposeJmxStatistics() {
      return jmxStatistics.enabled;
   }

   /**
    * @return true if invocation batching is enabled.
    * @since 4.0
    */
   public boolean isInvocationBatchingEnabled() {
      return invocationBatching.enabled;
   }

   public boolean isIndexingEnabled() {
      return indexing.isEnabled();
   }

   public boolean isIndexLocalOnly() {
      return indexing.isIndexLocalOnly();
   }

   public TypedProperties getIndexingProperties() {
      return indexing.properties;
   }

   public boolean isFetchInMemoryState() {
      return clustering.stateRetrieval.fetchInMemoryState;
   }

   public boolean isAlwaysProvideInMemoryState() {
      return clustering.stateRetrieval.alwaysProvideInMemoryState;
   }

   /**
    * Returns true if and only if {@link #isUseEagerLocking()}, {@link #isEagerLockSingleNode()} and the cache is
    * distributed.
    * @deprecated this is deprecated as starting with Infinispan 5.1 a single lock is always acquired disregarding the
    * number of owner.
    */
   @Deprecated
   public boolean isEagerLockingSingleNodeInUse() {
      return isUseEagerLocking() && isEagerLockSingleNode() && getCacheMode().isDistributed();
   }


   public long getLockAcquisitionTimeout() {
      return locking.lockAcquisitionTimeout;
   }

   public long getSyncReplTimeout() {
      return clustering.sync.replTimeout;
   }

   public CacheMode getCacheMode() {
      return clustering.mode;
   }

   /**
    * Returns the locking mode for this cache.
    *
    * @see LockingMode
    */
   public LockingMode getTransactionLockingMode() {
      return transaction.lockingMode;
   }

   /**
    * Returns cache's transaction mode. By default a cache is not transactinal, i.e. the transaction mode
    * is {@link TransactionMode#NON_TRANSACTIONAL}
    * @see TransactionMode
    */
   public TransactionMode getTransactionMode() {
      return transaction.transactionMode;
   }

   /**
    * If the cache is transactional (i.e. {@link #isTransactionalCache()} == true) and transactionAutoCommit is enabled
    * then for single operation transactions the user doesn't need to manually start a transaction, but a transactions
    * is injected by the system. Defaults to true.
    */
   public boolean isTransactionAutoCommit() {
      return transaction.autoCommit;
   }

   /**
    * Enabling this would cause autoCommit transactions ({@link #isTransactionAutoCommit()}) to complete with 1 RPC
    * instead of 2 RPCs (which is default).
    * <br/>
    * Important: enabling this feature might cause inconsistencies when two transactions concurrently write on the same key. This is
    * explained here: {@link org.infinispan.config.Configuration#isSyncCommitPhase()}.
    * <br/>
    * The reason this configuration was added is the following:
    * <ul>
    *    <li>
    *  before infinispan 5.1 caches could be used in a mixed way, i.e. transactional and non transactional
    *    </li>
    *    <li>
    *  for this mixed access mode, the non transactional calls were more efficient (1 RPC vs 2 RPCs needed by 2PC) but
    *    </li>
    * also offer fewer guarantees when it comes to concurrent access
    *    <li>
    *  for these existing use cases, and similar new ones, it makes sense to enable <b>use1PcForAutoCommitTransactions</b>
    * in order to better trade between consistency and performance.
    *   </li>
    * </ul>
    */
   public boolean isUse1PcForAutoCommitTransactions() {
      return transaction.use1PcForAutoCommitTransactions;
   }

   public IsolationLevel getIsolationLevel() {
      return locking.isolationLevel;
   }

   public String getTransactionManagerLookupClass() {
      return transaction.transactionManagerLookupClass;
   }

   public TransactionManagerLookup getTransactionManagerLookup() {
      return transaction.transactionManagerLookup;
   }

   public TransactionSynchronizationRegistryLookup getTransactionSynchronizationRegistryLookup() {
      return transaction.transactionSynchronizationRegistryLookup;
   }

   /**
    * @deprecated Use {@link #getCacheLoaders()}, {@link #isCacheLoaderShared()}
    * {@link #isFetchPersistentState()}, {@link #isCacheLoaderPassivation()}
    * and {@link #isCacheLoaderPreload()} instead
    */
   @Deprecated
   public CacheLoaderManagerConfig getCacheLoaderManagerConfig() {
      return loaders;
   }

   public List<CacheLoaderConfig> getCacheLoaders() {
      return loaders.getCacheLoaderConfigs();
   }

   public boolean isCacheLoaderShared() {
      return loaders.isShared();
   }

   public boolean isFetchPersistentState() {
      return loaders.isFetchPersistentState();
   }

   public boolean isCacheLoaderPassivation() {
      return loaders.isPassivation();
   }

   public boolean isCacheLoaderPreload() {
      return loaders.isPreload();
   }

   /**
    * Important - to be used with caution: if you have two transactions writing to the same key concurrently and
    * the commit is configured to be performed asynchronously then inconsistencies might happen. This is because in
    * order to have such consistency guarantees locks need to be released asynchronously after all the commits are
    * acknowledged on the originator. In the case of an asynchronous commit messages we don't wait for all the
    * commit messages to be acknowledged, but release the locks together with the commit message.
    */
   public boolean isSyncCommitPhase() {
      return transaction.syncCommitPhase;
   }

   public boolean isSyncRollbackPhase() {
      return transaction.syncRollbackPhase;
   }

   /**
    * Returns true if the 2nd phase of the 2PC (i.e. either commit or rollback) is sent asynchronously.
    */
   public boolean isSecondPhaseAsync() {
      return !isSyncCommitPhase() || isUseReplQueue() || !getCacheMode().isSynchronous();
   }

   /**
    * This is now deprecated. An "eager" locking cache is a transactional cache running in pessimistic mode.
    * @see #getTransactionLockingMode()
    */
   @Deprecated
   public boolean isUseEagerLocking() {
      return transaction.useEagerLocking;
   }

   /**
    * @deprecated starting with Infinispan 5.1 single node locking is used by default
    */
   @Deprecated
   public boolean isEagerLockSingleNode() {
      return transaction.eagerLockSingleNode;
   }

   public int getCacheStopTimeout() {
      return transaction.cacheStopTimeout;
   }

   public long getStateRetrievalTimeout() {
      return clustering.stateRetrieval.timeout;
   }

   public long getStateRetrievalInitialRetryWaitTime() {
      return clustering.stateRetrieval.initialRetryWaitTime;
   }

   public int getStateRetrievalRetryWaitTimeIncreaseFactor() {
      return clustering.stateRetrieval.retryWaitTimeIncreaseFactor;
   }

   public int getStateRetrievalNumRetries() {
      return clustering.stateRetrieval.numRetries;
   }

   public int getStateRetrievalMaxNonProgressingLogWrites() {
      return clustering.stateRetrieval.maxNonProgressingLogWrites;
   }

   public int getStateRetrievalChunkSize() {
      return clustering.stateRetrieval.chunkSize;
   }

   public long getStateRetrievalLogFlushTimeout() {
      return clustering.stateRetrieval.logFlushTimeout;
   }

   /**
    * @deprecated Use {@link #isStoreAsBinary()}
    */
   @Deprecated
   public boolean isUseLazyDeserialization() {
      return isStoreAsBinary();
   }

   public boolean isStoreAsBinary() {
      if (lazyDeserialization.enabled) {
         storeAsBinary.enabled = true;
      }
      return storeAsBinary.enabled;
   }

   public boolean isL1CacheEnabled() {
      return clustering.l1.enabled;
   }

   public boolean isL1CacheActivated() {
      return clustering.l1.activated && isL1CacheEnabled();
   }

   public long getL1Lifespan() {
      return clustering.l1.lifespan;
   }

   public boolean isL1OnRehash() {
      return clustering.l1.onRehash;
   }

   /**
    * @deprecated No longer used since 5.2, use {@link org.infinispan.configuration.cache.HashConfigurationBuilder#consistentHashFactory(org.infinispan.distribution.ch.ConsistentHashFactory)} instead.
    */
   @Deprecated
   public String getConsistentHashClass() {
      return clustering.hash.consistentHashClass;
   }

   /**
    * @deprecated No longer useful, since {@link #getConsistentHashClass()} is not used.
    */
   @Deprecated
   public boolean isCustomConsistentHashClass() {
      return false;
   }

   public boolean isCustomHashFunctionClass() {
      return clustering.hash.hashFunctionClass != null &&
            !clustering.hash.hashFunctionClass.equals(MurmurHash3.class.getName());
   }

   public String getHashFunctionClass() {
      return clustering.hash.hashFunctionClass;
   }

   public int getNumOwners() {
      return clustering.hash.numOwners;
   }

   public int getNumVirtualNodes() {
      return clustering.hash.numVirtualNodes;
   }

   public boolean isGroupsEnabled() {
      clustering.hash.groups.setConfiguration(this);
      return clustering.hash.groups.enabled;
   }

   public List<Grouper<?>> getGroupers() {
      clustering.hash.groups.setConfiguration(this);
      return clustering.hash.groups.groupers;
   }

   public boolean isRehashEnabled() {
      return clustering.hash.rehashEnabled;
   }

   public long getRehashWaitTime() {
      return clustering.hash.rehashWait;
   }

   /**
    * Returns true if transaction recovery information is collected.
    */
   public boolean isTransactionRecoveryEnabled() {
      return transaction.recovery.isEnabled();
   }

   /**
    * Returns the name of the cache used in order to keep recovery information.
    */
   public String getTransactionRecoveryCacheName() {
      return transaction.recovery.getRecoveryInfoCacheName();
   }

   /**
    * If enabled Infinispan enlists within transactions as a {@link javax.transaction.Synchronization}. If disabled
    * (default) then Infinispan enlists as an {@link javax.transaction.xa.XAResource}, being able to fully participate
    * in distributed transaction. More about this <a href="http://community.jboss.org/wiki/Infinispantransactions#Enlisting_Synchronization">here</a>.
    */
   public boolean isUseSynchronizationForTransactions() {
      return transaction.isUseSynchronization();
   }

   // ------------------------------------------------------------------------------------------------------------
   //   HELPERS
   // ------------------------------------------------------------------------------------------------------------

   // ------------------------------------------------------------------------------------------------------------
   //   OVERRIDDEN METHODS
   // ------------------------------------------------------------------------------------------------------------

   public void accept(ConfigurationBeanVisitor v) {
      v.visitConfiguration(this);
      clustering.accept(v);
      customInterceptors.accept(v);
      dataContainer.accept(v);
      deadlockDetection.accept(v);
      eviction.accept(v);
      expiration.accept(v);
      invocationBatching.accept(v);
      jmxStatistics.accept(v);
      storeAsBinary.accept(v);
      lazyDeserialization.accept(v);
      loaders.accept(v);
      locking.accept(v);
      transaction.accept(v);
      unsafe.accept(v);
      indexing.accept(v);
      versioning.accept(v);
   }

   /**
    * Also see {@link #equalsIgnoreName(Object)} for equality that does not consider the name of the configuration.
    */
   @Override
   public boolean equals(Object o) {
      if (!equalsIgnoreName(o)) return false;
      Configuration that = (Configuration) o;
      return !(name != null ? !name.equals(that.name) : that.name != null);
   }

   /**
    * Same as {@link #equals(Object)} but it ignores the {@link #getName()} attribute in the comparison.
    */
   public boolean equalsIgnoreName(Object o) {
      if (this == o) return true;
      if (!(o instanceof Configuration)) return false;

      Configuration that = (Configuration) o;

      if (clustering != null ? !clustering.equals(that.clustering) : that.clustering != null) return false;
      if (customInterceptors != null ? !customInterceptors.equals(that.customInterceptors) : that.customInterceptors != null)
         return false;
      if (dataContainer != null ? !dataContainer.equals(that.dataContainer) : that.dataContainer != null) return false;
      if (deadlockDetection != null ? !deadlockDetection.equals(that.deadlockDetection) : that.deadlockDetection != null)
         return false;
      if (eviction != null ? !eviction.equals(that.eviction) : that.eviction != null) return false;
      if (expiration != null ? !expiration.equals(that.expiration) : that.expiration != null) return false;
      if (globalConfiguration != null ? !globalConfiguration.equals(that.globalConfiguration) : that.globalConfiguration != null)
         return false;
      if (invocationBatching != null ? !invocationBatching.equals(that.invocationBatching) : that.invocationBatching != null)
         return false;
      if (jmxStatistics != null ? !jmxStatistics.equals(that.jmxStatistics) : that.jmxStatistics != null) return false;
      if (storeAsBinary != null ? !storeAsBinary.equals(that.storeAsBinary) : that.storeAsBinary != null)
         return false;
      if (lazyDeserialization != null ? !lazyDeserialization.equals(that.lazyDeserialization) : that.lazyDeserialization != null)
         return false;
      if (loaders != null ? !loaders.equals(that.loaders) : that.loaders != null) return false;
      if (locking != null ? !locking.equals(that.locking) : that.locking != null) return false;
      if (transaction != null ? !transaction.equals(that.transaction) : that.transaction != null) return false;
      if (unsafe != null ? !unsafe.equals(that.unsafe) : that.unsafe != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = globalConfiguration != null ? globalConfiguration.hashCode() : 0;
      result = 31 * result + (name != null ? name.hashCode() : 0);
      result = 31 * result + (locking != null ? locking.hashCode() : 0);
      result = 31 * result + (loaders != null ? loaders.hashCode() : 0);
      result = 31 * result + (transaction != null ? transaction.hashCode() : 0);
      result = 31 * result + (customInterceptors != null ? customInterceptors.hashCode() : 0);
      result = 31 * result + (dataContainer != null ? dataContainer.hashCode() : 0);
      result = 31 * result + (eviction != null ? eviction.hashCode() : 0);
      result = 31 * result + (expiration != null ? expiration.hashCode() : 0);
      result = 31 * result + (unsafe != null ? unsafe.hashCode() : 0);
      result = 31 * result + (clustering != null ? clustering.hashCode() : 0);
      result = 31 * result + (jmxStatistics != null ? jmxStatistics.hashCode() : 0);
      result = 31 * result + (storeAsBinary != null ? storeAsBinary.hashCode() : 0);
      result = 31 * result + (lazyDeserialization != null ? lazyDeserialization.hashCode() : 0);
      result = 31 * result + (invocationBatching != null ? invocationBatching.hashCode() : 0);
      result = 31 * result + (deadlockDetection != null ? deadlockDetection.hashCode() : 0);
      return result;
   }

   @Override
   public Configuration clone() {
      try {
         Configuration dolly = (Configuration) super.clone();
         if (clustering != null) {
            dolly.clustering = clustering.clone();
            dolly.clustering.setConfiguration(dolly);
         }
         // The globalConfiguration reference is shared, shouldn't clone it
         //if (globalConfiguration != null) dolly.globalConfiguration = globalConfiguration.clone();
         if (locking != null) {
            dolly.locking = (LockingType) locking.clone();
            dolly.locking.setConfiguration(dolly);
         }
         if (loaders != null) {
            dolly.loaders = loaders.clone();
            dolly.loaders.setConfiguration(dolly);
         }
         if (transaction != null) {
            dolly.transaction = transaction.clone();
            dolly.transaction.setConfiguration(dolly);
         }
         if (customInterceptors != null) {
            dolly.customInterceptors = customInterceptors.clone();
            dolly.customInterceptors.setConfiguration(dolly);
         }
         if (dataContainer != null) {
            dolly.dataContainer = (DataContainerType) dataContainer.clone();
            dolly.dataContainer.setConfiguration(dolly);
         }
         if (eviction != null) {
            dolly.eviction = (EvictionType) eviction.clone();
            dolly.eviction.setConfiguration(dolly);
         }
         if (expiration != null) {
            dolly.expiration = (ExpirationType) expiration.clone();
            dolly.expiration.setConfiguration(dolly);
         }
         if (unsafe != null) {
            dolly.unsafe = (UnsafeType) unsafe.clone();
            dolly.unsafe.setConfiguration(dolly);
         }
         if (clustering != null) {
            dolly.clustering = clustering.clone();
            dolly.clustering.setConfiguration(dolly);
         }
         if (jmxStatistics != null) {
            dolly.jmxStatistics = (JmxStatistics) jmxStatistics.clone();
            dolly.jmxStatistics.setConfiguration(dolly);
         }
         if (storeAsBinary != null) {
            dolly.storeAsBinary = storeAsBinary.clone();
            dolly.storeAsBinary.setConfiguration(dolly);
         }
         if (lazyDeserialization != null) {
            dolly.lazyDeserialization = (LazyDeserialization) lazyDeserialization.clone();
            dolly.lazyDeserialization.setConfiguration(dolly);
         }
         if (invocationBatching != null) {
            dolly.invocationBatching = (InvocationBatching) invocationBatching.clone();
            dolly.invocationBatching.setConfiguration(dolly);
         }
         if (deadlockDetection != null) {
            dolly.deadlockDetection = (DeadlockDetectionType) deadlockDetection.clone();
            dolly.deadlockDetection.setConfiguration(dolly);
         }
         if (transaction != null) {
            dolly.transaction = transaction.clone();
            dolly.transaction.setConfiguration(dolly);
         }
         if (indexing != null) {
            dolly.indexing = indexing.clone();
            dolly.indexing.setConfiguration(dolly);
         }
         dolly.fluentConfig = new FluentConfiguration(dolly);
         return dolly;
      } catch (CloneNotSupportedException e) {
         throw new CacheException("Unexpected!", e);
      }
   }

   /**
    * Converts this configuration instance to an XML representation containing the current settings.
    *
    * @return a string containing the formatted XML representation of this configuration instance.
    */
   public String toXmlString() {
      return InfinispanConfiguration.toXmlString(this);
   }

   public boolean isUsingCacheLoaders() {
      return getCacheLoaderManagerConfig() != null && !getCacheLoaderManagerConfig().getCacheLoaderConfigs().isEmpty();
   }

   /**
    * Returns the {@link org.infinispan.config.CustomInterceptorConfig}, if any, associated with this configuration
    * object. The custom interceptors will be added to the cache at startup in the sequence defined by this list.
    *
    * @return List of custom interceptors, never null
    */
   @SuppressWarnings("unchecked")
   public List<CustomInterceptorConfig> getCustomInterceptors() {
      return customInterceptors.customInterceptors == null
            ? Collections.<CustomInterceptorConfig>emptyList()
            : customInterceptors.customInterceptors;
   }

   public boolean isStoreKeysAsBinary() {
      return storeAsBinary.isStoreKeysAsBinary();
   }

   public boolean isStoreValuesAsBinary() {
      return storeAsBinary.isStoreValuesAsBinary();
   }
   /**
    * @deprecated Use {@link FluentConfiguration.CustomInterceptorsConfig#add(org.infinispan.interceptors.base.CommandInterceptor)}
    */
   @Deprecated
   public void setCustomInterceptors(List<CustomInterceptorConfig> customInterceptors) {
      this.customInterceptors.setCustomInterceptors(customInterceptors);
   }

   public void assertValid() throws ConfigurationException {
      if (clustering.mode.isClustered() && (globalConfiguration != null
              && (globalConfiguration.getTransportClass() == null || globalConfiguration.getTransportClass().length() == 0)))
         throw new ConfigurationException("Cache cannot use a clustered mode (" + clustering.mode + ") mode and not define a transport!");
   }

   public boolean isOnePhaseCommit() {
      return !getCacheMode().isSynchronous() || getTransactionLockingMode() == LockingMode.PESSIMISTIC;
   }

   /**
    * Returns true if the cache is configured to run in transactional mode, false otherwise. Starting with Infinispan
    * version 5.1 a cache doesn't support mixed access: i.e.won't support transactional and non-transactional
    * operations.
    * A cache is transactional if one the following:
    * <pre>
    * - a transactionManagerLookup is configured for the cache
    * - batching is enabled
    * - it is explicitly marked as transactional: config.fluent().transaction().transactionMode(TransactionMode.TRANSACTIONAL).
    *   In this last case a transactionManagerLookup needs to be explicitly set
    * </pre>
    * By default a cache is not transactional.
    *
    * @see #isTransactionAutoCommit()
    */
   public boolean isTransactionalCache() {
      return transaction.transactionMode.equals(TransactionMode.TRANSACTIONAL);
   }

   public boolean isExpirationReaperEnabled() {
       return expiration.reaperEnabled;
    }

   public boolean isHashActivated() {
      return clustering.hash.activated;
   }

   public long getL1InvalidationCleanupTaskFrequency() {
      return clustering.l1.getL1InvalidationCleanupTaskFrequency();
   }

   public void setL1InvalidationCleanupTaskFrequency(long frequencyMillis) {
      clustering.l1.setL1InvalidationCleanupTaskFrequency(frequencyMillis);
   }

   /**
    * Defines transactional (JTA) characteristics of the cache.
    *
    * @see <a href="../../../config.html#ce_default_transaction">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @XmlType(propOrder = {})
   @ConfigurationDoc(name = "transaction")
   @Deprecated public static class TransactionType extends AbstractFluentConfigurationBean implements TransactionConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -3867090839830874603L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setTransactionManagerLookupClass")
      protected String transactionManagerLookupClass;

      @XmlTransient
      protected TransactionManagerLookup transactionManagerLookup;

      @XmlTransient
      protected TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup;

      @Dynamic
      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setSyncCommitPhase")
      protected Boolean syncCommitPhase = true;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setSyncRollbackPhase")
      @Dynamic
      protected Boolean syncRollbackPhase = false;

      @Dynamic
      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setUseEagerLocking")
      protected Boolean useEagerLocking = false;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "isUseSynchronizationForTransactions")
      protected Boolean useSynchronization = false;

      @Dynamic
      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setEagerLockSingleNode")
      protected Boolean eagerLockSingleNode = false;

      @Dynamic
      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setCacheStopTimeout")
      protected Integer cacheStopTimeout = 30000;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "getTransactionLockingMode")
      protected LockingMode lockingMode = LockingMode.OPTIMISTIC;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "getTransactionMode")
      protected TransactionMode transactionMode = TransactionMode.NON_TRANSACTIONAL;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "isTransactionAutoCommit")
      protected Boolean autoCommit = true;

      @XmlElement
      protected RecoveryType recovery = new RecoveryType();

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "isUse1PcForAutoCommitTransactions")
      private Boolean use1PcForAutoCommitTransactions = Boolean.FALSE;

      public TransactionType(String transactionManagerLookupClass) {
         this.transactionManagerLookupClass = transactionManagerLookupClass;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitTransactionType(this);
         recovery.accept(v);
      }

      public TransactionType() {
         this.transactionManagerLookupClass = GenericTransactionManagerLookup.class.getName();
      }


      @XmlAttribute
      public String getTransactionManagerLookupClass() {
         return transactionManagerLookupClass;
      }

      @Override
      public TransactionConfig transactionMode(TransactionMode txMode) {
         testImmutability("transactionMode");
         this.transactionMode = txMode;
         return this;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #transactionManagerLookupClass(Class)} instead
       */
      @Deprecated
      public void setTransactionManagerLookupClass(String transactionManagerLookupClass) {
         testImmutability("transactionManagerLookupClass");
         if (transactionManagerLookupClass != null) transactionMode(TransactionMode.TRANSACTIONAL);
         this.transactionManagerLookupClass = transactionManagerLookupClass;
      }

      @Override
      public TransactionConfig transactionManagerLookupClass(Class<? extends TransactionManagerLookup> transactionManagerLookupClass) {
         setTransactionManagerLookupClass(transactionManagerLookupClass == null ? null : transactionManagerLookupClass.getName());
         return this;
      }

      @XmlAttribute
      public Boolean isSyncCommitPhase() {
         return syncCommitPhase;
      }

      @XmlAttribute
      public Boolean getUse1PcForAutoCommitTransactions() {
         return use1PcForAutoCommitTransactions;
      }

      public void setUse1PcForAutoCommitTransactions(Boolean use1PcForAutoCommitTransactions) {
         this.use1PcForAutoCommitTransactions = use1PcForAutoCommitTransactions;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #syncCommitPhase(Boolean)} instead
       */
      @Deprecated
      public void setSyncCommitPhase(Boolean syncCommitPhase) {
         testImmutability("syncCommitPhase");
         this.syncCommitPhase = syncCommitPhase;
      }

      /**
       * Important: enabling this might cause inconsistencies if multiple transactions update the same key concurrently.
       * See {@link org.infinispan.config.Configuration#isSyncCommitPhase()} for more details.
       */
      @Override
      public TransactionConfig syncCommitPhase(Boolean syncCommitPhase) {
         setSyncCommitPhase(syncCommitPhase);
         return this;
      }

      @Override
      public TransactionConfig useSynchronization(Boolean useSynchronization) {
         return setUseSynchronization(useSynchronization);
      }

      @Override
      public TransactionConfig lockingMode(LockingMode lockingMode) {
         testImmutability("lockingMode");
         this.lockingMode = lockingMode;
         return this;
      }

      @Override
      public TransactionConfig autoCommit(boolean enabled) {
         testImmutability("autoCommit");
         this.autoCommit = enabled;
         return this;
      }

      /**
       * Please refer to {@link org.infinispan.config.Configuration#isUse1PcForAutoCommitTransactions()}.
       */
      @Override
      public TransactionType use1PcForAutoCommitTransactions(boolean b) {
         testImmutability("use1PcForAutoCommitTransactions");
         this.use1PcForAutoCommitTransactions = b;
         return this;
      }

      @XmlAttribute
      public Boolean isUseSynchronization() {
         return useSynchronization;
      }

      /**
       * Needed for JAXB
       */
      private TransactionConfig setUseSynchronization(Boolean useSynchronization) {
         testImmutability("useSynchronization");
         this.useSynchronization = useSynchronization;
         return this;
      }

      @XmlAttribute
      public Boolean isSyncRollbackPhase() {
         return syncRollbackPhase;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #setSyncRollbackPhase(Boolean)} instead
       */
      @Deprecated
      public void setSyncRollbackPhase(Boolean syncRollbackPhase) {
         testImmutability("syncRollbackPhase");
         this.syncRollbackPhase = syncRollbackPhase;
      }

      @Override
      public TransactionConfig syncRollbackPhase(Boolean syncRollbackPhase) {
         setSyncRollbackPhase(syncRollbackPhase);
         return this;
      }


      @XmlAttribute
      public Boolean isUseEagerLocking() {
         return useEagerLocking;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #useEagerLocking(Boolean)} instead
       */
      @Deprecated
      public void setUseEagerLocking(Boolean useEagerLocking) {
         testImmutability("useEagerLocking");
         this.useEagerLocking = useEagerLocking;
      }

      @Override
      public TransactionConfig useEagerLocking(Boolean useEagerLocking) {
         setUseEagerLocking(useEagerLocking);
         return this;
      }

      @Override
      public TransactionConfig transactionManagerLookup(TransactionManagerLookup transactionManagerLookup) {
         testImmutability("transactionManagerLookup");
         this.transactionManagerLookup = transactionManagerLookup;
         if (transactionManagerLookup != null) transactionMode(TransactionMode.TRANSACTIONAL);
         return this;
      }

      @Override
      public TransactionConfig transactionSynchronizationRegistryLookup(TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup) {
         testImmutability("transactionSynchronizationRegistryLookup");
         this.transactionSynchronizationRegistryLookup = transactionSynchronizationRegistryLookup;
         if (transactionSynchronizationRegistryLookup != null) transactionMode(TransactionMode.TRANSACTIONAL);
         return this;
      }

      public TransactionSynchronizationRegistryLookup getTransactionSynchronizationRegistryLookup() {
         return transactionSynchronizationRegistryLookup;
      }

      @XmlAttribute
      public Boolean isEagerLockSingleNode() {
         return eagerLockSingleNode;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #eagerLockSingleNode(Boolean)} instead
       */
      @Deprecated
      public TransactionConfig setEagerLockSingleNode(Boolean eagerLockSingleNode) {
         testImmutability("eagerLockSingleNode");
         this.eagerLockSingleNode = eagerLockSingleNode;
         return this;
      }

      @Override
      public TransactionConfig eagerLockSingleNode(Boolean eagerLockSingleNode) {
         setEagerLockSingleNode(eagerLockSingleNode);
         return this;
      }


      @XmlAttribute
      public Integer getCacheStopTimeout() {
         return cacheStopTimeout;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #cacheStopTimeout(Integer)} instead
       */
      @Deprecated
      public void setCacheStopTimeout(Integer cacheStopTimeout) {
         testImmutability("cacheStopTimeout");
         this.cacheStopTimeout = cacheStopTimeout;
      }

      @Override
      public TransactionConfig cacheStopTimeout(Integer cacheStopTimeout) {
         setCacheStopTimeout(cacheStopTimeout);
         return this;
      }

      @Override
      public RecoveryConfig recovery() {
         recovery.setEnabled(true);
         recovery.setConfiguration(config);
         return recovery;
      }

      @Override
      protected TransactionType setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      public TransactionMode getTransactionMode() {
         return transactionMode;
      }

      @XmlAttribute
      public void setTransactionMode(TransactionMode transactionMode) {
         testImmutability("transactionMode");
         this.transactionMode = transactionMode;
      }

      public LockingMode getLockingMode() {
         return lockingMode;
      }

      @XmlAttribute
      public void setLockingMode(LockingMode lockingMode) {
         testImmutability("lockingMode");
         this.lockingMode = lockingMode;
      }

      @XmlAttribute
      private void setAutoCommit(Boolean autoCommit) {
         testImmutability("autoCommit");
         this.autoCommit = autoCommit;
      }

      public Boolean getAutoCommit() {
         return autoCommit;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof TransactionType)) return false;

         TransactionType that = (TransactionType) o;

         if (syncCommitPhase != null ? !syncCommitPhase.equals(that.syncCommitPhase) : that.syncCommitPhase != null)
            return false;
         if (syncRollbackPhase != null ? !syncRollbackPhase.equals(that.syncRollbackPhase) : that.syncRollbackPhase != null)
            return false;
         if (transactionManagerLookup != null ? !transactionManagerLookup.equals(that.transactionManagerLookup) : that.transactionManagerLookup != null)
            return false;
         if (transactionManagerLookupClass != null ? !transactionManagerLookupClass.equals(that.transactionManagerLookupClass) : that.transactionManagerLookupClass != null)
            return false;
         if (useEagerLocking != null ? !useEagerLocking.equals(that.useEagerLocking) : that.useEagerLocking != null)
            return false;
         if (cacheStopTimeout != null ? !cacheStopTimeout.equals(that.cacheStopTimeout) : that.cacheStopTimeout != null)
            return false;
         if (transactionMode != null ? !transactionMode.equals(that.transactionMode) : that.transactionMode != null)
            return false;
         if (lockingMode != null ? !lockingMode.equals(that.lockingMode) : that.lockingMode != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = transactionManagerLookupClass != null ? transactionManagerLookupClass.hashCode() : 0;
         result = 31 * result + (transactionManagerLookup != null ? transactionManagerLookup.hashCode() : 0);
         result = 31 * result + (syncCommitPhase != null ? syncCommitPhase.hashCode() : 0);
         result = 31 * result + (syncRollbackPhase != null ? syncRollbackPhase.hashCode() : 0);
         result = 31 * result + (useEagerLocking != null ? useEagerLocking.hashCode() : 0);
         result = 31 * result + (cacheStopTimeout != null ? cacheStopTimeout.hashCode() : 0);
         return result;
      }

      @Override
      public void willUnmarshall(Object parent) {
         // set the REAL default now!!
         // make sure we use the setter so that the change is registered for merging
         setTransactionManagerLookupClass(GenericTransactionManagerLookup.class.getName());
      }

      @Override
      public TransactionType clone() throws CloneNotSupportedException {
         TransactionType dolly = (TransactionType) super.clone();
         if (recovery != null)
            dolly.recovery = (RecoveryType) recovery.clone();
         return dolly;
      }
   }

   /**
    * Defines the local, in-VM locking and concurrency characteristics of the cache.
    *
    * @see <a href="../../../config.html#ce_default_locking">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "locking")
   @Deprecated public static class LockingType extends AbstractFluentConfigurationBean implements LockingConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 8142143187082119506L;

      @Dynamic
      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setLockAcquisitionTimeout")
      protected Long lockAcquisitionTimeout = 10000L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setIsolationLevel")
      protected IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setWriteSkewCheck")
      protected Boolean writeSkewCheck = false;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setUseLockStriping")
      protected Boolean useLockStriping = false;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setConcurrencyLevel")
      protected Integer concurrencyLevel = 32;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitLockingType(this);
      }


      @XmlAttribute
      public Long getLockAcquisitionTimeout() {
         return lockAcquisitionTimeout;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #lockAcquisitionTimeout(Long)} instead
       */
      @Deprecated
      public void setLockAcquisitionTimeout(Long lockAcquisitionTimeout) {
         testImmutability("lockAcquisitionTimeout");
         this.lockAcquisitionTimeout = lockAcquisitionTimeout;
      }

      @Override
      public LockingConfig lockAcquisitionTimeout(Long lockAcquisitionTimeout) {
         setLockAcquisitionTimeout(lockAcquisitionTimeout);
         return this;
      }


      @XmlAttribute
      public IsolationLevel getIsolationLevel() {
         return isolationLevel;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #isolationLevel(org.infinispan.util.concurrent.IsolationLevel)} instead
       */
      @Deprecated
      public void setIsolationLevel(IsolationLevel isolationLevel) {
         testImmutability("isolationLevel");
         this.isolationLevel = isolationLevel;
      }

      @Override
      public LockingConfig isolationLevel(IsolationLevel isolationLevel) {
         setIsolationLevel(isolationLevel);
         return this;
      }


      @XmlAttribute
      public Boolean isWriteSkewCheck() {
         return writeSkewCheck;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #writeSkewCheck(Boolean)} instead
       */
      @Deprecated
      public void setWriteSkewCheck(Boolean writeSkewCheck) {
         testImmutability("writeSkewCheck");
         this.writeSkewCheck = writeSkewCheck;
      }

      @Override
      public LockingConfig writeSkewCheck(Boolean writeSkewCheck) {
         setWriteSkewCheck(writeSkewCheck);
         return this;
      }


      @XmlAttribute
      public Boolean isUseLockStriping() {
         return useLockStriping;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #useLockStriping(Boolean)} instead
       */
      @Deprecated
      public void setUseLockStriping(Boolean useLockStriping) {
         testImmutability("useLockStriping");
         this.useLockStriping = useLockStriping;
      }

      @Override
      public LockingConfig useLockStriping(Boolean useLockStriping) {
         setUseLockStriping(useLockStriping);
         return this;
      }


      @XmlAttribute
      public Integer getConcurrencyLevel() {
         return concurrencyLevel;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #concurrencyLevel(Integer)} instead
       */
      @Deprecated
      public void setConcurrencyLevel(Integer concurrencyLevel) {
         testImmutability("concurrencyLevel");
         this.concurrencyLevel = concurrencyLevel;
      }

      @Override
      public LockingConfig concurrencyLevel(Integer concurrencyLevel) {
         setConcurrencyLevel(concurrencyLevel);
         return this;
      }

      @Override
      protected LockingType setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof LockingType)) return false;

         LockingType that = (LockingType) o;

         if (concurrencyLevel != null ? !concurrencyLevel.equals(that.concurrencyLevel) : that.concurrencyLevel != null)
            return false;
         if (isolationLevel != that.isolationLevel) return false;
         if (lockAcquisitionTimeout != null ? !lockAcquisitionTimeout.equals(that.lockAcquisitionTimeout) : that.lockAcquisitionTimeout != null)
            return false;
         if (useLockStriping != null ? !useLockStriping.equals(that.useLockStriping) : that.useLockStriping != null)
            return false;
         if (writeSkewCheck != null ? !writeSkewCheck.equals(that.writeSkewCheck) : that.writeSkewCheck != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = lockAcquisitionTimeout != null ? lockAcquisitionTimeout.hashCode() : 0;
         result = 31 * result + (isolationLevel != null ? isolationLevel.hashCode() : 0);
         result = 31 * result + (writeSkewCheck != null ? writeSkewCheck.hashCode() : 0);
         result = 31 * result + (useLockStriping != null ? useLockStriping.hashCode() : 0);
         result = 31 * result + (concurrencyLevel != null ? concurrencyLevel.hashCode() : 0);
         return result;
      }
   }

   /**
    * Recovery makes sure data in both transactional resource and Infinispan end up in a consistent state.
    * Fore more details see
    * <a href="https://docs.jboss.org/author/display/ISPN/Transaction+recovery">Infinispan Transaction Recovery</a>.
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "recovery", parentName = "transaction")
   @Deprecated public static class RecoveryType extends AbstractFluentConfigurationBean implements RecoveryConfig {

      /** The serialVersionUID */
      private static final long serialVersionUID = 7727835976746044904L;

      public static final String DEFAULT_RECOVERY_INFO_CACHE = "__recoveryInfoCacheName__";

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "isTransactionRecoveryEnabled")
      private boolean enabled = false;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "getTransactionRecoveryCacheName")
      private String recoveryInfoCacheName = DEFAULT_RECOVERY_INFO_CACHE;

      @Override
      public RecoveryConfig disable() {
         return setEnabled(false);
      }

      @XmlAttribute(required = false)
      public boolean isEnabled() {
         return enabled;
      }

      /**
       * Needed for JAXB
       */
      private RecoveryType setEnabled(boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
         return this;
      }

      @Override
      public RecoveryConfig recoveryInfoCacheName(String cacheName) {
         return setRecoveryInfoCacheName(cacheName);
      }

      @XmlAttribute (required = false)
      public String getRecoveryInfoCacheName() {
         return recoveryInfoCacheName;
      }

      /**
       * Needed for JAXB
       */
      private RecoveryType setRecoveryInfoCacheName(String recoveryInfoCacheName) {
         testImmutability("recoveryInfoCacheName");
         this.recoveryInfoCacheName = recoveryInfoCacheName;
         return this;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitRecoveryType(this);
      }

      @Override
      public TransactionConfig lockingMode(LockingMode lockingMode) {
         return transaction().lockingMode(lockingMode);
      }

      @Override
      public TransactionConfig autoCommit(boolean enabled) {
         return transaction().autoCommit(enabled);
      }

      @Override
      public TransactionConfig transactionMode(TransactionMode transactionMode) {
         return transaction().transactionMode(transactionMode);
      }

      @Override
      public TransactionType use1PcForAutoCommitTransactions(boolean b) {
         return transaction().use1PcForAutoCommitTransactions(b);
      }
   }

   /**
    * Defines clustered characteristics of the cache.
    *
    * @see <a href="../../../config.html#ce_default_clustering">Configuration reference</a>
    */
   @XmlJavaTypeAdapter(ClusteringTypeAdapter.class)
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @XmlType(propOrder = {})
   @ConfigurationDoc(name = "clustering")
   @Deprecated public static class ClusteringType extends AbstractFluentConfigurationBean implements ClusteringConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 4048135465543498430L;

      @XmlAttribute(name = "mode")
      protected String stringMode;

      @XmlTransient
      protected boolean configuredAsync = false;

      @XmlTransient
      protected boolean configuredSync = false;

      @XmlTransient
      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setCacheMode")
      protected CacheMode mode;

      @XmlElement
      protected SyncType sync = new SyncType();

      @XmlElement
      protected StateRetrievalType stateRetrieval = new StateRetrievalType();

      @XmlElement
      protected L1Type l1 = new L1Type();

      @XmlElement
      protected AsyncType async = new AsyncType(false);

      @XmlElement
      protected HashType hash = new HashType();

      public ClusteringType(CacheMode mode) {
         this.mode = mode;
      }

      public ClusteringType() {
         this.mode = DIST_SYNC;
      }

      @Override
      public AsyncConfig async() {
         if (configuredSync)
            throw new ConfigurationException("Already configured as sync");
         configuredAsync = true;
         async.setConfiguration(config);
         return async;
      }

      @Override
      public SyncConfig sync() {
         if (configuredAsync)
            throw new ConfigurationException("Already configured as async");
         configuredSync = true;
         sync.setConfiguration(config);
         return sync;
      }

      @Override
      public StateRetrievalConfig stateRetrieval() {
         stateRetrieval.setConfiguration(config);
         return stateRetrieval;
      }

      @Override
      public L1Config l1() {
         l1.setEnabled(true);
         l1.activate();
         l1.setConfiguration(config);
         return l1;
      }

      @Override
      public HashConfig hash() {
         hash.setConfiguration(config);
         hash.activate();
         return hash;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #mode(CacheMode)}
       */
      @Deprecated
      public void setMode(CacheMode mode) {
         testImmutability("mode");
         this.mode = mode;
      }

      @Override
      public ClusteringConfig mode(CacheMode mode) {
         testImmutability("mode");
         this.mode = mode;
         return this;
      }

      public boolean isSynchronous() {
         return !async.readFromXml;
      }

      @Override
      protected ClusteringType setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public ClusteringType clone() throws CloneNotSupportedException {
         ClusteringType dolly = (ClusteringType) super.clone();
         dolly.sync = (SyncType) sync.clone();
         dolly.stateRetrieval = (StateRetrievalType) stateRetrieval.clone();
         dolly.l1 = (L1Type) l1.clone();
         dolly.async = (AsyncType) async.clone();
         dolly.hash = hash.clone();
         return dolly;
      }

      public void accept(ConfigurationBeanVisitor v) {
         async.accept(v);
         hash.accept(v);
         l1.accept(v);
         stateRetrieval.accept(v);
         sync.accept(v);
         v.visitClusteringType(this);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof ClusteringType)) return false;

         ClusteringType that = (ClusteringType) o;

         if (async != null ? !async.equals(that.async) : that.async != null) return false;
         if (hash != null ? !hash.equals(that.hash) : that.hash != null) return false;
         if (l1 != null ? !l1.equals(that.l1) : that.l1 != null) return false;
         if (mode != that.mode) return false;
         if (stateRetrieval != null ? !stateRetrieval.equals(that.stateRetrieval) : that.stateRetrieval != null)
            return false;
         if (sync != null ? !sync.equals(that.sync) : that.sync != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = stringMode != null ? stringMode.hashCode() : 0;
         result = 31 * result + (mode != null ? mode.hashCode() : 0);
         result = 31 * result + (sync != null ? sync.hashCode() : 0);
         result = 31 * result + (stateRetrieval != null ? stateRetrieval.hashCode() : 0);
         result = 31 * result + (l1 != null ? l1.hashCode() : 0);
         result = 31 * result + (async != null ? async.hashCode() : 0);
         result = 31 * result + (hash != null ? hash.hashCode() : 0);
         return result;
      }

      @Override
      public void willUnmarshall(Object parent) {
         // set the REAL default now!!
         // must use the setter to ensure the change is registered on the bean
         setMode(DIST_SYNC);
      }
   }

   @Deprecated public static class ClusteringTypeAdapter extends XmlAdapter<ClusteringType, ClusteringType> {

      @Override
      public ClusteringType marshal(ClusteringType ct) throws Exception {
         return ct;
      }

      @Override
      public ClusteringType unmarshal(ClusteringType ct) throws Exception {
         if (ct.stringMode != null) {
            String mode = ct.stringMode.toLowerCase();
            if (mode.startsWith("r")) {
               if (ct.isSynchronous())
                  ct.setMode(REPL_SYNC);
               else
                  ct.setMode(REPL_ASYNC);
            } else if (mode.startsWith("i")) {
               if (ct.isSynchronous())
                  ct.setMode(INVALIDATION_SYNC);
               else
                  ct.setMode(INVALIDATION_ASYNC);
            } else if (mode.startsWith("d")) {
               if (ct.isSynchronous())
                  ct.setMode(DIST_SYNC);
               else
                  ct.setMode(DIST_ASYNC);
            } else if (mode.startsWith("l")) {
               ct.setMode(LOCAL);
            } else {
               throw new ConfigurationException("Invalid clustering mode " + ct.stringMode);
            }
         }
         return ct;
      }
   }

   /**
    * If this element is present, all communications are asynchronous, in that whenever a thread sends a message sent
    * over the wire, it does not wait for an acknowledgment before returning. This element is mutually exclusive with
    * the  <code> &lt;sync /&gt;</code> element.<br /> <br /> Characteristics of this can be tuned here.
    *
    * @see <a href="../../../config.html#ce_clustering_async">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "async", parentName = "clustering")
   @Deprecated public static class AsyncType extends AbstractFluentConfigurationBean implements AsyncConfig {

      @XmlTransient
      private boolean readFromXml = false;

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -7726319188826197399L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setUseReplQueue")
      protected Boolean useReplQueue = false;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setReplQueueMaxElements")
      protected Integer replQueueMaxElements = 1000;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setReplQueueInterval")
      protected Long replQueueInterval = 5000L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setUseAsyncMarshalling")
      protected Boolean asyncMarshalling = false;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setReplQueueClass")
      protected String replQueueClass = ReplicationQueueImpl.class.getName();

      @XmlTransient
      private boolean unmarshalledFromXml = false;

      private AsyncType(boolean readFromXml) {
         super();
         this.readFromXml = readFromXml;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitAsyncType(this);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof AsyncType)) return false;

         AsyncType asyncType = (AsyncType) o;

         if (readFromXml != asyncType.readFromXml) return false;
         if (asyncMarshalling != null ? !asyncMarshalling.equals(asyncType.asyncMarshalling) : asyncType.asyncMarshalling != null)
            return false;
         if (replQueueInterval != null ? !replQueueInterval.equals(asyncType.replQueueInterval) : asyncType.replQueueInterval != null)
            return false;
         if (replQueueMaxElements != null ? !replQueueMaxElements.equals(asyncType.replQueueMaxElements) : asyncType.replQueueMaxElements != null)
            return false;
         if (useReplQueue != null ? !useReplQueue.equals(asyncType.useReplQueue) : asyncType.useReplQueue != null)
            return false;
         if (!Util.safeEquals(replQueueClass, asyncType.replQueueClass))
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = (readFromXml ? 1 : 0);
         result = 31 * result + (useReplQueue != null ? useReplQueue.hashCode() : 0);
         result = 31 * result + (replQueueMaxElements != null ? replQueueMaxElements.hashCode() : 0);
         result = 31 * result + (replQueueInterval != null ? replQueueInterval.hashCode() : 0);
         result = 31 * result + (asyncMarshalling != null ? asyncMarshalling.hashCode() : 0);
         result = 31 * result + (replQueueClass != null ? replQueueClass.hashCode() : 0);
         return result;
      }

      private AsyncType() {
         this(true);
      }

      @XmlAttribute
      public Boolean isUseReplQueue() {
         return useReplQueue;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #useReplQueue(Boolean)}
       */
      @Deprecated
      public void setUseReplQueue(Boolean useReplQueue) {
         testImmutability("useReplQueue");
         this.useReplQueue = useReplQueue;
      }

      @Override
      public AsyncConfig useReplQueue(Boolean useReplQueue) {
         setUseReplQueue(useReplQueue);
         return this;
      }


      @XmlAttribute
      public Integer getReplQueueMaxElements() {
         return replQueueMaxElements;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #replQueueMaxElements(Integer)}
       */
      @Deprecated
      public void setReplQueueMaxElements(Integer replQueueMaxElements) {
         testImmutability("replQueueMaxElements");
         this.replQueueMaxElements = replQueueMaxElements;
      }

      @Override
      public AsyncConfig replQueueMaxElements(Integer replQueueMaxElements) {
         setReplQueueMaxElements(replQueueMaxElements);
         return this;
      }


      @XmlAttribute
      public Long getReplQueueInterval() {
         return replQueueInterval;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #replQueueInterval(Long)}
       */
      @Deprecated
      public void setReplQueueInterval(Long replQueueInterval) {
         testImmutability("replQueueInterval");
         this.replQueueInterval = replQueueInterval;
      }

      @Override
      public AsyncConfig replQueueInterval(Long replQueueInterval) {
         setReplQueueInterval(replQueueInterval);
         return this;
      }


      @XmlAttribute
      public Boolean isAsyncMarshalling() {
         return asyncMarshalling;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #asyncMarshalling(Boolean)}
       */
      @Deprecated
      public void setAsyncMarshalling(Boolean asyncMarshalling) {
         testImmutability("asyncMarshalling");
         this.asyncMarshalling = asyncMarshalling;
      }

      @Override
      public AsyncConfig asyncMarshalling(Boolean asyncMarshalling) {
         setAsyncMarshalling(asyncMarshalling);
         return this;
      }


      @XmlAttribute
      public String getReplQueueClass() {
         return replQueueClass;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #replQueueClass(Class)}
       */
      @Deprecated
      public void setReplQueueClass(String replQueueClass) {
         testImmutability("replQueueClass");
         this.replQueueClass = replQueueClass;
      }

      @Override
      public AsyncConfig replQueueClass(Class<? extends ReplicationQueue> replQueueClass) {
         setReplQueueClass(replQueueClass.getName());
         return this;
      }


      @Override
      public void willUnmarshall(Object parent) {
         ClusteringType clustering = (ClusteringType) parent;
         if (clustering.sync.unmarshalledFromXml)
            throw new ConfigurationException("Cannot have both <sync /> and <async /> tags in a <clustering /> tag!");
         unmarshalledFromXml = true;
      }

   }

   /**
    * This element controls the default expiration settings for entries in the cache.
    *
    * @see <a href="../../../config.html#ce_default_expiration">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "expiration")
   @Deprecated public static class ExpirationType extends AbstractFluentConfigurationBean implements ExpirationConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 5757161438110848530L;

      @ConfigurationDocRef(bean = ExpirationConfig.class, targetElement = "lifespan")
      protected Long lifespan = -1L;

      @ConfigurationDocRef(bean = ExpirationConfig.class, targetElement = "maxIdle")
      protected Long maxIdle = -1L;

      @ConfigurationDocRef(bean = ExpirationConfig.class, targetElement = "wakeUpInterval")
      protected Long wakeUpInterval = TimeUnit.MINUTES.toMillis(1);

      @ConfigurationDocRef(bean = ExpirationConfig.class, targetElement = "reaperEnabled")
      @XmlAttribute
      protected Boolean reaperEnabled = true;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitExpirationType(this);
      }

      @XmlAttribute
      public Long getLifespan() {
         return lifespan;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #lifespan(Long)}
       */
      @Deprecated
      public void setLifespan(Long lifespan) {
         testImmutability("lifespan");
         this.lifespan = lifespan;
      }

      @Override
      public ExpirationConfig lifespan(Long lifespan) {
         setLifespan(lifespan);
         return this;
      }

      @XmlAttribute
      public Long getMaxIdle() {
         return maxIdle;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #maxIdle(Long)}
       */
      @Deprecated
      public void setMaxIdle(Long maxIdle) {
         testImmutability("maxIdle");
         this.maxIdle = maxIdle;
      }

      @Override
      public ExpirationConfig maxIdle(Long maxIdle) {
         setMaxIdle(maxIdle);
         return this;
      }

      @Override
      public ExpirationConfig wakeUpInterval(Long wakeUpInterval) {
         setWakeUpInterval(wakeUpInterval);
         return this;
      }

      @XmlAttribute
      private void setWakeUpInterval(Long wakeUpInterval) {
         testImmutability("wakeUpInterval");
         this.wakeUpInterval = wakeUpInterval;
      }

      public Long getWakeUpInterval() {
         return wakeUpInterval;
      }

      @Override
      public ExpirationConfig reaperEnabled(Boolean enabled) {
         testImmutability("reaperEnabled");
         this.reaperEnabled = enabled;
         return this;
      }

      @Override
      protected ExpirationType setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof ExpirationType)) return false;

         ExpirationType that = (ExpirationType) o;

         if (!Util.safeEquals(lifespan, that.lifespan)) return false;
         if (!Util.safeEquals(maxIdle, that.maxIdle)) return false;
         if (!Util.safeEquals(wakeUpInterval, that.wakeUpInterval)) return false;
         if (!Util.safeEquals(reaperEnabled, that.reaperEnabled)) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = lifespan != null ? lifespan.hashCode() : 0;
         result = 31 * result + (maxIdle != null ? maxIdle.hashCode() : 0);
         result = 31 * result + (wakeUpInterval != null ? wakeUpInterval.hashCode() : 0);
         result = 31 * result + (reaperEnabled != null ? reaperEnabled.hashCode() : 0);
         return result;
      }
   }

   /**
    * This element controls the eviction settings for the cache.
    *
    * @see <a href="../../../config.html#ce_default_eviction">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "eviction")
   @Deprecated public static class EvictionType extends AbstractFluentConfigurationBean implements EvictionConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -1248563712058858791L;

      @Deprecated
      @ConfigurationDocRef(bean = EvictionType.class, targetElement = "setWakeUpInterval")
      protected Long wakeUpInterval = Long.MIN_VALUE;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setEvictionStrategy")
      protected EvictionStrategy strategy = EvictionStrategy.NONE;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setEvictionMaxEntries")
      protected Integer maxEntries = -1;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setEvictionThreadPolicy")
      protected EvictionThreadPolicy threadPolicy = EvictionThreadPolicy.DEFAULT;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitEvictionType(this);
      }

      /**
       * @deprecated Use {@link org.infinispan.config.Configuration#getExpirationWakeUpInterval()}
       */
      @XmlAttribute
      @Deprecated
      public Long getWakeUpInterval() {
         return wakeUpInterval;
      }

      /**
       * Deprecated setting. Please use wakeUpInterval of expiration.
       * @deprecated Use {@link ExpirationConfig#wakeUpInterval(Long)}
       */
      @Deprecated
      public void setWakeUpInterval(Long wakeUpInterval) {
         testImmutability("wakeUpInterval");
         this.wakeUpInterval = wakeUpInterval;
      }

      @XmlAttribute
      public EvictionStrategy getStrategy() {
         return strategy;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #strategy(EvictionStrategy)}
       */
      @Deprecated
      public void setStrategy(EvictionStrategy strategy) {
         testImmutability("strategy");
         this.strategy = strategy;
      }

      @Override
      public EvictionConfig strategy(EvictionStrategy strategy) {
         setStrategy(strategy);
         return this;
      }

      @XmlAttribute
      public EvictionThreadPolicy getThreadPolicy() {
         return threadPolicy;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #threadPolicy(EvictionThreadPolicy)}
       */
      @Deprecated
      public void setThreadPolicy(EvictionThreadPolicy threadPolicy) {
         testImmutability("threadPolicy");
         this.threadPolicy = threadPolicy;
      }

      @Override
      public EvictionConfig threadPolicy(EvictionThreadPolicy threadPolicy) {
         setThreadPolicy(threadPolicy);
         return this;
      }

      @XmlAttribute
      public Integer getMaxEntries() {
         return maxEntries;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #maxEntries(Integer)}
       */
      @Deprecated
      public void setMaxEntries(Integer maxEntries) {
         testImmutability("maxEntries");
         this.maxEntries = maxEntries;
      }

      @Override
      public EvictionConfig maxEntries(Integer maxEntries) {
         setMaxEntries(maxEntries);
         return this;
      }

      @Override
      protected EvictionType setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof EvictionType)) return false;

         EvictionType that = (EvictionType) o;

         if (maxEntries != null ? !maxEntries.equals(that.maxEntries) : that.maxEntries != null) return false;
         if (strategy != that.strategy) return false;
         if (threadPolicy != that.threadPolicy) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = strategy != null ? strategy.hashCode() : 0;
         result = 31 * result + (threadPolicy != null ? threadPolicy.hashCode() : 0);
         result = 31 * result + (maxEntries != null ? maxEntries.hashCode() : 0);
         return result;
      }
   }

   /**
    * This element controls the data container for the cache.
    *
    * @see <a href="../../../config.html#todo">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "dataContainer")
   @Deprecated public static class DataContainerType extends AbstractFluentConfigurationBean implements DataContainerConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -959027510815676570L;

      @ConfigurationDocRef(name = "class", bean = Configuration.class, targetElement = "getDataContainerClass")
      protected String dataContainerClass = DefaultDataContainer.class.getName();

      @XmlElement(name = "properties")
      protected TypedProperties properties = new TypedProperties();

      protected DataContainer dataContainer;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitDataContainerType(this);
      }

      @XmlAttribute(name = "class")
      public String getDataContainerClass() {
         return dataContainerClass;
      }

      /**
       * Needed for JAXB
       */
      private DataContainerType setDataContainerClass(String dataContainerClass) {
         testImmutability("dataContainerClass");
         this.dataContainerClass = dataContainerClass;
         return this;
      }

      @Override
      public DataContainerConfig dataContainerClass(Class<? extends DataContainer> dataContainerClass) {
         return setDataContainerClass(dataContainerClass.getName());
      }

      @Override
      public DataContainerConfig withProperties(Properties properties) {
         testImmutability("properties");
         this.properties = toTypedProperties(properties);
         return this;
      }

      @Override
      public DataContainerConfig addProperty(String key, String value) {
         if (this.properties == null) {
            this.properties= new TypedProperties();
         }
         this.properties.setProperty(key, value);
         return this;
      }

      @Override
      public DataContainerConfig dataContainer(DataContainer dataContainer) {
         this.dataContainer = dataContainer;
         return this;
      }

      @Override
      protected DataContainerType setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof DataContainerType)) return false;

         DataContainerType that = (DataContainerType) o;

         if (dataContainerClass != null ? !dataContainerClass.equals(that.dataContainerClass) : that.dataContainerClass != null)
            return false;
         if (dataContainer != null ? !dataContainer.equals(that.dataContainer) : that.dataContainer != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = dataContainerClass != null ? dataContainerClass.hashCode() : 0;
         result = 31 * result + (dataContainer != null ? dataContainer.hashCode() : 0);
         return result;
      }
   }

   /**
    * Configures how state is retrieved when a new cache joins the cluster. This element is only used with invalidation
    * and replication clustered modes.
    *
    * @see <a href="../../../config.html#ce_clustering_stateRetrieval">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "stateRetrieval")
   @Deprecated public static class StateRetrievalType extends AbstractFluentConfigurationBean implements StateRetrievalConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 3709234918426217096L;

      // Do not switch default value to true, otherwise DIST caches have to explicitly disable it.
      @Dynamic
      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setFetchInMemoryState")
      protected Boolean fetchInMemoryState = false;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setAlwaysProvideInMemoryState")
      protected Boolean alwaysProvideInMemoryState = false;

      @Dynamic
      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setStateRetrievalTimeout")
      protected Long timeout = 240000L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setStateRetrievalInitialRetryWaitTime")
      protected Long initialRetryWaitTime = 500L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setStateRetrievalRetryWaitTimeIncreaseFactor")
      protected Integer retryWaitTimeIncreaseFactor = 2;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setStateRetrievalNumRetries")
      protected Integer numRetries = 5;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setStateRetrievalLogFlushTimeout")
      protected Long logFlushTimeout = 60000L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setStateRetrievalMaxNonProgressingLogWrites")
      protected Integer maxNonProgressingLogWrites = 100;

      protected Integer chunkSize = 10000;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitStateRetrievalType(this);
      }

      @XmlAttribute
      public Boolean isFetchInMemoryState() {
         return fetchInMemoryState;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #fetchInMemoryState(Boolean)} instead
       */
      @Deprecated
      public void setFetchInMemoryState(Boolean fetchInMemoryState) {
         testImmutability("fetchInMemoryState");
         this.fetchInMemoryState = fetchInMemoryState;
      }

      @Override
      public StateRetrievalConfig fetchInMemoryState(Boolean fetchInMemoryState) {
         setFetchInMemoryState(fetchInMemoryState);
         return this;
      }


      @XmlAttribute
      public Boolean isAlwaysProvideInMemoryState() {
         return alwaysProvideInMemoryState;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #alwaysProvideInMemoryState(Boolean)} instead
       */
      @Deprecated
      public void setAlwaysProvideInMemoryState(Boolean alwaysProvideInMemoryState) {
         testImmutability("alwaysProvideInMemoryState");
         this.alwaysProvideInMemoryState = alwaysProvideInMemoryState;
      }

      @Override
      public StateRetrievalConfig alwaysProvideInMemoryState(Boolean alwaysProvideInMemoryState) {
         setAlwaysProvideInMemoryState(alwaysProvideInMemoryState);
         return this;
      }


      @XmlAttribute
      public Long getInitialRetryWaitTime() {
         return initialRetryWaitTime;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #initialRetryWaitTime(Long)} instead
       */
      @Deprecated
      public void setInitialRetryWaitTime(Long initialRetryWaitTime) {
         testImmutability("initialRetryWaitTime");
         this.initialRetryWaitTime = initialRetryWaitTime;
      }

      @Override
      public StateRetrievalConfig initialRetryWaitTime(Long initialRetryWaitTime) {
         setInitialRetryWaitTime(initialRetryWaitTime);
         return this;
      }


      @XmlAttribute
      public Integer getRetryWaitTimeIncreaseFactor() {
         return retryWaitTimeIncreaseFactor;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #retryWaitTimeIncreaseFactor(Integer)} instead
       */
      @Deprecated
      public void setRetryWaitTimeIncreaseFactor(Integer retryWaitTimeIncreaseFactor) {
         testImmutability("retryWaitTimeIncreaseFactor");
         this.retryWaitTimeIncreaseFactor = retryWaitTimeIncreaseFactor;
      }

      @Override
      public StateRetrievalConfig retryWaitTimeIncreaseFactor(Integer retryWaitTimeIncreaseFactor) {
         setRetryWaitTimeIncreaseFactor(retryWaitTimeIncreaseFactor);
         return this;
      }


      @XmlAttribute
      public Integer getNumRetries() {
         return numRetries;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #numRetries(Integer)} instead
       */
      @Deprecated
      public void setNumRetries(Integer numRetries) {
         testImmutability("numRetries");
         this.numRetries = numRetries;
      }

      @Override
      public StateRetrievalConfig numRetries(Integer numRetries) {
         setNumRetries(numRetries);
         return this;
      }


      @XmlAttribute
      public Long getTimeout() {
         return timeout;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #timeout(Long)} instead
       */
      @Deprecated
      public void setTimeout(Long timeout) {
         testImmutability("timeout");
         this.timeout = timeout;
      }

      @Override
      public StateRetrievalConfig timeout(Long timeout) {
         setTimeout(timeout);
         return this;
      }


      @XmlAttribute
      public Long getLogFlushTimeout() {
         return logFlushTimeout;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #logFlushTimeout(Long)} instead
       */
      @Deprecated
      public void setLogFlushTimeout(Long logFlushTimeout) {
         testImmutability("logFlushTimeout");
         this.logFlushTimeout = logFlushTimeout;
      }

      @Override
      public StateRetrievalConfig logFlushTimeout(Long logFlushTimeout) {
         setLogFlushTimeout(logFlushTimeout);
         return this;
      }


      @XmlAttribute
      public Integer getMaxNonProgressingLogWrites() {
         return maxNonProgressingLogWrites;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #maxNonProgressingLogWrites(Integer)} instead
       */
      @Deprecated
      public void setMaxNonProgressingLogWrites(Integer maxNonProgressingLogWrites) {
         testImmutability("maxNonProgressingLogWrites");
         this.maxNonProgressingLogWrites = maxNonProgressingLogWrites;
      }

      @Override
      public StateRetrievalConfig maxNonProgressingLogWrites(Integer maxNonProgressingLogWrites) {
         setMaxNonProgressingLogWrites(maxNonProgressingLogWrites);
         return this;
      }

      public Integer getChunkSize() {
         return chunkSize;
      }

      /**
       * @deprecated
       */
      @Deprecated
      public void setChunkSize(Integer chunkSize) {
         testImmutability("chunkSize");
         this.chunkSize = chunkSize;
      }

      @Override
      public StateRetrievalConfig chunkSize(Integer chunkSize) {
         setChunkSize(chunkSize);
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof StateRetrievalType)) return false;

         StateRetrievalType that = (StateRetrievalType) o;

         if (fetchInMemoryState != null ? !fetchInMemoryState.equals(that.fetchInMemoryState) : that.fetchInMemoryState != null)
            return false;
         if (timeout != null ? !timeout.equals(that.timeout) : that.timeout != null) return false;
         if (initialRetryWaitTime != null ? !initialRetryWaitTime.equals(that.initialRetryWaitTime) : that.initialRetryWaitTime != null)
            return false;
         if (retryWaitTimeIncreaseFactor != null ? !retryWaitTimeIncreaseFactor.equals(that.retryWaitTimeIncreaseFactor) : that.retryWaitTimeIncreaseFactor != null)
            return false;
         if (numRetries != null ? !numRetries.equals(that.numRetries) : that.numRetries != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = fetchInMemoryState != null ? fetchInMemoryState.hashCode() : 0;
         result = 31 * result + (timeout != null ? timeout.hashCode() : 0);
         result = 31 * result + (initialRetryWaitTime != null ? initialRetryWaitTime.hashCode() : 0);
         result = 31 * result + (retryWaitTimeIncreaseFactor != null ? retryWaitTimeIncreaseFactor.hashCode() : 0);
         result = 31 * result + (numRetries != null ? numRetries.hashCode() : 0);
         return result;
      }
   }

   /**
    * If this element is present, all communications are synchronous, in that whenever a thread sends a message sent
    * over the wire, it blocks until it receives an acknowledgment from the recipient. This element is mutually
    * exclusive with the <code> &lt;async /&gt;</code> element. <br /> <br /> Characteristics of this can be tuned
    * here.
    *
    * @see <a href="../../../config.html#ce_clustering_sync">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "sync")
   @Deprecated public static class SyncType extends AbstractFluentConfigurationBean implements SyncConfig {
      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 8419216253674289524L;

      @Dynamic
      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setSyncReplTimeout")
      protected Long replTimeout = 15000L;

      @XmlTransient
      private boolean unmarshalledFromXml = false;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitSyncType(this);
      }

      @XmlAttribute
      public Long getReplTimeout() {
         return replTimeout;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #replTimeout(Long)}
       */
      @Deprecated
      public void setReplTimeout(Long replTimeout) {
         testImmutability("replTimeout");
         this.replTimeout = replTimeout;
      }

      @Override
      public SyncConfig replTimeout(Long replTimeout) {
         setReplTimeout(replTimeout);
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof SyncType)) return false;

         SyncType syncType = (SyncType) o;

         if (replTimeout != null ? !replTimeout.equals(syncType.replTimeout) : syncType.replTimeout != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         return replTimeout != null ? replTimeout.hashCode() : 0;
      }

      @Override
      public void willUnmarshall(Object parent) {
         ClusteringType clustering = (ClusteringType) parent;
         if (clustering.async.unmarshalledFromXml)
            throw new ConfigurationException("Cannot have both <sync /> and <async /> tags in a <clustering /> tag!");
         unmarshalledFromXml = true;
      }
   }

   /**
    * Allows fine-tuning of rehashing characteristics. Only used with 'distributed' cache mode, and otherwise ignored.
    *
    * @see <a href="../../../config.html#ce_clustering_hash">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "hash")
   @Deprecated public static class HashType extends AbstractFluentConfigurationBean implements HashConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 752218766840948822L;

      @ConfigurationDocRef(name = "class", bean = HashConfig.class, targetElement = "setConsistentHashClass")
      protected String consistentHashClass;

      @ConfigurationDocRef(bean = HashConfig.class, targetElement = "hashFunctionClass")
      protected String hashFunctionClass = MurmurHash3.class.getName();

      @ConfigurationDocRef(bean = HashConfig.class, targetElement = "numOwners")
      protected Integer numOwners = 2;

      @ConfigurationDoc(desc = "Future flag. Currenly unused.")
      protected Long rehashWait = MINUTES.toMillis(1);

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setRehashRpcTimeout")
      protected Long rehashRpcTimeout = MINUTES.toMillis(10);

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setRehashEnabled")
      protected Boolean rehashEnabled = true;

      @ConfigurationDocRef(bean = HashConfig.class, targetElement = "numVirtualNodes")
      protected Integer numVirtualNodes = 1;

      @ConfigurationDocRef(bean = HashConfig.class, targetElement = "groups")
      protected GroupsConfiguration groups = new GroupsConfiguration();
      @XmlTransient
      public boolean activated = false;

      public void accept(ConfigurationBeanVisitor v) {
         groups.accept(v);
         v.visitHashType(this);
      }

      @XmlAttribute(name = "class")
      public String getConsistentHashClass() {
         return consistentHashClass;
      }

      /**
       * @deprecated No longer used since 5.2, use {@link org.infinispan.configuration.cache.HashConfigurationBuilder#consistentHashFactory(org.infinispan.distribution.ch.ConsistentHashFactory)} instead.
       */
      @Deprecated
      public void setConsistentHashClass(String consistentHashClass) {
         testImmutability("consistentHashClass");
         activate();
         this.consistentHashClass = consistentHashClass;
      }

      @Override
      public HashConfig consistentHashClass(Class<? extends ConsistentHash> consistentHashClass) {
         setConsistentHashClass(consistentHashClass.getName());
         return this;
      }


      @XmlAttribute
      public String getHashFunctionClass() {
         return hashFunctionClass;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #hashFunctionClass(Class)}
       */
      @Deprecated
      public void setHashFunctionClass(String hashFunctionClass) {
         testImmutability("hashFunctionClass");
         activate();
         this.hashFunctionClass = hashFunctionClass;
      }

      private void activate() {
         activated = true;
         overriddenConfigurationElements.add("activated");
      }

      @Override
      public HashConfig hashFunctionClass(Class<? extends Hash> hashFunctionClass) {
         setHashFunctionClass(hashFunctionClass.getName());
         return this;
      }

      @XmlAttribute
      public Integer getNumOwners() {
         return numOwners;
      }

      @XmlAttribute
      public Integer getNumVirtualNodes() {
         return numVirtualNodes;
      }

      @Override
      public HashConfig numVirtualNodes(Integer numVirtualNodes) {
         setNumVirtualNodes(numVirtualNodes);
         return this;
      }

      /**
       * @deprecated No longer used since 5.2, use {@link org.infinispan.configuration.cache.HashConfigurationBuilder#numSegments(int)} instead.
       */
      @Deprecated
      public void setNumVirtualNodes(Integer numVirtualNodes) {
         testImmutability("numVirtualNodes");
         activate();
         this.numVirtualNodes = numVirtualNodes;
      }


      /**
       * @deprecated The visibility of this will be reduced, use {@link #numOwners(Integer)}
       */
      @Deprecated
      public void setNumOwners(Integer numOwners) {
         testImmutability("numOwners");
         activate();
         this.numOwners = numOwners;
      }

      @Override
      public HashConfig numOwners(Integer numOwners) {
         setNumOwners(numOwners);
         return this;
      }


      @XmlAttribute
      public Long getRehashWait() {
         return rehashWait;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #rehashWait(Long)}
       */
      @Deprecated
      public void setRehashWait(Long rehashWaitTime) {
         testImmutability("rehashWait");
         activate();
         this.rehashWait = rehashWaitTime;
      }

      @Override
      public HashConfig rehashWait(Long rehashWaitTime) {
         setRehashWait(rehashWaitTime);
         return this;
      }


      @XmlAttribute
      public Long getRehashRpcTimeout() {
         return rehashRpcTimeout;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #rehashRpcTimeout(Long)}
       */
      @Deprecated
      public void setRehashRpcTimeout(Long rehashRpcTimeout) {
         testImmutability("rehashRpcTimeout");
         activate();
         this.rehashRpcTimeout = rehashRpcTimeout;
      }

      @Override
      public HashConfig rehashRpcTimeout(Long rehashRpcTimeout) {
         setRehashRpcTimeout(rehashRpcTimeout);
         return this;
      }


      @XmlAttribute
      public Boolean isRehashEnabled() {
         return rehashEnabled;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #rehashEnabled(Boolean)}
       */
      @Deprecated
      public void setRehashEnabled(Boolean rehashEnabled) {
         testImmutability("rehashEnabled");
         activate();
         this.rehashEnabled = rehashEnabled;
      }

      @Override
      public HashConfig rehashEnabled(Boolean rehashEnabled) {
         setRehashEnabled(rehashEnabled);
         return this;
      }

      @Override
      public GroupsConfiguration groups() {
         groups.setConfiguration(config);
         activate();
         return groups;
      }

      @XmlElement
      public void setGroups(GroupsConfiguration groups) {
         testImmutability("groups");
         this.groups = groups;
      }


      public GroupsConfiguration getGroups() {
         return groups();
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof HashType)) return false;

         HashType hashType = (HashType) o;

         if (consistentHashClass != null ? !consistentHashClass.equals(hashType.consistentHashClass) : hashType.consistentHashClass != null)
            return false;
         if (hashFunctionClass != null ? !hashFunctionClass.equals(hashType.hashFunctionClass) : hashType.hashFunctionClass != null)
            return false;
         if (numOwners != null ? !numOwners.equals(hashType.numOwners) : hashType.numOwners != null) return false;
         if (numVirtualNodes != null ? !numVirtualNodes.equals(hashType.numVirtualNodes) : hashType.numVirtualNodes != null) return false;
         if (groups != null ? !groups.equals(hashType.groups) : hashType.groups != null) return false;
         if (rehashRpcTimeout != null ? !rehashRpcTimeout.equals(hashType.rehashRpcTimeout) : hashType.rehashRpcTimeout != null)
            return false;
         if (rehashWait != null ? !rehashWait.equals(hashType.rehashWait) : hashType.rehashWait != null) return false;
         if (rehashEnabled != hashType.rehashEnabled) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = consistentHashClass != null ? consistentHashClass.hashCode() : 0;
         result = 31 * result + (hashFunctionClass != null ? hashFunctionClass.hashCode() : 0);
         result = 31 * result + (numOwners != null ? numOwners.hashCode() : 0);
         result = 31 * result + (numVirtualNodes != null ? numVirtualNodes.hashCode() : 0);
         result = 31 * result + (groups != null ? groups.hashCode() : 0);
         result = 31 * result + (rehashWait != null ? rehashWait.hashCode() : 0);
         result = 31 * result + (rehashRpcTimeout != null ? rehashRpcTimeout.hashCode() : 0);
         result = 31 * result + (rehashEnabled ? 0 : 1);
         return result;
      }

      @Override
      public HashType clone() throws CloneNotSupportedException {
         HashType dolly = (HashType) super.clone();
         dolly.consistentHashClass = consistentHashClass;
         dolly.hashFunctionClass = hashFunctionClass;
         dolly.numOwners = numOwners;
         dolly.numVirtualNodes = numVirtualNodes;
         dolly.rehashEnabled = rehashEnabled;
         dolly.rehashRpcTimeout = rehashRpcTimeout;
         dolly.rehashWait = rehashWait;
         dolly.groups = groups.clone();
         return dolly;
      }
   }

   /**
    * This element configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes, this
    * element is ignored.
    *
    * @see <a href="../../../config.html#ce_clustering_l1">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "l1")
   @Deprecated public static class L1Type extends AbstractFluentConfigurationBean implements L1Config {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -4703587764861110638L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setL1CacheEnabled")
      protected Boolean enabled = true;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setL1Lifespan")
      protected Long lifespan = 600000L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setL1OnRehash")
      protected Boolean onRehash = true;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setL1InvalidationThreshold")
      protected Integer invalidationThreshold = 0;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setL1InvalidationReaperThreadFrequency")
      protected Long frequency = 600000L;

      @XmlTransient
      public boolean activated = false;

      private void activate() {
         activated = true;
         overriddenConfigurationElements.add("activated");
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitL1Type(this);
      }

      @XmlAttribute
      public Boolean isEnabled() {
         return enabled;
      }

      /**
       * @deprecated The visibility of this will be reduced
       */
      @Deprecated
      public L1Config setEnabled(Boolean enabled) {
         testImmutability("enabled");
         activate();
         this.enabled = enabled;
         return this;
      }

      @XmlAttribute
      public Long getLifespan() {
         return lifespan;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #lifespan(Long)}
       */
      @Deprecated
      public L1Config setLifespan(Long lifespan) {
         testImmutability("lifespan");
         activate();
         this.lifespan = lifespan;
         return this;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #invalidationReaperThreadFrequency(Long)}
       */
      @Deprecated
      public L1Config setL1InvalidationCleanupTaskFrequency(long frequencyMillis) {
         testImmutability("frequency");
         this.frequency = frequencyMillis;
         return this;
      }

      @Override
      public L1Config cleanupTaskFrequency(Long frequencyMillis) {
         return setL1InvalidationCleanupTaskFrequency(frequencyMillis);
      }

      @XmlAttribute (name = "cleanupTaskFrequency")
      public Long getL1InvalidationCleanupTaskFrequency() {
         return frequency;
      }

      @Override
      public L1Config lifespan(Long lifespan) {
         setLifespan(lifespan);
         return this;
      }


      @XmlAttribute
      public Boolean isOnRehash() {
         return onRehash;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #onRehash(Boolean)}
       */
      @Deprecated
      public L1Config setOnRehash(Boolean onRehash) {
         testImmutability("onRehash");
         activate();
         this.onRehash = onRehash;
         return this;
      }

      @Override
      public L1Config onRehash(Boolean onRehash) {
         setOnRehash(onRehash);
         return this;
      }

      @Override
      public L1Config invalidationThreshold(Integer threshold) {
         setInvalidationThreshold(threshold);
         return this;
      }


      public void setInvalidationThreshold(Integer threshold) {
         testImmutability("invalidationThreshold");
         activate();
         this.invalidationThreshold = threshold;
      }

      @XmlAttribute
      public Integer getInvalidationThreshold() {
	      return invalidationThreshold;
      }

      @Override
      public L1Config disable() {
         return setEnabled(false);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof L1Type)) return false;

         L1Type l1Type = (L1Type) o;

         if (enabled != null ? !enabled.equals(l1Type.enabled) : l1Type.enabled != null) return false;
         if (lifespan != null ? !lifespan.equals(l1Type.lifespan) : l1Type.lifespan != null) return false;
         if (onRehash != null ? !onRehash.equals(l1Type.onRehash) : l1Type.onRehash != null) return false;
         if (invalidationThreshold != null ? !invalidationThreshold.equals(l1Type.invalidationThreshold) : l1Type.invalidationThreshold != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = enabled != null ? enabled.hashCode() : 0;
         result = 31 * result + (lifespan != null ? lifespan.hashCode() : 0);
         result = 31 * result + (onRehash != null ? onRehash.hashCode() : 0);
         result = 31 * result + (invalidationThreshold != null ? invalidationThreshold.hashCode() : 0);
         return result;
      }
   }

   @XmlAccessorType(XmlAccessType.PROPERTY)
   @Deprecated public static class BooleanAttributeType extends AbstractFluentConfigurationBean {

      @XmlTransient
      protected final String fieldName;

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 2296863404153834686L;

      @ConfigurationDoc(desc = "Toggle switch")
      protected Boolean enabled = false;

      public BooleanAttributeType() {
         fieldName = "undefined";
      }

      public BooleanAttributeType(String fieldName) {
         this.fieldName = fieldName;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitBooleanAttributeType(this);
      }

      public String getFieldName() {
         return fieldName;
      }

      @XmlAttribute
      public Boolean isEnabled() {
         return enabled;
      }

      public BooleanAttributeType enabled(Boolean enabled) {
         setEnabled(enabled);
         return this;
      }

      public BooleanAttributeType disable() {
         setEnabled(false);
         return this;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #enabled(Boolean)} instead
       */
      @Deprecated
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof BooleanAttributeType)) return false;

         BooleanAttributeType that = (BooleanAttributeType) o;

         if (enabled != null ? !enabled.equals(that.enabled) : that.enabled != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return enabled != null ? enabled.hashCode() : 0;
      }
   }

   /**
    * A mechanism by which data is stored as a binary byte array. This allows
    * serialization and deserialization of objects is deferred till the point
    * in time in which they are used and needed. This typically means that any
    * deserialization happens using the thread context class loader of the
    * invocation that requires deserialization, and is an effective mechanism
    * to provide classloader isolation.
    *
    * @see <a href="../../../config.html#ce_default_lazyDeserialization">Configuration reference</a>
    */
   @ConfigurationDoc(name = "storeAsBinary")
   @Deprecated public static class StoreAsBinary extends BooleanAttributeType implements StoreAsBinaryConfig {

      @ConfigurationDoc(desc = "If enabled, keys are stored as binary, in their serialized form.  If false, keys are stored as object references.")
      @XmlAttribute
      private Boolean storeKeysAsBinary = true;
      @ConfigurationDoc(desc = "If enabled, values are stored as binary, in their serialized form.  If false, values are stored as object references.")
      @XmlAttribute
      private Boolean storeValuesAsBinary = true;
      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 7404820498857564962L;

      public StoreAsBinary() {
         super("storeAsBinary");
      }

      @Override
      protected StoreAsBinary setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public StoreAsBinary enabled(Boolean enabled) {
         super.enabled(enabled);
         return this;
      }

      @Override
      public StoreAsBinary disable() {
         super.disable();
         return this;
      }

      @Override
      public StoreAsBinaryConfig storeKeysAsBinary(Boolean storeKeysAsBinary) {
         testImmutability("storeKeysAsBinary");
         this.storeKeysAsBinary = storeKeysAsBinary;
         return this;
      }

      @Override
      public StoreAsBinaryConfig storeValuesAsBinary(Boolean storeValuesAsBinary) {
         testImmutability("storeValuesAsBinary");
         this.storeValuesAsBinary = storeValuesAsBinary;
         return this;
      }

      public Boolean isStoreKeysAsBinary() {
         return this.storeKeysAsBinary;
      }

      public Boolean isStoreValuesAsBinary() {
         return this.storeValuesAsBinary;
      }

      @Override
      public void accept(ConfigurationBeanVisitor v) {
         v.visitStoreAsBinaryType(this);
      }

      @Override
      public StoreAsBinary clone() {
         try {
            StoreAsBinary dolly = (StoreAsBinary) super.clone();
            dolly.storeKeysAsBinary = storeKeysAsBinary;
            dolly.storeValuesAsBinary = storeValuesAsBinary;
            return dolly;
         } catch (CloneNotSupportedException e) {
            throw new IllegalArgumentException("Should never get here");
         }
      }
   }

   /**
    * Deprecated configuration element. Use storeAsBinary instead.
    */
   @ConfigurationDoc(name = "lazyDeserialization")
   @Deprecated public static class LazyDeserialization extends BooleanAttributeType {

      public LazyDeserialization() {
         super("lazyDeserialization");
      }

      @Override
      public LazyDeserialization enabled(Boolean enabled) {
         log.lazyDeserializationDeprecated();
         super.enabled(enabled);
         return this;
      }

      @Override
      public LazyDeserialization disable() {
         log.lazyDeserializationDeprecated();
         super.disable();
         return this;
      }

      @Override
      public void setEnabled(Boolean enabled) {
         log.lazyDeserializationDeprecated();
         super.setEnabled(enabled);
      }

      @Override
      protected LazyDeserialization setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }
   }

   /**
    * This element specifies whether cache statistics are gathered and reported via JMX.
    *
    * @see <a href="../../../config.html#ce_default_jmxStatistics">Configuration reference</a>
    */
   @ConfigurationDoc(name = "jmxStatistics")
   @Deprecated public static class JmxStatistics extends BooleanAttributeType implements JmxStatisticsConfig {
      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 8716456707015486673L;

      public JmxStatistics() {
         super("jmxStatistics");
      }

      @Override
      protected JmxStatistics setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public JmxStatistics enabled(Boolean enabled) {
         super.enabled(enabled);
         return this;
      }

      @Override
      public JmxStatistics disable() {
         super.disable();
         return this;
      }
   }

   /**
    * Defines whether invocation batching is allowed in this cache instance, and sets up internals accordingly to allow
    * use of this API.
    *
    * @see <a href="../../../config.html#ce_default_invocationBatching">Configuration reference</a>
    */
   @ConfigurationDoc(name = "invocationBatching")
   @Deprecated public static class InvocationBatching extends BooleanAttributeType implements InvocationBatchingConfig {
      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 5854115656815587815L;

      public InvocationBatching() {
         super("invocationBatching");
      }

      @Override
      public InvocationBatching enabled(Boolean enabled) {
         super.enabled(enabled);
         return this;
      }

      @Override
      public void setEnabled(Boolean enabled) {
         super.setEnabled(enabled);
         updateTransactionMode();
      }

      @Override
      protected InvocationBatching setConfiguration(Configuration config) {
         super.setConfiguration(config);
         updateTransactionMode();
         return this;
      }

      @Override
      public InvocationBatching disable() {
         super.disable();
         return this;
      }

      private void updateTransactionMode() {
         if (enabled && config != null) config.transaction.transactionMode(TransactionMode.TRANSACTIONAL);
      }
   }

   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "versioning")
   @Deprecated
   public static class VersioningConfigurationBean extends AbstractFluentConfigurationBean implements VersioningConfig {
      private static final long serialVersionUID = -123456789001234L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setEnableVersioning")
      protected Boolean enabled = false;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setVersioningScheme")
      protected VersioningScheme versioningScheme = VersioningScheme.NONE;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitVersioningConfigurationBean(this);
      }

      @XmlAttribute
      public Boolean isEnabled() {
         return enabled;
      }

      /**
       * @deprecated The visibility of this will be reduced
       */
      @Deprecated
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      @XmlAttribute
      public VersioningScheme getVersioningScheme() {
         return versioningScheme;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #versioningScheme(org.infinispan.configuration.cache.VersioningScheme)}
       */
      @Deprecated
      public void setVersioningScheme(VersioningScheme versioningScheme) {
         testImmutability("versioningScheme");
         this.versioningScheme = versioningScheme;
      }

      @Override
      public VersioningConfigurationBean versioningScheme(VersioningScheme versioningScheme) {
         setVersioningScheme(versioningScheme);
         return this;
      }

      @Override
      protected VersioningConfigurationBean setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public VersioningConfigurationBean disable() {
         setEnabled(false);
         return this;
      }

      @Override
      public VersioningConfigurationBean enable() {
         setEnabled(true);
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof VersioningConfigurationBean)) return false;

         VersioningConfigurationBean that = (VersioningConfigurationBean) o;

         if (!Util.safeEquals(enabled, that.enabled)) return false;
         if (!Util.safeEquals(versioningScheme, that.versioningScheme)) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = enabled != null ? enabled.hashCode() : 0;
         result = 31 * result + (versioningScheme != null ? versioningScheme.hashCode() : 0);
         return result;
      }
   }

   /**
    * This element configures deadlock detection.
    *
    * @see <a href="../../../config.html#ce_default_deadlockDetection">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "deadlockDetection")
   @Deprecated public static class DeadlockDetectionType extends AbstractFluentConfigurationBean implements DeadlockDetectionConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -7178286048602531152L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setEnableDeadlockDetection")
      protected Boolean enabled = false;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setDeadlockDetectionSpinDuration")
      protected Long spinDuration = 100L;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitDeadlockDetectionType(this);
      }

      @XmlAttribute
      public Boolean isEnabled() {
         return enabled;
      }

      /**
       * @deprecated The visibility of this will be reduced
       */
      @Deprecated
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      @XmlAttribute
      public Long getSpinDuration() {
         return spinDuration;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #spinDuration(Long)}
       */
      @Deprecated
      public void setSpinDuration(Long spinDuration) {
         testImmutability("spinDuration");
         this.spinDuration = spinDuration;
      }

      @Override
      public DeadlockDetectionConfig spinDuration(Long spinDuration) {
         setSpinDuration(spinDuration);
         return this;
      }

      @Override
      protected DeadlockDetectionType setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public DeadlockDetectionConfig disable() {
         setEnabled(false);
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof DeadlockDetectionType)) return false;

         DeadlockDetectionType that = (DeadlockDetectionType) o;

         if (enabled != null ? !enabled.equals(that.enabled) : that.enabled != null) return false;
         if (spinDuration != null ? !spinDuration.equals(that.spinDuration) : that.spinDuration != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = enabled != null ? enabled.hashCode() : 0;
         result = 31 * result + (spinDuration != null ? spinDuration.hashCode() : 0);
         return result;
      }
   }

   /**
    * Allows you to tune various unsafe or non-standard characteristics. Certain operations such as Cache.put() that are
    * supposed to return the previous value associated with the specified key according to the java.util.Map contract
    * will not fulfill this contract if unsafe toggle is turned on. Use with care. See details at
    * <a href="https://docs.jboss.org/author/display/ISPN/Technical+FAQs">Technical FAQ</a>
    *
    * @see <a href="../../../config.html#ce_default_unsafe">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "unsafe")
   @Deprecated public static class UnsafeType extends AbstractFluentConfigurationBean implements UnsafeConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -9200921443651234163L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setUnsafeUnreliableReturnValues")
      protected Boolean unreliableReturnValues = false;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitUnsafeType(this);
      }

      @XmlAttribute
      public Boolean isUnreliableReturnValues() {
         return unreliableReturnValues;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #unreliableReturnValues(Boolean)} instead
       */
      @Deprecated
      public void setUnreliableReturnValues(Boolean unreliableReturnValues) {
         testImmutability("unreliableReturnValues");
         this.unreliableReturnValues = unreliableReturnValues;
      }

      @Override
      public UnsafeConfig unreliableReturnValues(Boolean unreliableReturnValues) {
         setUnreliableReturnValues(unreliableReturnValues);
         return this;
      }

      @Override
      protected UnsafeType setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof UnsafeType)) return false;

         UnsafeType that = (UnsafeType) o;

         if (unreliableReturnValues != null ? !unreliableReturnValues.equals(that.unreliableReturnValues) : that.unreliableReturnValues != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         return unreliableReturnValues != null ? unreliableReturnValues.hashCode() : 0;
      }
   }

   /**
    * Configures custom interceptors to be added to the cache.
    *
    * @see <a href="../../../config.html#ce_default_customInterceptors">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.FIELD)
   @ConfigurationDoc(name = "customInterceptors")
   @Deprecated public static class CustomInterceptorsType extends AbstractFluentConfigurationBean implements CustomInterceptorsConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 7187545782011884661L;

      @XmlElement(name = "interceptor")
      private List<CustomInterceptorConfig> customInterceptors = new LinkedList<CustomInterceptorConfig>();

      public CustomInterceptorsType() {
         testImmutability("customInterceptors");
      }

      @Override
      public CustomInterceptorsType clone() throws CloneNotSupportedException {
         CustomInterceptorsType dolly = (CustomInterceptorsType) super.clone();
         if (customInterceptors != null) {
            dolly.customInterceptors = new LinkedList<CustomInterceptorConfig>();
            for (CustomInterceptorConfig config : customInterceptors) {
               CustomInterceptorConfig clone = config.clone();
               dolly.customInterceptors.add(clone);
            }
         }
         return dolly;
      }

      public void accept(ConfigurationBeanVisitor v) {
         for (CustomInterceptorConfig i : customInterceptors) {
            i.accept(v);
         }
         v.visitCustomInterceptorsType(this);
      }

      public List<CustomInterceptorConfig> getCustomInterceptors() {
         return customInterceptors;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof CustomInterceptorsType)) return false;

         CustomInterceptorsType that = (CustomInterceptorsType) o;

         if (customInterceptors != null ? !customInterceptors.equals(that.customInterceptors) : that.customInterceptors != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         return customInterceptors != null ? customInterceptors.hashCode() : 0;
      }

      /**
       * @deprecated Use {@link #add(org.infinispan.interceptors.base.CommandInterceptor)} instead
       */
      @Deprecated
      public void setCustomInterceptors(List<CustomInterceptorConfig> customInterceptors) {
         testImmutability("customInterceptors");
         this.customInterceptors = customInterceptors;
      }

      @Override
      public CustomInterceptorPosition add(CommandInterceptor interceptor) {
         testImmutability("customInterceptors");
         return new CustomInterceptorPositionType(interceptor, this);
      }

      @Override
      protected CustomInterceptorsType setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }
   }

   @Deprecated public static class CustomInterceptorPositionType extends AbstractFluentConfigurationBean implements CustomInterceptorPosition {

      private final CommandInterceptor interceptor;
      private final CustomInterceptorsType type;

      public CustomInterceptorPositionType(CommandInterceptor interceptor, CustomInterceptorsType type) {
         this.interceptor = interceptor;
         this.type = type;
      }

      @Override
      public CustomInterceptorsConfig first() {
         CustomInterceptorConfig cfg = new CustomInterceptorConfig(interceptor, true, false, -1, "", "");
         type.getCustomInterceptors().add(cfg);
         return type;
      }

      @Override
      public CustomInterceptorsConfig last() {
         CustomInterceptorConfig cfg = new CustomInterceptorConfig(interceptor, false, true, -1, "", "");
         type.getCustomInterceptors().add(cfg);
         return type;
      }

      @Override
      public CustomInterceptorsConfig atIndex(int index) {
         CustomInterceptorConfig cfg = new CustomInterceptorConfig(interceptor, false, false, index, "", "");
         type.getCustomInterceptors().add(cfg);
         return type;
      }

      @Override
      public CustomInterceptorsConfig after(Class<? extends CommandInterceptor> interceptorClass) {
         CustomInterceptorConfig cfg = new CustomInterceptorConfig(interceptor, false, false, -1, interceptorClass.getName(), null);
         type.getCustomInterceptors().add(cfg);
         return type;
      }

      @Override
      public CustomInterceptorsConfig before(Class<? extends CommandInterceptor> interceptorClass) {
         CustomInterceptorConfig cfg = new CustomInterceptorConfig(interceptor, false, false, -1, null, interceptorClass.getName());
         type.getCustomInterceptors().add(cfg);
         return type;
      }
   }

   /**
    * Configures indexing of entries in the cache for searching. Note that infinispan-query.jar and its dependencies
    * needs to be available if this option is to be used.
    *
    * @see <a href="../../../config.html#ce_default_indexing">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "indexing")
   @Deprecated public static class QueryConfigurationBean extends AbstractFluentConfigurationBean implements IndexingConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 2891683014353342549L;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setIndexingEnabled")
      protected Boolean enabled = false;

      @ConfigurationDocRef(bean = Configuration.class, targetElement = "setIndexLocalOnly")
      protected Boolean indexLocalOnly = false;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitQueryConfigurationBean(this);
      }

      @XmlAttribute
      public Boolean isEnabled() {
         return enabled;
      }

      /**
       * @deprecated The visibility will be reduced
       */
      @Deprecated
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      @XmlAttribute
      public Boolean isIndexLocalOnly() {
         return indexLocalOnly;
      }

      /**
       * @deprecated Use {@link #indexLocalOnly(Boolean)} instead
       */
      @Deprecated
      public void setIndexLocalOnly(Boolean indexLocalOnly) {
         testImmutability("indexLocalOnly");
         this.indexLocalOnly = indexLocalOnly;
      }

      @Override
      public IndexingConfig indexLocalOnly(Boolean indexLocalOnly) {
         setIndexLocalOnly(indexLocalOnly);
         return this;
      }

      @XmlElement(name = "properties")
      protected TypedProperties properties = new TypedProperties();

      @Override
      public IndexingConfig withProperties(Properties properties) {
         testImmutability("properties");
         this.properties = toTypedProperties(properties);
         return this;
      }

      @Override
      public IndexingConfig addProperty(String key, String value) {
         if (properties == null) {
            properties = new TypedProperties();
         }
         this.properties.setProperty(key, value);
         return this;
      }

      @Override
      public IndexingConfig disable() {
         setEnabled(false);
         return this;
      }

      @Override
      protected QueryConfigurationBean setConfiguration(Configuration config) {
         super.setConfiguration(config);
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o)
            return true;
         if (!(o instanceof QueryConfigurationBean))
            return false;

         QueryConfigurationBean that = (QueryConfigurationBean) o;

         if (enabled != null ? !enabled.equals(that.enabled) : that.enabled != null)
            return false;

         if (indexLocalOnly != null ? !indexLocalOnly.equals(that.indexLocalOnly) : that.indexLocalOnly != null)
            return false;

         if (!properties.equals(that.properties))
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = enabled != null ? enabled.hashCode() : 0;
         result = 31 * result + (indexLocalOnly != null ? indexLocalOnly.hashCode() : 0);
         result = 31 * result + (properties != null ? properties.hashCode() : 0);
         return result;
      }

      @Override
      protected boolean hasComponentStarted() {
         return false;
      }

      @Override
      public QueryConfigurationBean clone() {
         try {
            QueryConfigurationBean dolly = (QueryConfigurationBean) super.clone();
            dolly.enabled = enabled;
            dolly.indexLocalOnly = indexLocalOnly;
            dolly.properties = properties;
            return dolly;
         } catch (CloneNotSupportedException shouldNotHappen) {
            throw new RuntimeException("Should not happen!", shouldNotHappen);
         }
      }

      @Override
      public String toString(){
         return "Indexing[enabled="+enabled+",localOnly="+indexLocalOnly+"]";
      }
   }


   /**
    * Cache replication mode.
    */
   @Deprecated
   public static enum CacheMode {
      /**
       * Data is not replicated.
       */
      LOCAL,

      /**
       * Data replicated synchronously.
       */
      REPL_SYNC,

      /**
       * Data replicated asynchronously.
       */
      REPL_ASYNC,

      /**
       * Data invalidated synchronously.
       */
      INVALIDATION_SYNC,

      /**
       * Data invalidated asynchronously.
       */
      INVALIDATION_ASYNC,

      /**
       * Synchronous DIST
       */
      DIST_SYNC,

      /**
       * Async DIST
       */
      DIST_ASYNC;

      /**
       * Returns true if the mode is invalidation, either sync or async.
       */
      public boolean isInvalidation() {
         return this == INVALIDATION_SYNC || this == INVALIDATION_ASYNC;
      }

      public boolean isSynchronous() {
         return this == REPL_SYNC || this == DIST_SYNC || this == INVALIDATION_SYNC || this == LOCAL;
      }

      public boolean isClustered() {
         return this != LOCAL;
      }

      public boolean isDistributed() {
         return this == DIST_SYNC || this == DIST_ASYNC;
      }

      public boolean isReplicated() {
         return this == REPL_SYNC || this == REPL_ASYNC;
      }

      public CacheMode toSync() {
         switch (this) {
            case REPL_ASYNC:
               return REPL_SYNC;
            case INVALIDATION_ASYNC:
               return INVALIDATION_SYNC;
            case DIST_ASYNC:
               return DIST_SYNC;
            default:
               return this;
         }
      }

      public CacheMode toAsync() {
         switch (this) {
            case REPL_SYNC:
               return REPL_ASYNC;
            case INVALIDATION_SYNC:
               return INVALIDATION_ASYNC;
            case DIST_SYNC:
               return DIST_ASYNC;
            default:
               return this;
         }
      }
   }
}
