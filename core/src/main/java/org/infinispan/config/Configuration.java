/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.config;

import org.infinispan.distribution.DefaultConsistentHash;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.annotations.Start;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.CacheException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates the configuration of a Cache.
 * 
 * <p>
 * Note that class Configuration contains JAXB annotations. These annotations determine how XML
 * configuration files are read into instances of configuration class hierarchy as well as they
 * provide meta data for configuration file XML schema generation. Please modify these annotations
 * and Java element types they annotate with utmost understanding and care.
 * 
 * @configRef name="default",desc="Configures the default cache which can be retrieved via CacheManager.getCache().
 *                                 These default settings are also used as a starting point when configuring namedCaches,
 *                                 since the default settings are inherited by any named cache."
 * @configRef name="namedCache",desc="&nbsp;&nbsp;If you use &lt;namedCache ... &gt; instead of &lt;default ... &gt; this defines a
 *                                    a named cache whose name can be passed to CacheManager.getCache(String)
 *                                    in order to retrieve the named instance.  Named caches inherit settings from the
 *                                    default, and additional behavior can be specified or overridden.  Also the 'name' attribute is mandatory."
 * 
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 4.0
 * 
 * @see <a href="../../../config.html#ce_infinispan_default">Configuration reference</a>
 */
@SurvivesRestarts
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder={})
public class Configuration extends AbstractNamedCacheConfigurationBean {  

   private static final long serialVersionUID = 5553791890144997466L;

   // reference to a global configuration
   @XmlTransient
   private GlobalConfiguration globalConfiguration;

   /** 
    * @configRef desc="Only used with the namedCache element, this attribute specifies the name of the cache.  Can be any String, but must be unique in a given configuration."
    * */
   @XmlAttribute
   protected String name;


   // ------------------------------------------------------------------------------------------------------------
   //   CONFIGURATION OPTIONS
   // ------------------------------------------------------------------------------------------------------------

   @XmlElement
   private LockingType locking = new LockingType();

   @XmlElement
   private CacheLoaderManagerConfig loaders = new CacheLoaderManagerConfig();

   @XmlElement
   private TransactionType transaction = new TransactionType(null);

   @XmlElement
   private CustomInterceptorsType customInterceptors = new CustomInterceptorsType();

   @XmlElement
   private EvictionType eviction = new EvictionType();

   @XmlElement
   private ExpirationType expiration = new ExpirationType();

   @XmlElement
   private UnsafeType unsafe = new UnsafeType();

   @XmlElement
   private ClusteringType clustering = new ClusteringType();

   @XmlElement
   private JmxStatistics jmxStatistics = new JmxStatistics();

   @XmlElement
   private LazyDeserialization lazyDeserialization = new LazyDeserialization();

   @XmlElement
   private InvocationBatching invocationBatching = new InvocationBatching();

   @XmlElement
   private DeadlockDetectionType deadlockDetection = new DeadlockDetectionType();
   
   @XmlElement
   private QueryConfigurationBean indexing = new QueryConfigurationBean();

   @SuppressWarnings("unused")
   @Start(priority = 1)
   private void correctIsolationLevels() {
      // ensure the correct isolation level upgrades and/or downgrades are performed.
      switch (locking.isolationLevel) {
         case NONE:
         case READ_UNCOMMITTED:
            locking.isolationLevel = IsolationLevel.READ_COMMITTED;
            break;
         case SERIALIZABLE:
            locking.isolationLevel = IsolationLevel.REPEATABLE_READ;
            break;
      }
   }

   public void applyOverrides(Configuration overrides) {
      OverrideConfigurationVisitor v1 =new OverrideConfigurationVisitor();
      this.accept(v1);
      OverrideConfigurationVisitor v2 =new OverrideConfigurationVisitor();
      overrides.accept(v2);
      v1.override(v2);      
   }

   public void inject(ComponentRegistry cr) {
      this.accept(new InjectComponentRegistryVisitor(cr));
   }

   // ------------------------------------------------------------------------------------------------------------
   //   SETTERS - MAKE SURE ALL SETTERS PERFORM testImmutability()!!!
   // ------------------------------------------------------------------------------------------------------------

   public GlobalConfiguration getGlobalConfiguration() {
      return globalConfiguration;
   }

   public String getName() {
      return name;
   }

   @SuppressWarnings("unused")
   @Inject
   private void injectGlobalConfiguration(GlobalConfiguration globalConfiguration) {
      this.globalConfiguration = globalConfiguration;
   }

   public boolean isStateTransferEnabled() {
      return clustering.stateRetrieval.fetchInMemoryState || (loaders != null && loaders.isFetchPersistentState());
   }

   public long getDeadlockDetectionSpinDuration() {
      return deadlockDetection.spinDuration;
   }

   
   public void setDeadlockDetectionSpinDuration(long eagerDeadlockSpinDuration) {
      this.deadlockDetection.setSpinDuration(eagerDeadlockSpinDuration);
   }
   
   public boolean isEnableDeadlockDetection() {
      return deadlockDetection.enabled;
   }

   public void setEnableDeadlockDetection(boolean useEagerDeadlockDetection) {
      this.deadlockDetection.setEnabled(useEagerDeadlockDetection);
   }

   public void setUseLockStriping(boolean useLockStriping) {
      locking.setUseLockStriping(useLockStriping);
   }

   public boolean isUseLockStriping() {
      return locking.useLockStriping;
   }

   public boolean isUnsafeUnreliableReturnValues() {
      return unsafe.unreliableReturnValues;
   }
   
   public void setUnsafeUnreliableReturnValues(boolean unsafeUnreliableReturnValues) {
      this.unsafe.setUnreliableReturnValues(unsafeUnreliableReturnValues);
   }
 
   public void setRehashRpcTimeout(long rehashRpcTimeout) {
      this.clustering.hash.setRehashRpcTimeout(rehashRpcTimeout);
   }

   public long getRehashRpcTimeout() {
      return clustering.hash.rehashRpcTimeout;
   }
   public boolean isWriteSkewCheck() {
      return locking.writeSkewCheck;
   }

   public void setWriteSkewCheck(boolean writeSkewCheck) {
      locking.setWriteSkewCheck(writeSkewCheck);
   }

   public int getConcurrencyLevel() {
      return locking.concurrencyLevel;
   }

   public void setConcurrencyLevel(int concurrencyLevel) {
      locking.setConcurrencyLevel(concurrencyLevel);
   }

   public void setReplQueueMaxElements(int replQueueMaxElements) {
      this.clustering.async.setReplQueueMaxElements(replQueueMaxElements);
   }

   public void setReplQueueInterval(long replQueueInterval) {
      this.clustering.async.setReplQueueInterval(replQueueInterval);
   }

   public void setReplQueueInterval(long replQueueInterval, TimeUnit timeUnit) {
      setReplQueueInterval(timeUnit.toMillis(replQueueInterval));
   }

  
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
    */
   public void setInvocationBatchingEnabled(boolean enabled) {
      invocationBatching.setEnabled(enabled);
   }

   public void setFetchInMemoryState(boolean fetchInMemoryState) {
      this.clustering.stateRetrieval.setFetchInMemoryState(fetchInMemoryState);
   }

   public void setLockAcquisitionTimeout(long lockAcquisitionTimeout) {
      locking.setLockAcquisitionTimeout(lockAcquisitionTimeout);
   }

   public void setLockAcquisitionTimeout(long lockAcquisitionTimeout, TimeUnit timeUnit) {
      setLockAcquisitionTimeout(timeUnit.toMillis(lockAcquisitionTimeout));
   }

   public void setSyncReplTimeout(long syncReplTimeout) {
      this.clustering.sync.setReplTimeout(syncReplTimeout);
   }

   public void setSyncReplTimeout(long syncReplTimeout, TimeUnit timeUnit) {
      setSyncReplTimeout(timeUnit.toMillis(syncReplTimeout));
   }

   public void setCacheMode(CacheMode cacheModeInt) {
      clustering.setMode(cacheModeInt);
   }

   public void setCacheMode(String cacheMode) {
      if (cacheMode == null) throw new ConfigurationException("Cache mode cannot be null", "CacheMode");
      clustering.setMode(CacheMode.valueOf(uc(cacheMode)));
      if (clustering.mode == null) {
         log.warn("Unknown cache mode '" + cacheMode + "', using defaults.");
         clustering.setMode(CacheMode.LOCAL);
      }
   }

   public String getCacheModeString() {
      return clustering.mode == null ? null : clustering.mode.toString();
   }

   public void setCacheModeString(String cacheMode) {
      setCacheMode(cacheMode);
   }

   public long getEvictionWakeUpInterval() {
      return eviction.wakeUpInterval;
   }

   public void setEvictionWakeUpInterval(long evictionWakeUpInterval) {
      this.eviction.setWakeUpInterval(evictionWakeUpInterval);
   }

   public EvictionStrategy getEvictionStrategy() {
      return eviction.strategy;
   }

   public void setEvictionStrategy(EvictionStrategy evictionStrategy) {
      this.eviction.setStrategy(evictionStrategy);
   }

   public void setEvictionStrategy(String eStrategy){
      this.eviction.strategy = EvictionStrategy.valueOf(uc(eStrategy));
      if (this.eviction.strategy == null) {
         log.warn("Unknown evictionStrategy  '" + eStrategy + "', using defaults.");
         this.eviction.setStrategy(EvictionStrategy.NONE);
      }
   }

   public int getEvictionMaxEntries() {
      return eviction.maxEntries;
   }

   public void setEvictionMaxEntries(int evictionMaxEntries) {
      this.eviction.setMaxEntries(evictionMaxEntries);
   }

   public long getExpirationLifespan() {
      return expiration.lifespan;
   }

   public void setExpirationLifespan(long expirationLifespan) {
      this.expiration.setLifespan(expirationLifespan);
   }

   public long getExpirationMaxIdle() {
      return expiration.maxIdle;
   }

   public void setExpirationMaxIdle(long expirationMaxIdle) {
      this.expiration.setMaxIdle(expirationMaxIdle);
   }

   public void setTransactionManagerLookupClass(String transactionManagerLookupClass) {
      this.transaction.setTransactionManagerLookupClass(transactionManagerLookupClass);
   }

   public void setTransactionManagerLookup(TransactionManagerLookup transactionManagerLookup) {
      this.transaction.transactionManagerLookup = transactionManagerLookup;
   }

   public void setCacheLoaderManagerConfig(CacheLoaderManagerConfig cacheLoaderManagerConfig) {
      this.loaders = cacheLoaderManagerConfig;
   }

   public void setSyncCommitPhase(boolean syncCommitPhase) {
      this.transaction.setSyncCommitPhase(syncCommitPhase);
   }

   public void setSyncRollbackPhase(boolean syncRollbackPhase) {
      this.transaction.setSyncRollbackPhase(syncRollbackPhase);
   }

   public void setUseEagerLocking(boolean useEagerLocking) {
      this.transaction.setUseEagerLocking(useEagerLocking);
   }

   public void setUseReplQueue(boolean useReplQueue) {
      this.clustering.async.setUseReplQueue(useReplQueue);
   }

   public void setIsolationLevel(IsolationLevel isolationLevel) {
      locking.setIsolationLevel(isolationLevel);
   }

   public void setStateRetrievalTimeout(long stateRetrievalTimeout) {
      this.clustering.stateRetrieval.setTimeout(stateRetrievalTimeout);
   }

   public void setStateRetrievalTimeout(long stateRetrievalTimeout, TimeUnit timeUnit) {
      setStateRetrievalTimeout(timeUnit.toMillis(stateRetrievalTimeout));
   }

   public void setStateRetrievalInitialRetryWaitTime(long initialRetryWaitTime) {
      clustering.stateRetrieval.setInitialRetryWaitTime(initialRetryWaitTime);
   }

   public void setStateRetrievalInitialRetryWaitTime(long initialRetryWaitTime, TimeUnit timeUnit) {
      setStateRetrievalInitialRetryWaitTime(timeUnit.toMillis(initialRetryWaitTime));
   }

   public void setStateRetrievalRetryWaitTimeIncreaseFactor(int retryWaitTimeIncreaseFactor) {
      clustering.stateRetrieval.setRetryWaitTimeIncreaseFactor(retryWaitTimeIncreaseFactor);
   }

   public void setStateRetrievalNumRetries(int numRetries) {
      clustering.stateRetrieval.setNumRetries(numRetries);
   }

   public void setIsolationLevel(String isolationLevel) {
      if (isolationLevel == null) throw new ConfigurationException("Isolation level cannot be null", "IsolationLevel");
      locking.setIsolationLevel(IsolationLevel.valueOf(uc(isolationLevel)));
      if (locking.isolationLevel == null) {
         log.warn("Unknown isolation level '" + isolationLevel + "', using defaults.");
         locking.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      }
   }
   
   public void setUseLazyDeserialization(boolean useLazyDeserialization) {
      lazyDeserialization.setEnabled(useLazyDeserialization);
   }

   public void setL1CacheEnabled(boolean l1CacheEnabled) {
      this.clustering.l1.setEnabled(l1CacheEnabled);
   }

   public void setL1Lifespan(long l1Lifespan) {
      this.clustering.l1.setLifespan(l1Lifespan);
   }

   public void setL1OnRehash(boolean l1OnRehash) {
      this.clustering.l1.setOnRehash(l1OnRehash);
   }

   public void setConsistentHashClass(String consistentHashClass) {
      this.clustering.hash.setConsistentHashClass(consistentHashClass);
   }

   public void setNumOwners(int numOwners) {
      this.clustering.hash.setNumOwners(numOwners);
   }

   public void setRehashEnabled(boolean rehashEnabled) {
      this.clustering.hash.setRehashEnabled(rehashEnabled);
   }

   public void setRehashWaitTime(long rehashWaitTime) {
      this.clustering.hash.setRehashWait(rehashWaitTime);
   }

   public void setUseAsyncMarshalling(boolean useAsyncMarshalling) {
      this.clustering.async.setAsyncMarshalling(useAsyncMarshalling);
   }

   public void setIndexingEnabled(boolean enabled) {
      this.indexing.setEnabled(enabled);
   }

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

   public boolean isExposeJmxStatistics() {
      return jmxStatistics.enabled;
   }

   /**
    * @return true if invocation batching is enabled.
    * @since 4.0
    */
   public boolean isInvocationBatchingEnabled() {
      return invocationBatching.enabled ;
   }
   
   public boolean isIndexingEnabled() {
      return indexing.isEnabled();
   }

   public boolean isIndexLocalOnly() {
      return indexing.isIndexLocalOnly();
   }

   public boolean isFetchInMemoryState() {
      return clustering.stateRetrieval.fetchInMemoryState;
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

   public IsolationLevel getIsolationLevel() {
      return locking.isolationLevel;
   }

   public String getTransactionManagerLookupClass() {
      return transaction.transactionManagerLookupClass;
   }

   public TransactionManagerLookup getTransactionManagerLookup() {
      return transaction.transactionManagerLookup;
   }

   public CacheLoaderManagerConfig getCacheLoaderManagerConfig() {
      return loaders;
   }

   public boolean isSyncCommitPhase() {
      return transaction.syncCommitPhase;
   }

   public boolean isSyncRollbackPhase() {
      return transaction.syncRollbackPhase;
   }

   public boolean isUseEagerLocking() {
      return transaction.useEagerLocking;
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

   public boolean isUseLazyDeserialization() {
      return lazyDeserialization.enabled;
   }

   public boolean isL1CacheEnabled() {
      return clustering.l1.enabled;
   }

   public long getL1Lifespan() {
      return clustering.l1.lifespan;
   }

   public boolean isL1OnRehash() {
      return clustering.l1.onRehash;
   }

   public String getConsistentHashClass() {
      return clustering.hash.consistentHashClass;
   }

   public int getNumOwners() {
      return clustering.hash.numOwners;
   }

   public boolean isRehashEnabled() {
      return clustering.hash.rehashEnabled;
   }

   public long getRehashWaitTime() {
      return clustering.hash.rehashWait;
   }

   // ------------------------------------------------------------------------------------------------------------
   //   HELPERS
   // ------------------------------------------------------------------------------------------------------------

   // ------------------------------------------------------------------------------------------------------------
   //   OVERRIDDEN METHODS
   // ------------------------------------------------------------------------------------------------------------

   public void accept(ConfigurationBeanVisitor v) {      
      clustering.accept(v);
      customInterceptors.accept(v);
      deadlockDetection.accept(v);
      eviction.accept(v);
      expiration.accept(v);
      invocationBatching.accept(v);
      jmxStatistics.accept(v);
      lazyDeserialization.accept(v);
      loaders.accept(v);
      locking.accept(v);
      transaction.accept(v);
      unsafe.accept(v);
      indexing.accept(v);
      v.visitConfiguration(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Configuration)) return false;

      Configuration that = (Configuration) o;

      if (clustering != null ? !clustering.equals(that.clustering) : that.clustering != null) return false;
      if (customInterceptors != null ? !customInterceptors.equals(that.customInterceptors) : that.customInterceptors != null)
         return false;
      if (deadlockDetection != null ? !deadlockDetection.equals(that.deadlockDetection) : that.deadlockDetection != null)
         return false;
      if (eviction != null ? !eviction.equals(that.eviction) : that.eviction != null) return false;
      if (expiration != null ? !expiration.equals(that.expiration) : that.expiration != null) return false;
      if (globalConfiguration != null ? !globalConfiguration.equals(that.globalConfiguration) : that.globalConfiguration != null)
         return false;
      if (invocationBatching != null ? !invocationBatching.equals(that.invocationBatching) : that.invocationBatching != null)
         return false;
      if (jmxStatistics != null ? !jmxStatistics.equals(that.jmxStatistics) : that.jmxStatistics != null) return false;
      if (lazyDeserialization != null ? !lazyDeserialization.equals(that.lazyDeserialization) : that.lazyDeserialization != null)
         return false;
      if (loaders != null ? !loaders.equals(that.loaders) : that.loaders != null) return false;
      if (locking != null ? !locking.equals(that.locking) : that.locking != null) return false;
      if (name != null ? !name.equals(that.name) : that.name != null) return false;
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
      result = 31 * result + (eviction != null ? eviction.hashCode() : 0);
      result = 31 * result + (expiration != null ? expiration.hashCode() : 0);
      result = 31 * result + (unsafe != null ? unsafe.hashCode() : 0);
      result = 31 * result + (clustering != null ? clustering.hashCode() : 0);
      result = 31 * result + (jmxStatistics != null ? jmxStatistics.hashCode() : 0);
      result = 31 * result + (lazyDeserialization != null ? lazyDeserialization.hashCode() : 0);
      result = 31 * result + (invocationBatching != null ? invocationBatching.hashCode() : 0);
      result = 31 * result + (deadlockDetection != null ? deadlockDetection.hashCode() : 0);
      return result;
   }

   @Override
   public Configuration clone() {
      try {
         Configuration dolly = (Configuration) super.clone();
         if (clustering != null) dolly.clustering = clustering.clone();
         if (globalConfiguration!= null) dolly.globalConfiguration = globalConfiguration.clone();
         if (locking != null) dolly.locking = (LockingType) locking.clone();
         if (loaders != null) dolly.loaders = loaders.clone();
         if (transaction != null) dolly.transaction = (TransactionType) transaction.clone();
         if (customInterceptors != null) dolly.customInterceptors = customInterceptors.clone();
         if (eviction != null) dolly.eviction = (EvictionType) eviction.clone();
         if (expiration != null) dolly.expiration = (ExpirationType) expiration.clone();
         if (unsafe != null) dolly.unsafe = (UnsafeType) unsafe.clone();
         if (clustering != null) dolly.clustering = clustering.clone();
         if (jmxStatistics != null) dolly.jmxStatistics = (JmxStatistics) jmxStatistics.clone();
         if (lazyDeserialization != null) dolly.lazyDeserialization = (LazyDeserialization) lazyDeserialization.clone();
         if (invocationBatching != null) dolly.invocationBatching = (InvocationBatching) invocationBatching.clone();
         if (deadlockDetection != null) dolly.deadlockDetection = (DeadlockDetectionType) deadlockDetection.clone();
         if (indexing != null) dolly.indexing = indexing.clone();
         return dolly;
      } catch (CloneNotSupportedException e) {
         throw new CacheException("Unexpected!",e);
      }
   }

   public boolean isUsingCacheLoaders() {
      return getCacheLoaderManagerConfig() != null && !getCacheLoaderManagerConfig().getCacheLoaderConfigs().isEmpty();
   }

   /**
    * Returns the {@link org.infinispan.config.CustomInterceptorConfig}, if any, associated with this configuration
    * object. The custom interceptors will be added to the cache at startup in the sequence defined by this list.
    *
    * @return List of cutom interceptors, never null
    */
   @SuppressWarnings("unchecked")
   public List<CustomInterceptorConfig> getCustomInterceptors() {
      return customInterceptors.customInterceptors == null ? Collections.EMPTY_LIST : customInterceptors.customInterceptors;
   }

   /**
    * @see #getCustomInterceptors()
    */
   public void setCustomInterceptors(List<CustomInterceptorConfig> customInterceptors) {
      testImmutability("customInterceptors");
      this.customInterceptors.customInterceptors = customInterceptors;
   }  

   public void assertValid() throws ConfigurationException {
      // certain combinations are illegal, such as state transfer + DIST
      if (clustering.mode.isDistributed() && clustering.stateRetrieval.fetchInMemoryState)
         throw new ConfigurationException("Cache cannot use DISTRIBUTION mode and have fetchInMemoryState set to true.  Perhaps you meant to enable rehashing?");
   }

   public boolean isOnePhaseCommit() {
      return !getCacheMode().isSynchronous();
   }
   
   /**
    * 
    * @configRef name="transaction",desc="Defines transactional (JTA) characteristics of the cache."
    * 
    * @see <a href="../../../config.html#ce_default_transaction">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class TransactionType extends AbstractNamedCacheConfigurationBean{
           
      /** The serialVersionUID */
      private static final long serialVersionUID = -3867090839830874603L;

      /** 
       * @configRef desc="Fully qualified class name of a class that looks up a reference to a {@link javax.transaction.TransactionManager}.
       * The default provided is capable of locating the default TransactionManager in most popular Java EE systems,
       * using a JNDI lookup."
       * */
      protected String transactionManagerLookupClass;
      
      @XmlTransient
      protected TransactionManagerLookup transactionManagerLookup;
      
      /** 
       * @configRef desc="If true, the cluster-wide commit phase in two-phase commit (2PC) transactions will be synchronous,
       *            so Infinispan will wait for responses from all nodes to which the commit was sent. Otherwise, the
       *            commit phase will be asynchronous. Keeping it as false improves performance of 2PC transactions, since
       *            any remote failures are trapped during the prepare phase anyway and appropriate rollbacks are issued."
       * */
      @Dynamic
      protected Boolean syncCommitPhase = false;
      
      /** 
       * @configRef desc="If true, the cluster-wide rollback phase in two-phase commit (2PC) transactions will be synchronous,
       *            so Infinispan will wait for responses from all nodes to which the rollback was sent. Otherwise, the
       *            rollback phase will be asynchronous. Keeping it as false improves performance of 2PC transactions."
       */

      @Dynamic
      protected Boolean syncRollbackPhase = false;
      
      /** 
       * @configRef desc="When eager locking is set to true, whenever a lock on key is required, cluster-wide locks will
       *            be acquired at the same time as local, in-VM locks. If false, cluster-wide locks are only acquired
       *            during the prepare phase in the two-phase commit protocol. Note that this setting
       *            is implicit and so it's indiscriminate. Alternatively, you can keep this setting as false and instead
       *            do eager locking explicitly on a per invocation basis by calling AdvancedCache.lock(Object)".
       * */
      @Dynamic
      protected Boolean useEagerLocking = false;
      
      public TransactionType(String transactionManagerLookupClass) {
         this.transactionManagerLookupClass = transactionManagerLookupClass;
      }
      
      public void accept(ConfigurationBeanVisitor v) {
         v.visitTransactionType(this);
      }

      public TransactionType() {
         this.transactionManagerLookupClass = GenericTransactionManagerLookup.class.getName();
      }

      @XmlAttribute
      public void setTransactionManagerLookupClass(String transactionManagerLookupClass) {
         testImmutability("transactionManagerLookupClass");
         this.transactionManagerLookupClass = transactionManagerLookupClass;         
      }

      @XmlAttribute
      public void setSyncCommitPhase(Boolean syncCommitPhase) {
         testImmutability("syncCommitPhase");
         this.syncCommitPhase = syncCommitPhase;
      }

      @XmlAttribute
      public void setSyncRollbackPhase(Boolean syncRollbackPhase) {
         testImmutability("syncRollbackPhase");
         this.syncRollbackPhase = syncRollbackPhase;
      }

      @XmlAttribute
      public void setUseEagerLocking(Boolean useEagerLocking) {
         testImmutability("useEagerLocking");
         this.useEagerLocking = useEagerLocking;
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

         return true;
      }

      @Override
      public int hashCode() {
         int result = transactionManagerLookupClass != null ? transactionManagerLookupClass.hashCode() : 0;
         result = 31 * result + (transactionManagerLookup != null ? transactionManagerLookup.hashCode() : 0);
         result = 31 * result + (syncCommitPhase != null ? syncCommitPhase.hashCode() : 0);
         result = 31 * result + (syncRollbackPhase != null ? syncRollbackPhase.hashCode() : 0);
         result = 31 * result + (useEagerLocking != null ? useEagerLocking.hashCode() : 0);
         return result;
      }
   }
   /**
    * 
    * @configRef name="locking",desc="Defines the local, in-VM locking and concurrency characteristics of the cache."
    * 
    * @see <a href="../../../config.html#ce_default_locking">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class LockingType  extends AbstractNamedCacheConfigurationBean{

      /** The serialVersionUID */
      private static final long serialVersionUID = 8142143187082119506L;

      /** @configRef desc="Maximum time to attempt a particular lock acquisition" */
      @Dynamic
      protected Long lockAcquisitionTimeout = 10000L;

      /** @configRef desc="Cache isolation level. Infinispan only supports READ_COMMITTED
       *             or REPEATABLE_READ isolation levels.  See <a href='http://en.wikipedia.org/wiki/Isolation_level'>http://en.wikipedia.org/wiki/Isolation_level</a>
       *             for a discussion on isolation levels." */
      protected IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;

      /** @configRef desc="This setting is only applicable in the case of REPEATABLE_READ. 
       *             When write skew check is set to false, if the writer at commit time discovers that the working
       *             entry and the underlying entry have different versions, the working entry will overwrite the
       *             underlying entry. If true, such version conflict - known as a write-skew - will throw an Exception. */
      protected Boolean writeSkewCheck = false;

      /** @configRef desc="If true, a pool of shared locks is maintained for all entries that need to be locked.
       *             Otherwise, a lock is created per entry in the cache.  Lock striping helps control memory footprint
       *             but may reduce concurrency in the system."
       */
      protected Boolean useLockStriping = true;
      
      /** @configRef desc="Concurrency level for lock containers. Adjust this value according to the number of concurrent 
       *             threads interating with Infinispan.  Similar to the concurrencyLevel tuning parameter seen in
       *             the JDK's ConcurrentHashMap."*/
      protected Integer concurrencyLevel = 512;

      @XmlAttribute
      public void setLockAcquisitionTimeout(Long lockAcquisitionTimeout) {
         testImmutability("lockAcquisitionTimeout");
         this.lockAcquisitionTimeout = lockAcquisitionTimeout;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitLockingType(this);
      }

      @XmlAttribute
      public void setIsolationLevel(IsolationLevel isolationLevel) {
         testImmutability("isolationLevel");
         this.isolationLevel = isolationLevel;
      }

      @XmlAttribute
      public void setWriteSkewCheck(Boolean writeSkewCheck) {
         testImmutability("writeSkewCheck");
         this.writeSkewCheck = writeSkewCheck;
      }

     
      @XmlAttribute
      public void setUseLockStriping(Boolean useLockStriping) {
         testImmutability("useLockStriping");
         this.useLockStriping = useLockStriping;
      }

      @XmlAttribute
      public void setConcurrencyLevel(Integer concurrencyLevel) {
         testImmutability("concurrencyLevel");
         this.concurrencyLevel = concurrencyLevel;
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
    * @configRef name="clustering",desc="Defines clustered characteristics of the cache."
    * 
    * @see <a href="../../../config.html#ce_default_clustering">Configuration reference</a>
    */
   @XmlJavaTypeAdapter(ClusteringTypeAdapter.class)
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @XmlType(propOrder={})
   public static class ClusteringType extends AbstractNamedCacheConfigurationBean {

      /** The serialVersionUID */
      private static final long serialVersionUID = 4048135465543498430L;

      /** 
       * @configRef name="mode",desc="Cache mode. For distribution, set mode to either 'd', 'dist' or 'distribution'.
       *            For replication, use either 'r', 'repl' or 'replication'. Finally, for invalidation, 
       *            'i', 'inv' or 'invalidation'.",defaultValue="D"
       */
      @XmlAttribute(name="mode")
      protected String stringMode;

      @XmlTransient
      protected CacheMode mode = CacheMode.LOCAL;

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

      public void setMode(CacheMode mode) {
         testImmutability("mode");
         this.mode = mode;
      }

      public boolean isSynchronous() {
         return !async.readFromXml;
      }

      @Override
      public ClusteringType clone() throws CloneNotSupportedException {
         ClusteringType dolly = (ClusteringType) super.clone();
         dolly.sync = (SyncType) sync.clone();
         dolly.stateRetrieval = (StateRetrievalType) stateRetrieval.clone();
         dolly.l1 = (L1Type) l1.clone();
         dolly.async = (AsyncType) async.clone();
         dolly.hash = (HashType) hash.clone();
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
   }

   public static class ClusteringTypeAdapter extends XmlAdapter<ClusteringType, ClusteringType> {

      @Override
      public ClusteringType marshal(ClusteringType ct) throws Exception {
         return ct;        
      }

      @Override
      public ClusteringType unmarshal(ClusteringType ct) throws Exception {
         if(ct.stringMode != null){
            String mode = ct.stringMode.toLowerCase();
            if(mode.startsWith("r")){
               if(ct.isSynchronous())
                  ct.setMode(CacheMode.REPL_SYNC);
               else 
                  ct.setMode(CacheMode.REPL_ASYNC);
            } else if (mode.startsWith("i")){
               if(ct.isSynchronous())
                  ct.setMode(CacheMode.INVALIDATION_SYNC);
               else 
                  ct.setMode(CacheMode.INVALIDATION_ASYNC);
            } else if (mode.startsWith("d")){
               if(ct.isSynchronous())
                  ct.setMode(CacheMode.DIST_SYNC);
               else 
                  ct.setMode(CacheMode.DIST_ASYNC);
            }
            else {
               throw new ConfigurationException("Invalid clustering mode" + ct.stringMode);
            }
         }
         return ct;
      }
   }

   /**
    * @configRef name="async",parentName="clustering",desc="If this element is present, all communications are 
    *            asynchronous, in that whenever a thread sends a message sent over the wire, it does not wait 
    *            for an acknowledgement before returning. This element is mutually exclusive with the <sync /> 
    *            element.<br /><br />Characteristics of this can be tuned here."
    *            
    * @see <a href="../../../config.html#ce_clustering_async">Configuration reference</a>
    *            
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class AsyncType extends AbstractNamedCacheConfigurationBean {

      @XmlTransient
      private boolean readFromXml = false;
      
      /** The serialVersionUID */
      private static final long serialVersionUID = -7726319188826197399L;

      /** @configRef desc="If true, this forces all async communications to be queued up and sent out periodically as 
       *             a batch." */
      protected Boolean useReplQueue=false;

      /** @configRef desc="If useReplQueue is set to true, this attribute can be used to trigger flushing of the queue 
       *             when it reaches a specific threshold." */
      protected Integer replQueueMaxElements=1000;

      /** @configRef desc="If useReplQueue is set to true, this attribute controls how often the asynchronous thread 
       *             used to flush the replication queue runs. This should be a positive integer which represents thread 
       *             wakeup time in milliseconds." */
      protected Long replQueueInterval=5000L;

      /** @configRef desc="If true, asynchronous marshalling is enabled which means that caller can return even quicker." */
      protected Boolean asyncMarshalling=true;

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

         return true;
      }

      @Override
      public int hashCode() {
         int result = (readFromXml ? 1 : 0);
         result = 31 * result + (useReplQueue != null ? useReplQueue.hashCode() : 0);
         result = 31 * result + (replQueueMaxElements != null ? replQueueMaxElements.hashCode() : 0);
         result = 31 * result + (replQueueInterval != null ? replQueueInterval.hashCode() : 0);
         result = 31 * result + (asyncMarshalling != null ? asyncMarshalling.hashCode() : 0);
         return result;
      }

      private AsyncType(){
         this(true);
      }

      @XmlAttribute
      public void setUseReplQueue(Boolean useReplQueue) {
         testImmutability("useReplQueue");
         this.useReplQueue = useReplQueue;
      }

      @XmlAttribute
      public void setReplQueueMaxElements(Integer replQueueMaxElements) {
         testImmutability("replQueueMaxElements");
         this.replQueueMaxElements = replQueueMaxElements;
      }

      @XmlAttribute
      public void setReplQueueInterval(Long replQueueInterval) {
         testImmutability("replQueueInterval");
         this.replQueueInterval = replQueueInterval;
      }

      @XmlAttribute
      public void setAsyncMarshalling(Boolean asyncMarshalling) {
         testImmutability("asyncMarshalling");
         this.asyncMarshalling = asyncMarshalling;
      }
   }
   
   /**
    * @configRef name="expiration",desc="This element controls the default expiration settings for entries in the cache."
    * 
    * @see <a href="../../../config.html#ce_default_expiration">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class ExpirationType extends AbstractNamedCacheConfigurationBean{

      /** The serialVersionUID */
      private static final long serialVersionUID = 5757161438110848530L;

      /** @configRef desc="Maximum lifespan of a cache entry, after which the entry is expired cluster-wide.  -1 means the entries never expire.
       *                   <br /><br />Note that this can be overriden on a per-entry bassi by using the Cache API."
       */
      protected Long lifespan=-1L;

      /** @configRef desc="Maximum idle time a cache entry will be maintained in the cache. If the idle time 
       *             is exceeded, the entry will be expired cluster-wide.  -1 means the entries never expire.
       *             <br /><br />Note that this can be overriden on a per-entry bassi by using the Cache API."
       */
      protected Long maxIdle=-1L;

      @XmlAttribute
      public void setLifespan(Long lifespan) {
         testImmutability("lifespan");
         this.lifespan = lifespan;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitExpirationType(this);         
      }

      @XmlAttribute
      public void setMaxIdle(Long maxIdle) {
         testImmutability("maxIdle");
         this.maxIdle = maxIdle;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof ExpirationType)) return false;

         ExpirationType that = (ExpirationType) o;

         if (lifespan != null ? !lifespan.equals(that.lifespan) : that.lifespan != null) return false;
         if (maxIdle != null ? !maxIdle.equals(that.maxIdle) : that.maxIdle != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = lifespan != null ? lifespan.hashCode() : 0;
         result = 31 * result + (maxIdle != null ? maxIdle.hashCode() : 0);
         return result;
      }
   }

   /**
    * @configRef name="eviction",desc="This element controls the eviction settings for the cache."
    * 
    * @see <a href="../../../config.html#ce_default_eviction">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class EvictionType extends AbstractNamedCacheConfigurationBean {

      /** The serialVersionUID */
      private static final long serialVersionUID = -1248563712058858791L;

      /** @configRef desc="Interval between subsequent eviction runs. If you wish to disable the periodic eviction process
       *             altogether, set wakeupInterval to -1."
       */
      protected Long wakeUpInterval=5000L;

      /** @configRef desc="Eviction strategy. Available options are 'NONE', 'FIFO' and 'LRU'."*/
      protected EvictionStrategy strategy=EvictionStrategy.NONE;

      /** @configRef desc="Maximum number of entries in a cache instance.  -1 means no limit." */
      protected Integer maxEntries=-1;

      @XmlAttribute
      public void setWakeUpInterval(Long wakeUpInterval) {
         testImmutability("wakeUpInterval");
         this.wakeUpInterval = wakeUpInterval;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitEvictionType(this);
      }

      @XmlAttribute
      public void setStrategy(EvictionStrategy strategy) {
         testImmutability("strategy");
         this.strategy = strategy;
      }

      @XmlAttribute
      public void setMaxEntries(Integer maxEntries) {
         testImmutability("maxEntries");
         this.maxEntries = maxEntries;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof EvictionType)) return false;

         EvictionType that = (EvictionType) o;

         if (maxEntries != null ? !maxEntries.equals(that.maxEntries) : that.maxEntries != null) return false;
         if (strategy != that.strategy) return false;
         if (wakeUpInterval != null ? !wakeUpInterval.equals(that.wakeUpInterval) : that.wakeUpInterval != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = wakeUpInterval != null ? wakeUpInterval.hashCode() : 0;
         result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
         result = 31 * result + (maxEntries != null ? maxEntries.hashCode() : 0);
         return result;
      }
   }

   /**
    * @configRef name="stateRetrieval",desc="Configures how state is retrieved when a new cache joins the cluster.  This
    *                                  element is only used with invalidation and replication clustered modes."
    *                                  
    * @see <a href="../../../config.html#ce_clustering_stateRetrieval">Configuration reference</a>                                  
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class StateRetrievalType extends AbstractNamedCacheConfigurationBean {

      /** The serialVersionUID */
      private static final long serialVersionUID = 3709234918426217096L;

      /** @configRef desc="If true, this will cause the cache to ask neighboring caches for state when it starts up, 
       *             so the cache starts 'warm', although it will impact startup time." */
      @Dynamic
      protected Boolean fetchInMemoryState = false;

      /** @configRef desc="This is the maximum amount of time - in milliseconds - to wait for state from neighboring 
       *             caches, before throwing an exception and aborting startup. " */
      @Dynamic      
      protected Long timeout = 10000L;

      /** @configRef desc="Initial wait time when backing off before retrying state transfer retrieval"*/
      protected Long initialRetryWaitTime = 500L;

      /** @configRef desc="Wait time increase factor over successive state retrieval backoffs"*/
      protected Integer retryWaitTimeIncreaseFactor = 2;

      /** @configRef desc="Number of state retrieval retries before giving up and aborting startup."*/
      protected Integer numRetries = 5;

      @XmlAttribute
      public void setFetchInMemoryState(Boolean fetchInMemoryState) {
         testImmutability("fetchInMemoryState");
         this.fetchInMemoryState = fetchInMemoryState;
      }

      @XmlAttribute
      public void setInitialRetryWaitTime(Long initialRetryWaitTime) {
         testImmutability("initialWaitTime");
         this.initialRetryWaitTime = initialRetryWaitTime;
      }

      @XmlAttribute
      public void setRetryWaitTimeIncreaseFactor(Integer retryWaitTimeIncreaseFactor) {
         testImmutability("retryWaitTimeIncreaseFactor");
         this.retryWaitTimeIncreaseFactor = retryWaitTimeIncreaseFactor;
      }

      @XmlAttribute
      public void setNumRetries(Integer numRetries) {
         testImmutability("numRetries");
         this.numRetries = numRetries;
      }

      @XmlAttribute
      public void setTimeout(Long timeout) {
         testImmutability("timeout");
         this.timeout = timeout;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitStateRetrievalType(this);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof StateRetrievalType)) return false;

         StateRetrievalType that = (StateRetrievalType) o;

         if (fetchInMemoryState != null ? !fetchInMemoryState.equals(that.fetchInMemoryState) : that.fetchInMemoryState != null) return false;
         if (timeout != null ? !timeout.equals(that.timeout) : that.timeout != null) return false;
         if (initialRetryWaitTime != null ? !initialRetryWaitTime.equals(that.initialRetryWaitTime) : that.initialRetryWaitTime != null) return false;
         if (retryWaitTimeIncreaseFactor != null ? !retryWaitTimeIncreaseFactor.equals(that.retryWaitTimeIncreaseFactor) : that.retryWaitTimeIncreaseFactor != null) return false;
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
    * @configRef name="sync",desc="If this element is present, all communications are synchronous, in that whenever a 
    *            thread sends a message sent over the wire, it blocks until it receives an acknowledgement from the 
    *            recipient. This element is mutually exclusive with the <async />  element.  <br /><br />Characteristics of this can be tuned here."
    *
    * @see <a href="../../../config.html#ce_clustering_sync">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class SyncType  extends AbstractNamedCacheConfigurationBean {
      /** The serialVersionUID */
      private static final long serialVersionUID = 8419216253674289524L;

      /** @configRef desc="This is the timeout used to wait for an acknowledgement when making a remote call, after 
       *             which the call is aborted and an exception is thrown. "*/
      @Dynamic
      protected Long replTimeout=15000L;

      @XmlAttribute
      public void setReplTimeout(Long replTimeout) {
         testImmutability("replTimeout");
         this.replTimeout = replTimeout;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitSyncType(this);
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
   }

   /**
    * @configRef name="hash",desc="Allows fine-tuning of rehashing characteristics. Only used with 'distributed' 
    *            cache mode, and otherwise ignored."
    *            
    * @see <a href="../../../config.html#ce_clustering_hash">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class HashType extends AbstractNamedCacheConfigurationBean {

      /** The serialVersionUID */
      private static final long serialVersionUID = 752218766840948822L;

      /** @configRef name="class",desc="Fully qualified name of class providing consistent hash algorithm" */
      protected String consistentHashClass = DefaultConsistentHash.class.getName();

      /** @configRef desc="Number of cluster-wide replicas for each cache entry." */
      protected Integer numOwners=2;

      /** @configRef desc="Future flag. Currenly unused." */
      protected Long rehashWait=60000L;

      /** @configRef desc="Rehashing timeout" */
      protected Long rehashRpcTimeout=60 * 1000 * 10L;

      /** @configRef desc="If false, no rebalancing or rehashing will take place when a new node joins the cluster or 
       *             a node leaves." **/
      protected Boolean rehashEnabled=true;

      @XmlAttribute(name="class")
      public void setConsistentHashClass(String consistentHashClass) {
         testImmutability("class");
         this.consistentHashClass = consistentHashClass;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitHashType(this);
      }

      @XmlAttribute
      public void setNumOwners(Integer numOwners) {
         testImmutability("numOwners");
         this.numOwners = numOwners;
      }

      @XmlAttribute
      public void setRehashWait(Long rehashWaitTime) {
         testImmutability("rehashWait");
         this.rehashWait = rehashWaitTime;
      }

      @XmlAttribute
      public void setRehashRpcTimeout(Long rehashRpcTimeout) {
         testImmutability("rehashRpcTimeout");
         this.rehashRpcTimeout = rehashRpcTimeout;
      }

      @XmlAttribute
      public void setRehashEnabled(Boolean rehashEnabled) {
         testImmutability("rehashEnabled");
         this.rehashEnabled = rehashEnabled;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof HashType)) return false;

         HashType hashType = (HashType) o;

         if (consistentHashClass != null ? !consistentHashClass.equals(hashType.consistentHashClass) : hashType.consistentHashClass != null)
            return false;
         if (numOwners != null ? !numOwners.equals(hashType.numOwners) : hashType.numOwners != null) return false;
         if (rehashRpcTimeout != null ? !rehashRpcTimeout.equals(hashType.rehashRpcTimeout) : hashType.rehashRpcTimeout != null)
            return false;
         if (rehashWait != null ? !rehashWait.equals(hashType.rehashWait) : hashType.rehashWait != null) return false;
         if (rehashEnabled != hashType.rehashEnabled) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = consistentHashClass != null ? consistentHashClass.hashCode() : 0;
         result = 31 * result + (numOwners != null ? numOwners.hashCode() : 0);
         result = 31 * result + (rehashWait != null ? rehashWait.hashCode() : 0);
         result = 31 * result + (rehashRpcTimeout != null ? rehashRpcTimeout.hashCode() : 0);
         result = 31 * result + (rehashEnabled ? 0 : 1);
         return result;
      }
   }

   /**
    * @configRef name="l1",desc="This element configures the L1 cache behavior in 'distributed' caches instances.
    *            In any other cache modes, this element is ignored. "
    *            
    * @see <a href="../../../config.html#ce_clustering_l1">Configuration reference</a>           
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class L1Type extends AbstractNamedCacheConfigurationBean {

      /** The serialVersionUID */
      private static final long serialVersionUID = -4703587764861110638L;

      /** @configRef desc="Toggle to enable/disable L1 cache." */
      protected Boolean enabled=true;

      /** @configRef desc="Maximum lifespan of an entry placed in the L1 cache." */
      protected Long lifespan=600000L;

      /** @configRef desc="If true, entries removed due to a rehash will be moved to L1 rather than being removed 
       *             altogether." */
      protected Boolean onRehash=true;

      @XmlAttribute
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitL1Type(this);
      }

      @XmlAttribute
      public void setLifespan(Long lifespan) {
         testImmutability("lifespan");
         this.lifespan = lifespan;
      }

      @XmlAttribute
      public void setOnRehash(Boolean onRehash) {
         testImmutability("onRehash");
         this.onRehash = onRehash;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof L1Type)) return false;

         L1Type l1Type = (L1Type) o;

         if (enabled != null ? !enabled.equals(l1Type.enabled) : l1Type.enabled != null) return false;
         if (lifespan != null ? !lifespan.equals(l1Type.lifespan) : l1Type.lifespan != null) return false;
         if (onRehash != null ? !onRehash.equals(l1Type.onRehash) : l1Type.onRehash != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = enabled != null ? enabled.hashCode() : 0;
         result = 31 * result + (lifespan != null ? lifespan.hashCode() : 0);
         result = 31 * result + (onRehash != null ? onRehash.hashCode() : 0);
         return result;
      }
   }

   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class BooleanAttributeType  extends AbstractNamedCacheConfigurationBean {

      @XmlTransient
      protected final String fieldName;

      /** The serialVersionUID */
      private static final long serialVersionUID = 2296863404153834686L;

      /** @configRef desc="Toggle switch" */
      protected Boolean enabled = false;

      public BooleanAttributeType() {
         fieldName= "undefined";
      }

      public BooleanAttributeType(String fieldName) {
         this.fieldName = fieldName;
      }

      public String getFieldName() {
         return fieldName;
      }

      @XmlAttribute
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitBooleanAttributeType(this);
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
    * @configRef name="lazyDeserialization",desc="A mechanism by which serialization and deserialization of objects is 
    *            deferred till the point in time in which they are used and needed. This typically means that any 
    *            deserialization happens using the thread context class loader of the invocation that requires 
    *            deserialization, and is an effective mechanism to provide classloader isolation."
    *            
    * @see <a href="../../../config.html#ce_default_lazyDeserialization">Configuration reference</a>           
    */
   public static class LazyDeserialization extends BooleanAttributeType {
      /** The serialVersionUID */
      private static final long serialVersionUID = 7404820498857564962L;

      public LazyDeserialization() {
         super("lazyDeserialization");
      }
   }

   /**
    * @configRef name="jmxStatistics",desc=" This element specifies whether cache statistics are gathered and reported 
    *            via JMX."
    * @see <a href="../../../config.html#ce_default_jmxStatistics">Configuration reference</a>           
    */
   public static class JmxStatistics extends BooleanAttributeType {
      /** The serialVersionUID */
      private static final long serialVersionUID = 8716456707015486673L;

      public JmxStatistics() {
         super("jmxStatistics");
      }
   }

   /**
    * @configRef name="invocationBatching",desc="Defines whether invocation batching is allowed in this cache instance, and sets up internals accordingly to allow use of this API."
    * 
    * @see <a href="../../../config.html#ce_default_invocationBatching">Configuration reference</a>
    * 
    */
   public static class InvocationBatching extends BooleanAttributeType {
      /** The serialVersionUID */
      private static final long serialVersionUID = 5854115656815587815L;

      public InvocationBatching() {
         super("invocationBatching");
      }
   }

   /**
    * @configRef name="deadlockDetection",desc="This element configures deadlock detection."
    * 
    * @see <a href="../../../config.html#ce_default_deadlockDetection">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class DeadlockDetectionType  extends AbstractNamedCacheConfigurationBean{

      /** The serialVersionUID */
      private static final long serialVersionUID = -7178286048602531152L;

      /** @configRef desc="Toggle to enable/disable deadlock detection"*/
      protected Boolean enabled=false;

      /** @configRef desc="Time period that determines how often is lock acquisition attempted within maximum time 
       *             allowed to acquire a particular lock" */
      protected Long spinDuration=100L;

      @XmlAttribute
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitDeadlockDetectionType(this);
      }

      @XmlAttribute
      public void setSpinDuration(Long spinDuration) {
         testImmutability("spinDuration");
         this.spinDuration = spinDuration;
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
    * @configRef name="unsafe",desc="Allows you to tune various unsafe or non-standard characteristics. Certain operations 
    * such as Cache.put() that are supposed to return the previous value associated with the specified key according 
    * to the java.util.Map contract will not fulfill this contract if unsafe toggle is turned on. Use with care.  
    * See details at http://www.jboss.org/community/wiki/infinispantechnicalfaqs"
    * 
    * @see <a href="../../../config.html#ce_default_unsafe">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class UnsafeType  extends AbstractNamedCacheConfigurationBean{

      /** The serialVersionUID */
      private static final long serialVersionUID = -9200921443651234163L;

      /** @configRef desc="Toggle to enable/disable return value fetching" */
      protected Boolean unreliableReturnValues=false;

      @XmlAttribute
      public void setUnreliableReturnValues(Boolean unreliableReturnValues) {
         testImmutability("unreliableReturnValues");
         this.unreliableReturnValues = unreliableReturnValues;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitUnsafeType(this);
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
    * @configRef name="customInterceptors",desc="Configures custom interceptors to be added to the cache."
    * 
    * @see <a href="../../../config.html#ce_default_customInterceptors">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.FIELD)
   public static class CustomInterceptorsType extends AbstractNamedCacheConfigurationBean {

      /** The serialVersionUID */
      private static final long serialVersionUID = 7187545782011884661L;

      @XmlElement(name="interceptor")
      private List<CustomInterceptorConfig> customInterceptors= new ArrayList<CustomInterceptorConfig>();

      @Override
      public CustomInterceptorsType clone() throws CloneNotSupportedException {
         CustomInterceptorsType dolly = (CustomInterceptorsType) super.clone();
         if (customInterceptors != null) {
            dolly.customInterceptors = new ArrayList<CustomInterceptorConfig>();
            for (CustomInterceptorConfig config: customInterceptors) {
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

      public List<CustomInterceptorConfig> getCustomInterceptors(){
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
   }
   
   /**
    * @configRef name="indexing",desc="Configures indexing of entries in the cache for searching.  Note that infinispan-query.jar and its dependencies needs to be available if this option is to be used."
    * 
    * @see <a href="../../../config.html#ce_default_indexing">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class QueryConfigurationBean extends AbstractConfigurationBean {

       /** The serialVersionUID */
       private static final long serialVersionUID = 2891683014353342549L;

       /** @configRef desc="If enabled, entries will be indexed when they are added to the cache.  Indexes will be updated as entries change or are removed." */
       protected Boolean enabled = false;

       /** @configRef desc="If true, only index changes made locally, ignoring remote changes.  This is useful if indexes are shared across a cluster to prevent redundant indexing of updates." */
       protected Boolean indexLocalOnly = false;

       public Boolean isEnabled() {
           return enabled;
       }

       @XmlAttribute
       public void setEnabled(Boolean enabled) {
           testImmutability("enabled");
           this.enabled = enabled;
       }

       public Boolean isIndexLocalOnly() {
           return indexLocalOnly;
       }

       @XmlAttribute
       public void setIndexLocalOnly(Boolean indexLocalOnly) {
           testImmutability("indexLocalOnly");
           this.indexLocalOnly = indexLocalOnly;
       }
       
       public void accept(ConfigurationBeanVisitor v) {
          v.visitQueryConfigurationBean(this);
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

           if (indexLocalOnly != null ? !indexLocalOnly.equals(that.indexLocalOnly): that.indexLocalOnly != null)
               return false;

           return true;
       }

       @Override
       public int hashCode() {
           int result = enabled != null ? enabled.hashCode() : 0;
           result = 31 * result + (indexLocalOnly != null ? indexLocalOnly.hashCode() : 0);
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
            return dolly;
         } catch (CloneNotSupportedException shouldNotHappen) {
            throw new RuntimeException("Should not happen!", shouldNotHappen);
         }
      }
   }


   /**
    * Cache replication mode.
    */
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