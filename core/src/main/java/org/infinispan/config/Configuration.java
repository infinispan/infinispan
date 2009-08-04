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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.infinispan.config.parsing.ClusteringConfigReader;
import org.infinispan.config.parsing.CustomInterceptorConfigReader;
import org.infinispan.distribution.DefaultConsistentHash;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.annotations.Start;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;

/**
 * Encapsulates the configuration of a Cache.
 * 
 * <p>
 * Note that class Configuration contains JAXB annotations. These annotations determine how XML
 * configuration files are read into instances of configuration class hierarchy as well as they
 * provide meta data for configuration file XML schema generation. Please modify these annotations
 * and Java element types they annotate with utmost understanding and care.
 * 
 * 
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@NonVolatile
@ConfigurationElements(elements = {
         @ConfigurationElement(name = "default", parent = "infinispan", description = ""),
         @ConfigurationElement(name = "namedCache", parent = "infinispan", description = ""),
         @ConfigurationElement(name = "locking", parent = "default", description = ""),
         @ConfigurationElement(name = "transaction", parent = "default", description = ""), 
         @ConfigurationElement(name = "jmxStatistics", parent = "default", description = ""),
         @ConfigurationElement(name = "lazyDeserialization", parent = "default", description = ""),  
         @ConfigurationElement(name = "invocationBatching", parent = "default", description = ""),   
         @ConfigurationElement(name = "clustering", parent = "default", description = "", customReader=ClusteringConfigReader.class),
         @ConfigurationElement(name = "stateRetrieval", parent = "clustering"),
         @ConfigurationElement(name = "sync", parent = "clustering"),
         @ConfigurationElement(name = "hash", parent = "clustering"),
         @ConfigurationElement(name = "l1", parent = "clustering"),
         @ConfigurationElement(name = "async", parent = "clustering", description = ""),
         @ConfigurationElement(name = "eviction", parent = "default", description = ""),
         @ConfigurationElement(name = "expiration", parent = "default", description = ""),
         @ConfigurationElement(name = "unsafe", parent = "default", description = ""),
         @ConfigurationElement(name = "deadlockDetection", parent = "default", description = ""),
         @ConfigurationElement(name = "customInterceptors", parent = "default",
                  customReader=CustomInterceptorConfigReader.class)         
})
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder={})
public class Configuration extends AbstractNamedCacheConfigurationBean {  

   private static final long serialVersionUID = 5553791890144997466L;

   // reference to a global configuration
   @XmlTransient
   private GlobalConfiguration globalConfiguration;
   
   @XmlAttribute
   private String name;


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
   private BooleanAttributeType jmxStatistics = new BooleanAttributeType();
   
   @XmlElement
   private BooleanAttributeType lazyDeserialization = new BooleanAttributeType();
   
   @XmlElement
   private BooleanAttributeType invocationBatching = new BooleanAttributeType();
   
   @XmlElement
   private DeadlockDetectionType deadlockDetection = new DeadlockDetectionType();
   

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

   @ConfigurationAttribute(name = "spinDuration",  containingElement = "deadlockDetection")
   public void setDeadlockDetectionSpinDuration(long eagerDeadlockSpinDuration) {
      this.deadlockDetection.setSpinDuration(eagerDeadlockSpinDuration);
   }


   public boolean isEnableDeadlockDetection() {
      return deadlockDetection.enabled;
   }

   @ConfigurationAttribute(name = "enabled",  containingElement = "deadlockDetection")
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

   @ConfigurationAttribute(name = "unreliableReturnValues", 
            containingElement = "unsafe")    
   public void setUnsafeUnreliableReturnValues(boolean unsafeUnreliableReturnValues) {
      this.unsafe.setUnreliableReturnValues(unsafeUnreliableReturnValues);
   }

   @ConfigurationAttribute(name = "rehashRpcTimeout", 
            containingElement = "hash")    
   public void setRehashRpcTimeout(long rehashRpcTimeout) {
      this.clustering.hash.setRehashRpcTimeout(rehashRpcTimeout);
   }

   public long getRehashRpcTimeout() {
      return clustering.hash.rehashRpcTimeout;
   }
   public boolean isWriteSkewCheck() {
      return locking.writeSkewCheck;
   }


   @ConfigurationAttribute(name = "writeSkewCheck", 
            containingElement = "locking")    
   public void setWriteSkewCheck(boolean writeSkewCheck) {
      locking.setWriteSkewCheck(writeSkewCheck);
   }

   public int getConcurrencyLevel() {
      return locking.concurrencyLevel;
   }


   @ConfigurationAttribute(name = "concurrencyLevel", 
            containingElement = "locking")    
   public void setConcurrencyLevel(int concurrencyLevel) {
      locking.setConcurrencyLevel(concurrencyLevel);
   }

   @ConfigurationAttribute(name = "replQueueMaxElements", 
            containingElement = "async")
   public void setReplQueueMaxElements(int replQueueMaxElements) {
      this.clustering.async.setReplQueueMaxElements(replQueueMaxElements);
   }

   @ConfigurationAttribute(name = "replQueueInterval", 
            containingElement = "async")
   public void setReplQueueInterval(long replQueueInterval) {
      this.clustering.async.setReplQueueInterval(replQueueInterval);
   }

   public void setReplQueueInterval(long replQueueInterval, TimeUnit timeUnit) {
      setReplQueueInterval(timeUnit.toMillis(replQueueInterval));
   }

   @ConfigurationAttribute(name = "enabled", 
            containingElement = "jmxStatistics")   
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
   
   @ConfigurationAttribute(name = "enabled", 
            containingElement = "invocationBatching") 
   public void setInvocationBatchingEnabled(boolean enabled) {
      invocationBatching.setEnabled(enabled);
   }

   @ConfigurationAttribute(name = "fetchInMemoryState", 
            containingElement = "stateRetrieval")
   public void setFetchInMemoryState(boolean fetchInMemoryState) {
      this.clustering.stateRetrieval.setFetchInMemoryState(fetchInMemoryState);
   }

   @ConfigurationAttribute(name = "lockAcquisitionTimeout", 
            containingElement = "locking")    
   public void setLockAcquisitionTimeout(long lockAcquisitionTimeout) {
      locking.setLockAcquisitionTimeout(lockAcquisitionTimeout);
   }

   public void setLockAcquisitionTimeout(long lockAcquisitionTimeout, TimeUnit timeUnit) {
      setLockAcquisitionTimeout(timeUnit.toMillis(lockAcquisitionTimeout));
   }

   @ConfigurationAttribute(name = "replTimeout", 
            containingElement = "sync")    
   public void setSyncReplTimeout(long syncReplTimeout) {
      this.clustering.sync.setReplTimeout(syncReplTimeout);
   }

   public void setSyncReplTimeout(long syncReplTimeout, TimeUnit timeUnit) {
      setSyncReplTimeout(timeUnit.toMillis(syncReplTimeout));
   }

   public void setCacheMode(CacheMode cacheModeInt) {
      clustering.setMode(cacheModeInt);
   }

   @ConfigurationAttribute(name = "mode", 
            containingElement = "clustering", allowedValues={"LOCAL","REPL","INVALIDATION","DIST"})
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

   @ConfigurationAttribute(name = "wakeUpInterval", 
            containingElement = "eviction")
   public void setEvictionWakeUpInterval(long evictionWakeUpInterval) {
      this.eviction.setWakeUpInterval(evictionWakeUpInterval);
   }

   public EvictionStrategy getEvictionStrategy() {
      return eviction.strategy;
   }

   public void setEvictionStrategy(EvictionStrategy evictionStrategy) {
      this.eviction.setStrategy(evictionStrategy);
   }
   
   @ConfigurationAttribute(name = "strategy", 
            containingElement = "eviction",allowedValues={"NONE", "FIFO", "LRU"})
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

   @ConfigurationAttribute(name = "maxEntries", 
            containingElement = "eviction")
   public void setEvictionMaxEntries(int evictionMaxEntries) {
      this.eviction.setMaxEntries(evictionMaxEntries);
   }

   public long getExpirationLifespan() {
      return expiration.lifespan;
   }

   @ConfigurationAttribute(name = "lifespan", 
            containingElement = "expiration")
   public void setExpirationLifespan(long expirationLifespan) {
      this.expiration.setLifespan(expirationLifespan);
   }

   public long getExpirationMaxIdle() {
      return expiration.maxIdle;
   }

   @ConfigurationAttribute(name = "maxIdle", 
            containingElement = "expiration")
   public void setExpirationMaxIdle(long expirationMaxIdle) {
      this.expiration.setMaxIdle(expirationMaxIdle);
   }

   @ConfigurationAttribute(name = "transactionManagerLookupClass", 
            containingElement = "transaction", 
            description = "",
             defaultValue="org.infinispan.transaction.lookup.GenericTransactionManagerLookup")
   public void setTransactionManagerLookupClass(String transactionManagerLookupClass) {
      this.transaction.setTransactionManagerLookupClass(transactionManagerLookupClass);
   }

   public void setCacheLoaderManagerConfig(CacheLoaderManagerConfig cacheLoaderManagerConfig) {
      this.loaders = cacheLoaderManagerConfig;
   }

   @ConfigurationAttribute(name = "syncCommitPhase", 
            containingElement = "transaction")
   public void setSyncCommitPhase(boolean syncCommitPhase) {
      this.transaction.setSyncCommitPhase(syncCommitPhase);
   }

   @ConfigurationAttribute(name = "syncRollbackPhase", 
            containingElement = "transaction")
   public void setSyncRollbackPhase(boolean syncRollbackPhase) {
      this.transaction.setSyncRollbackPhase(syncRollbackPhase);
   }
   
   @ConfigurationAttribute(name = "useEagerLocking", 
            containingElement = "transaction")           
   public void setUseEagerLocking(boolean useEagerLocking) {
      this.transaction.setUseEagerLocking(useEagerLocking);
   }

   @ConfigurationAttribute(name = "useReplQueue", 
            containingElement = "async")
   public void setUseReplQueue(boolean useReplQueue) {
      this.clustering.async.setUseReplQueue(useReplQueue);
   }

   public void setIsolationLevel(IsolationLevel isolationLevel) {
      locking.setIsolationLevel(isolationLevel);
   }

   @ConfigurationAttribute(name = "timeout", 
            containingElement = "stateRetrieval")
   public void setStateRetrievalTimeout(long stateRetrievalTimeout) {
      this.clustering.stateRetrieval.setTimeout(stateRetrievalTimeout);
   }

   public void setStateRetrievalTimeout(long stateRetrievalTimeout, TimeUnit timeUnit) {
      setStateRetrievalTimeout(timeUnit.toMillis(stateRetrievalTimeout));
   }

   @ConfigurationAttribute(name = "isolationLevel", 
            containingElement = "locking",
            allowedValues={"NONE","SERIALIZABLE","REPEATABLE_READ","READ_COMMITTED","READ_UNCOMMITTED"})    
   public void setIsolationLevel(String isolationLevel) {
      if (isolationLevel == null) throw new ConfigurationException("Isolation level cannot be null", "IsolationLevel");
      locking.setIsolationLevel(IsolationLevel.valueOf(uc(isolationLevel)));
      if (locking.isolationLevel == null) {
         log.warn("Unknown isolation level '" + isolationLevel + "', using defaults.");
         locking.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      }
   }

   @ConfigurationAttribute(name = "enabled", 
            containingElement = "lazyDeserialization") 
   public void setUseLazyDeserialization(boolean useLazyDeserialization) {
      lazyDeserialization.setEnabled(useLazyDeserialization);
   }

   @ConfigurationAttribute(name = "enabled", 
            containingElement = "l1")   
   public void setL1CacheEnabled(boolean l1CacheEnabled) {
      this.clustering.l1.setEnabled(l1CacheEnabled);
   }


   @ConfigurationAttribute(name = "lifespan", 
            containingElement = "l1")   
   public void setL1Lifespan(long l1Lifespan) {
      this.clustering.l1.setLifespan(l1Lifespan);
   }

   @ConfigurationAttribute(name = "onRehash", 
            containingElement = "l1")   
   public void setL1OnRehash(boolean l1OnRehash) {
      this.clustering.l1.setOnRehash(l1OnRehash);
   }

   @ConfigurationAttribute(name = "consistentHashClass", 
            containingElement = "hash")   
   public void setConsistentHashClass(String consistentHashClass) {
      this.clustering.hash.setConsistentHashClass(consistentHashClass);
   }
   
   @ConfigurationAttribute(name = "numOwners", 
            containingElement = "hash")    
   public void setNumOwners(int numOwners) {
      this.clustering.hash.setNumOwners(numOwners);
   }

   @ConfigurationAttribute(name = "rehashWait", 
            containingElement = "hash")    
   public void setRehashWaitTime(long rehashWaitTime) {
      this.clustering.hash.setRehashWait(rehashWaitTime);
   }

   @ConfigurationAttribute(name = "asyncMarshalling", 
            containingElement = "async")
   public void setUseAsyncMarshalling(boolean useAsyncMarshalling) {
      this.clustering.async.setAsyncMarshalling(useAsyncMarshalling);
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

   public long getRehashWaitTime() {
      return clustering.hash.rehashWait;
   }

   // ------------------------------------------------------------------------------------------------------------
   //   HELPERS
   // ------------------------------------------------------------------------------------------------------------

   // ------------------------------------------------------------------------------------------------------------
   //   OVERRIDDEN METHODS
   // ------------------------------------------------------------------------------------------------------------

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Configuration that = (Configuration) o;

      if (locking.concurrencyLevel != that.locking.concurrencyLevel) return false;
      if (eviction.maxEntries != that.eviction.maxEntries) return false;
      if (eviction.wakeUpInterval != that.eviction.wakeUpInterval) return false;
      if (expiration.lifespan != that.expiration.lifespan) return false;
      if (expiration.maxIdle != that.expiration.maxIdle) return false;
      if (jmxStatistics.enabled != that.jmxStatistics.enabled) return false;
      if (clustering.stateRetrieval.fetchInMemoryState != that.clustering.stateRetrieval.fetchInMemoryState) return false;
      if (invocationBatching.enabled  != that.invocationBatching.enabled ) return false;
      if (clustering.l1.enabled != that.clustering.l1.enabled) return false;
      if (clustering.l1.lifespan != that.clustering.l1.lifespan) return false;
      if (clustering.hash.rehashWait != that.clustering.hash.rehashWait) return false;
      if (clustering.l1.onRehash != that.clustering.l1.onRehash) return false;
      if (locking.lockAcquisitionTimeout != that.locking.lockAcquisitionTimeout) return false;
      if (clustering.hash.numOwners != that.clustering.hash.numOwners) return false;
      if (clustering.async.replQueueInterval != that.clustering.async.replQueueInterval) return false;
      if (clustering.async.replQueueMaxElements != that.clustering.async.replQueueMaxElements) return false;
      if (clustering.stateRetrieval.timeout != that.clustering.stateRetrieval.timeout) return false;
      if (transaction.syncCommitPhase != that.transaction.syncCommitPhase) return false;
      if (clustering.sync.replTimeout != that.clustering.sync.replTimeout) return false;
      if (clustering.hash.rehashRpcTimeout != that.clustering.hash.rehashRpcTimeout) return false;
      if (transaction.syncRollbackPhase != that.transaction.syncRollbackPhase) return false;
      if (transaction.useEagerLocking != that.transaction.useEagerLocking) return false;
      if (lazyDeserialization.enabled != that.lazyDeserialization.enabled) return false;
      if (locking.useLockStriping != that.locking.useLockStriping) return false;
      if (clustering.async.useReplQueue != that.clustering.async.useReplQueue) return false;
      if (locking.writeSkewCheck != that.locking.writeSkewCheck) return false;
      if (loaders != null ? !loaders.equals(that.loaders) : that.loaders != null)
         return false;
      if (clustering.mode != that.clustering.mode) return false;
      if (clustering.hash.consistentHashClass != null ? !clustering.hash.consistentHashClass.equals(that.clustering.hash.consistentHashClass) : that.clustering.hash.consistentHashClass != null)
         return false;
      if (customInterceptors.customInterceptors != null ? !customInterceptors.customInterceptors.equals(that.customInterceptors.customInterceptors) : that.customInterceptors.customInterceptors != null)
         return false;
      if (eviction.strategy != that.eviction.strategy) return false;
      if (globalConfiguration != null ? !globalConfiguration.equals(that.globalConfiguration) : that.globalConfiguration != null)
         return false;
      if (locking.isolationLevel != that.locking.isolationLevel) return false;
      if (transaction.transactionManagerLookupClass != null ? !transaction.transactionManagerLookupClass.equals(that.transaction.transactionManagerLookupClass) : that.transaction.transactionManagerLookupClass != null)
         return false;
      if (clustering.async.asyncMarshalling != that.clustering.async.asyncMarshalling) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = globalConfiguration != null ? globalConfiguration.hashCode() : 0;
      result = 31 * result + (locking.useLockStriping ? 1 : 0);
      result = 31 * result + (clustering.async.useReplQueue ? 1 : 0);
      result = 31 * result + clustering.async.replQueueMaxElements;
      result = 31 * result + (int) (clustering.async.replQueueInterval ^ (clustering.async.replQueueInterval >>> 32));
      result = 31 * result + (jmxStatistics.enabled ? 1 : 0);
      result = 31 * result + (clustering.stateRetrieval.fetchInMemoryState ? 1 : 0);
      result = 31 * result + (int) (locking.lockAcquisitionTimeout ^ (locking.lockAcquisitionTimeout >>> 32));
      result = 31 * result + (int) (clustering.sync.replTimeout ^ (clustering.sync.replTimeout >>> 32));
      result = 31 * result + (clustering.mode != null ? clustering.mode.hashCode() : 0);
      result = 31 * result + (int) (clustering.stateRetrieval.timeout ^ (clustering.stateRetrieval.timeout >>> 32));
      result = 31 * result + (locking.isolationLevel != null ? locking.isolationLevel.hashCode() : 0);
      result = 31 * result + (transaction.transactionManagerLookupClass != null ? transaction.transactionManagerLookupClass.hashCode() : 0);
      result = 31 * result + (loaders != null ? loaders.hashCode() : 0);
      result = 31 * result + (transaction.syncCommitPhase ? 1 : 0);
      result = 31 * result + (transaction.syncRollbackPhase ? 1 : 0);
      result = 31 * result + (transaction.useEagerLocking ? 1 : 0);
      result = 31 * result + (lazyDeserialization.enabled ? 1 : 0);
      result = 31 * result + (customInterceptors.customInterceptors != null ? customInterceptors.customInterceptors.hashCode() : 0);
      result = 31 * result + (locking.writeSkewCheck ? 1 : 0);
      result = 31 * result + locking.concurrencyLevel;
      result = 31 * result + (invocationBatching.enabled  ? 1 : 0);
      result = 31 * result + (int) (eviction.wakeUpInterval ^ (eviction.wakeUpInterval >>> 32));
      result = 31 * result + (eviction.strategy != null ? eviction.strategy.hashCode() : 0);
      result = 31 * result + eviction.maxEntries;
      result = 31 * result + (int) (expiration.lifespan ^ (expiration.lifespan >>> 32));
      result = 31 * result + (int) (expiration.maxIdle ^ (expiration.maxIdle >>> 32));
      result = 31 * result + (clustering.l1.enabled ? 1 : 0);
      result = 31 * result + (int) (clustering.l1.lifespan ^ (clustering.l1.lifespan >>> 32));
      result = 31 * result + (int) (clustering.hash.rehashWait ^ (clustering.hash.rehashWait >>> 32));
      result = 31 * result + (int) (clustering.hash.rehashRpcTimeout ^ (clustering.hash.rehashRpcTimeout >>> 32));
      result = 31 * result + (clustering.l1.onRehash ? 1 : 0);
      result = 31 * result + (clustering.hash.consistentHashClass != null ? clustering.hash.consistentHashClass.hashCode() : 0);
      result = 31 * result + clustering.hash.numOwners;
      result = 31 * result + (clustering.async.asyncMarshalling ? 1 : 0);
      return result;
   }

   @Override
   public Configuration clone() {
      
      Configuration obj = null;
      try {
          // Write the object out to a byte array
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutputStream out = new ObjectOutputStream(bos);
          out.writeObject(this);
          out.flush();
          out.close();

          // Make an input stream from the byte array and read
          // a copy of the object back in.
          ObjectInputStream in = new ObjectInputStream(
              new ByteArrayInputStream(bos.toByteArray()));
          obj= (Configuration) in.readObject();
      }
      catch(IOException e) {
          e.printStackTrace();
      }
      catch(ClassNotFoundException cnfe) {
          cnfe.printStackTrace();
      }
      return obj;
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
         throw new ConfigurationException("Cache cannot use DISTRIBUTION mode and have fetchInMemoryState set to true");
   }

   public boolean isOnePhaseCommit() {
      return !getCacheMode().isSynchronous();
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class TransactionType extends AbstractNamedCacheConfigurationBean{
           
      /** The serialVersionUID */
      private static final long serialVersionUID = -3867090839830874603L;

      private String transactionManagerLookupClass;
      
      @Dynamic
      private Boolean syncCommitPhase = false;
      
      @Dynamic
      private Boolean syncRollbackPhase = false;
      
      @Dynamic
      private Boolean useEagerLocking = false;
      
      public TransactionType(String transactionManagerLookupClass) {
         this.transactionManagerLookupClass = transactionManagerLookupClass;
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
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class LockingType  extends AbstractNamedCacheConfigurationBean{      
      

      /** The serialVersionUID */
      private static final long serialVersionUID = 8142143187082119506L;

      @Dynamic
      private Long lockAcquisitionTimeout = 10000L;
      
      private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
      
      private Boolean writeSkewCheck = false;
    
      private Boolean useLockStriping = true;
      
      private Integer concurrencyLevel = 500;   
      
      @XmlAttribute
      public void setLockAcquisitionTimeout(Long lockAcquisitionTimeout) {
         testImmutability("lockAcquisitionTimeout");
         this.lockAcquisitionTimeout = lockAcquisitionTimeout;
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
   } 
   
   @XmlAccessorType(XmlAccessType.FIELD)
   @XmlType(propOrder={})
   private static class ClusteringType extends AbstractNamedCacheConfigurationBean {
      
      /** The serialVersionUID */
      private static final long serialVersionUID = 4048135465543498430L;

      @XmlTransient
      private CacheMode mode = CacheMode.LOCAL;
      
      @XmlElement
      private SyncType sync = new SyncType();
      
      @XmlElement
      private StateRetrievalType stateRetrieval = new StateRetrievalType();
      
      @XmlElement
      private L1Type l1 = new L1Type();
      
      @XmlElement
      private AsyncType async = new AsyncType();
      
      @XmlElement
      private HashType hash = new HashType();

      @XmlAttribute
      public void setMode(CacheMode mode) {
         testImmutability("mode");
         this.mode = mode;
      }
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class AsyncType extends AbstractNamedCacheConfigurationBean {

      /** The serialVersionUID */
      private static final long serialVersionUID = -7726319188826197399L;

      private Boolean useReplQueue=false;
      
      private Integer replQueueMaxElements=1000;
      
      private Long replQueueInterval=5000L;
      
      private Boolean asyncMarshalling=true;
      
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
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class ExpirationType extends AbstractNamedCacheConfigurationBean{

      /** The serialVersionUID */
      private static final long serialVersionUID = 5757161438110848530L;

      private Long lifespan=-1L;
      
      private Long maxIdle=-1L;
      
      @XmlAttribute
      public void setLifespan(Long lifespan) {
         testImmutability("lifespan");
         this.lifespan = lifespan;
      }

      @XmlAttribute
      public void setMaxIdle(Long maxIdle) {
         testImmutability("maxIdle");
         this.maxIdle = maxIdle;
      }
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class EvictionType extends AbstractNamedCacheConfigurationBean {

      /** The serialVersionUID */
      private static final long serialVersionUID = -1248563712058858791L;

      private Long wakeUpInterval=5000L;
    
      private EvictionStrategy strategy=EvictionStrategy.NONE;
      
      private Integer maxEntries=-1;      
      
      @XmlAttribute
      public void setWakeUpInterval(Long wakeUpInterval) {
         testImmutability("wakeUpInterval");
         this.wakeUpInterval = wakeUpInterval;
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
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class StateRetrievalType extends AbstractNamedCacheConfigurationBean {

      /** The serialVersionUID */
      private static final long serialVersionUID = 3709234918426217096L;

      @Dynamic
      private Boolean fetchInMemoryState = false;
      
      @Dynamic      
      private Long timeout=10000L;     
      
      @XmlAttribute
      public void setFetchInMemoryState(Boolean fetchInMemoryState) {
         testImmutability("fetchInMemoryState");
         this.fetchInMemoryState = fetchInMemoryState;
      }

      @XmlAttribute
      public void setTimeout(Long timeout) {
         testImmutability("timeout");
         this.timeout = timeout;
      }
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class SyncType  extends AbstractNamedCacheConfigurationBean {
      /** The serialVersionUID */
      private static final long serialVersionUID = 8419216253674289524L;
      
      @Dynamic
      private Long replTimeout=15000L;
      
      @XmlAttribute
      public void setReplTimeout(Long replTimeout) {
         testImmutability("replTimeout");
         this.replTimeout = replTimeout;
      }
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class HashType extends AbstractNamedCacheConfigurationBean {

      /** The serialVersionUID */
      private static final long serialVersionUID = 752218766840948822L;

      private String consistentHashClass=DefaultConsistentHash.class.getName();
      
      private Integer numOwners=2;
      
      private Long rehashWait=60000L;
      
      private Long rehashRpcTimeout=60 * 1000 * 10L;     
      
      @XmlAttribute(name="class")
      public void setConsistentHashClass(String consistentHashClass) {
         testImmutability("class");
         this.consistentHashClass = consistentHashClass;
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
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class L1Type extends AbstractNamedCacheConfigurationBean {
      
      /** The serialVersionUID */
      private static final long serialVersionUID = -4703587764861110638L;

      private Boolean enabled=true;

      private Long lifespan=600000L;
      
      private Boolean onRehash=true;
      
      @XmlAttribute
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
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
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class BooleanAttributeType  extends AbstractNamedCacheConfigurationBean {
     
      /** The serialVersionUID */
      private static final long serialVersionUID = 2296863404153834686L;
      Boolean enabled = false;
      
      @XmlAttribute
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class DeadlockDetectionType  extends AbstractNamedCacheConfigurationBean{
      
      /** The serialVersionUID */
      private static final long serialVersionUID = -7178286048602531152L;

      private Boolean enabled=false;
      
      private Long spinDuration=100L;
      
      @XmlAttribute
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      @XmlAttribute
      public void setSpinDuration(Long spinDuration) {
         testImmutability("spinDuration");
         this.spinDuration = spinDuration;
      }
   }
   
   @XmlAccessorType(XmlAccessType.PROPERTY)
   private static class UnsafeType  extends AbstractNamedCacheConfigurationBean{
      
      /** The serialVersionUID */
      private static final long serialVersionUID = -9200921443651234163L;
      private Boolean unreliableReturnValues=false;
      
      @XmlAttribute
      public void setUnreliableReturnValues(Boolean unreliableReturnValues) {
         testImmutability("unreliableReturnValues");
         this.unreliableReturnValues = unreliableReturnValues;
      }
   }
   
   @XmlAccessorType(XmlAccessType.FIELD)
   private static class CustomInterceptorsType extends AbstractNamedCacheConfigurationBean {
      
      /** The serialVersionUID */
      private static final long serialVersionUID = 7187545782011884661L;
      
      @XmlElement(name="interceptor")
      private List<CustomInterceptorConfig> customInterceptors= new ArrayList<CustomInterceptorConfig>();
   }
}