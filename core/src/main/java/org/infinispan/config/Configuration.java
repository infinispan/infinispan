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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.config.parsing.ClusteringConfigReader;
import org.infinispan.config.parsing.CustomInterceptorConfigReader;
import org.infinispan.distribution.DefaultConsistentHash;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.annotations.Start;
import org.infinispan.config.parsing.CustomIntereceptorsSchemaWriter;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.concurrent.IsolationLevel;

/**
 * Encapsulates the configuration of a Cache.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
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
         @ConfigurationElement(name = "customInterceptors", parent = "default", 
                  customReader=CustomInterceptorConfigReader.class,
                  customWriter=CustomIntereceptorsSchemaWriter.class)         
})
public class Configuration extends AbstractNamedCacheConfigurationBean {
   private static final long serialVersionUID = 5553791890144997466L;

   private boolean useDeadlockDetection = false;

   // reference to a global configuration
   private GlobalConfiguration globalConfiguration;

   public GlobalConfiguration getGlobalConfiguration() {
      return globalConfiguration;
   }

   @Inject
   private void injectGlobalConfiguration(GlobalConfiguration globalConfiguration) {
      this.globalConfiguration = globalConfiguration;
   }

   public boolean isStateTransferEnabled() {
      return fetchInMemoryState || (cacheLoaderManagerConfig != null && cacheLoaderManagerConfig.isFetchPersistentState());
   }

   public void setUseLockStriping(boolean useLockStriping) {
      testImmutability("useLockStriping");
      this.useLockStriping = useLockStriping;
   }

   public boolean isUseLockStriping() {
      return useLockStriping;
   }

   public boolean isUnsafeUnreliableReturnValues() {
      return unsafeUnreliableReturnValues;
   }

   @ConfigurationAttribute(name = "unreliableReturnValues", 
            containingElement = "unsafe")    
   public void setUnsafeUnreliableReturnValues(boolean unsafeUnreliableReturnValues) {
      testImmutability("unsafeUnreliableReturnValues");
      this.unsafeUnreliableReturnValues = unsafeUnreliableReturnValues;
   }

   @ConfigurationAttribute(name = "rehashRpcTimeout", 
            containingElement = "hash")    
   public void setRehashRpcTimeout(long rehashRpcTimeout) {
      testImmutability("rehashRpcTimeout");
      this.rehashRpcTimeout = rehashRpcTimeout;
   }

   public long getRehashRpcTimeout() {
      return rehashRpcTimeout;
   }
   
   public boolean isUseDeadlockDetection() {
      return useDeadlockDetection;
   }

   public void setUseDeadlockDetection(boolean useDeadlockDetection) {
      this.useDeadlockDetection = useDeadlockDetection;
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

   // ------------------------------------------------------------------------------------------------------------
   //   CONFIGURATION OPTIONS
   // ------------------------------------------------------------------------------------------------------------

   private boolean useReplQueue = false;
   private int replQueueMaxElements = 1000;
   private long replQueueInterval = 5000;
   private boolean exposeJmxStatistics = false;
   @Dynamic
   private boolean fetchInMemoryState = false;
   @Dynamic
   private long lockAcquisitionTimeout = 10000;
   @Dynamic
   private long syncReplTimeout = 15000;
   private CacheMode cacheMode = CacheMode.LOCAL;
   @Dynamic
   private long stateRetrievalTimeout = 10000;
   private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
   private String transactionManagerLookupClass = null;
   private CacheLoaderManagerConfig cacheLoaderManagerConfig = null;
   @Dynamic
   private boolean syncCommitPhase = false;
   @Dynamic
   private boolean syncRollbackPhase = false;
   @Dynamic
   private boolean useEagerLocking = false;
   private boolean useLazyDeserialization = false;
   private List<CustomInterceptorConfig> customInterceptors = Collections.emptyList();
   private boolean writeSkewCheck = false;
   private int concurrencyLevel = 500;
   private boolean invocationBatchingEnabled;
   private long evictionWakeUpInterval = 5000;
   private EvictionStrategy evictionStrategy = EvictionStrategy.NONE;
   private int evictionMaxEntries = -1;
   private long expirationLifespan = -1;
   private long expirationMaxIdle = -1;
   private boolean l1CacheEnabled = true;
   private long l1Lifespan = 600000;
   private boolean l1OnRehash = true;
   private String consistentHashClass = DefaultConsistentHash.class.getName();
   private int numOwners = 2;
   private long rehashWaitTime = 60000;
   private boolean useLockStriping = true;
   private boolean unsafeUnreliableReturnValues = false;
   private boolean useAsyncMarshalling = true;
   private long rehashRpcTimeout = 60 * 1000 * 10; // 10 minutes

   @Start(priority = 1)
   private void correctIsolationLevels() {
      // ensure the correct isolation level upgrades and/or downgrades are performed.
      switch (isolationLevel) {
         case NONE:
         case READ_UNCOMMITTED:
            isolationLevel = IsolationLevel.READ_COMMITTED;
            break;
         case SERIALIZABLE:
            isolationLevel = IsolationLevel.REPEATABLE_READ;
            break;
      }
   }

   // ------------------------------------------------------------------------------------------------------------
   //   SETTERS - MAKE SURE ALL SETTERS PERFORM testImmutability()!!!
   // ------------------------------------------------------------------------------------------------------------

   public boolean isWriteSkewCheck() {
      return writeSkewCheck;
   }


   @ConfigurationAttribute(name = "writeSkewCheck", 
            containingElement = "locking")    
   public void setWriteSkewCheck(boolean writeSkewCheck) {
      testImmutability("writeSkewCheck");
      this.writeSkewCheck = writeSkewCheck;
   }

   public int getConcurrencyLevel() {
      return concurrencyLevel;
   }


   @ConfigurationAttribute(name = "concurrencyLevel", 
            containingElement = "locking")    
   public void setConcurrencyLevel(int concurrencyLevel) {
      testImmutability("concurrencyLevel");
      this.concurrencyLevel = concurrencyLevel;
   }

   @ConfigurationAttribute(name = "replQueueMaxElements", 
            containingElement = "async")
   public void setReplQueueMaxElements(int replQueueMaxElements) {
      testImmutability("replQueueMaxElements");
      this.replQueueMaxElements = replQueueMaxElements;
   }

   @ConfigurationAttribute(name = "replQueueInterval", 
            containingElement = "async")
   public void setReplQueueInterval(long replQueueInterval) {
      testImmutability("replQueueInterval");
      this.replQueueInterval = replQueueInterval;
   }

   public void setReplQueueInterval(long replQueueInterval, TimeUnit timeUnit) {
      setReplQueueInterval(timeUnit.toMillis(replQueueInterval));
   }

   @ConfigurationAttribute(name = "enabled", 
            containingElement = "jmxStatistics")   
   public void setExposeJmxStatistics(boolean useMbean) {
      testImmutability("exposeJmxStatistics");
      this.exposeJmxStatistics = useMbean;
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
      testImmutability("invocationBatchingEnabled");
      this.invocationBatchingEnabled = enabled;
   }

   @ConfigurationAttribute(name = "fetchInMemoryState", 
            containingElement = "stateRetrieval")
   public void setFetchInMemoryState(boolean fetchInMemoryState) {
      testImmutability("fetchInMemoryState");
      this.fetchInMemoryState = fetchInMemoryState;
   }

   @ConfigurationAttribute(name = "lockAcquisitionTimeout", 
            containingElement = "locking")    
   public void setLockAcquisitionTimeout(long lockAcquisitionTimeout) {
      testImmutability("lockAcquisitionTimeout");
      this.lockAcquisitionTimeout = lockAcquisitionTimeout;
   }

   public void setLockAcquisitionTimeout(long lockAcquisitionTimeout, TimeUnit timeUnit) {
      setLockAcquisitionTimeout(timeUnit.toMillis(lockAcquisitionTimeout));
   }

   @ConfigurationAttribute(name = "replTimeout", 
            containingElement = "sync")    
   public void setSyncReplTimeout(long syncReplTimeout) {
      testImmutability("syncReplTimeout");
      this.syncReplTimeout = syncReplTimeout;
   }

   public void setSyncReplTimeout(long syncReplTimeout, TimeUnit timeUnit) {
      setSyncReplTimeout(timeUnit.toMillis(syncReplTimeout));
   }

   public void setCacheMode(CacheMode cacheModeInt) {
      testImmutability("cacheMode");
      this.cacheMode = cacheModeInt;
   }

   @ConfigurationAttribute(name = "mode", 
            containingElement = "clustering", allowedValues="LOCAL,REPL,INVALIDATION,DIST")
   public void setCacheMode(String cacheMode) {
      testImmutability("cacheMode");
      if (cacheMode == null) throw new ConfigurationException("Cache mode cannot be null", "CacheMode");
      this.cacheMode = CacheMode.valueOf(uc(cacheMode));
      if (this.cacheMode == null) {
         log.warn("Unknown cache mode '" + cacheMode + "', using defaults.");
         this.cacheMode = CacheMode.LOCAL;
      }
   }

   public String getCacheModeString() {
      return cacheMode == null ? null : cacheMode.toString();
   }

   public void setCacheModeString(String cacheMode) {
      setCacheMode(cacheMode);
   }

   public long getEvictionWakeUpInterval() {
      return evictionWakeUpInterval;
   }

   @ConfigurationAttribute(name = "wakeUpInterval", 
            containingElement = "eviction")
   public void setEvictionWakeUpInterval(long evictionWakeUpInterval) {
      testImmutability("evictionWakeUpInterval");
      this.evictionWakeUpInterval = evictionWakeUpInterval;
   }

   public EvictionStrategy getEvictionStrategy() {
      return evictionStrategy;
   }

   public void setEvictionStrategy(EvictionStrategy evictionStrategy) {
      testImmutability("evictionStrategy");
      this.evictionStrategy = evictionStrategy;
   }
   
   @ConfigurationAttribute(name = "strategy", 
            containingElement = "eviction",allowedValues="NONE, FIFO, LRU")
   public void setEvictionStrategy(String eStrategy){
      testImmutability("evictionStrategy");
      this.evictionStrategy = EvictionStrategy.valueOf(uc(eStrategy));
      if (this.evictionStrategy == null) {
         log.warn("Unknown evictionStrategy  '" + eStrategy + "', using defaults.");
         this.evictionStrategy = EvictionStrategy.NONE;
      }
   }

   public int getEvictionMaxEntries() {
      return evictionMaxEntries;
   }

   @ConfigurationAttribute(name = "maxEntries", 
            containingElement = "eviction")
   public void setEvictionMaxEntries(int evictionMaxEntries) {
      testImmutability("evictionMaxEntries");
      this.evictionMaxEntries = evictionMaxEntries;
   }

   public long getExpirationLifespan() {
      return expirationLifespan;
   }

   @ConfigurationAttribute(name = "lifespan", 
            containingElement = "expiration")
   public void setExpirationLifespan(long expirationLifespan) {
      testImmutability("expirationLifespan");
      this.expirationLifespan = expirationLifespan;
   }

   public long getExpirationMaxIdle() {
      return expirationMaxIdle;
   }

   @ConfigurationAttribute(name = "maxIdle", 
            containingElement = "expiration")
   public void setExpirationMaxIdle(long expirationMaxIdle) {
      testImmutability("expirationMaxIdle");
      this.expirationMaxIdle = expirationMaxIdle;
   }

   @ConfigurationAttribute(name = "transactionManagerLookupClass", 
            containingElement = "transaction", 
            description = "",
             defaultValue="org.infinispan.transaction.lookup.GenericTransactionManagerLookup")
   public void setTransactionManagerLookupClass(String transactionManagerLookupClass) {
      testImmutability("transactionManagerLookupClass");
      this.transactionManagerLookupClass = transactionManagerLookupClass;
   }

   public void setCacheLoaderManagerConfig(CacheLoaderManagerConfig cacheLoaderManagerConfig) {
      testImmutability("cacheLoaderManagerConfig");
      this.cacheLoaderManagerConfig = cacheLoaderManagerConfig;
   }

   @ConfigurationAttribute(name = "syncCommitPhase", 
            containingElement = "transaction")
   public void setSyncCommitPhase(boolean syncCommitPhase) {
      testImmutability("syncCommitPhase");
      this.syncCommitPhase = syncCommitPhase;
   }

   @ConfigurationAttribute(name = "syncRollbackPhase", 
            containingElement = "transaction")
   public void setSyncRollbackPhase(boolean syncRollbackPhase) {
      testImmutability("syncRollbackPhase");
      this.syncRollbackPhase = syncRollbackPhase;
   }
   
   @ConfigurationAttribute(name = "useEagerLocking", 
            containingElement = "transaction")           
   public void setUseEagerLocking(boolean useEagerLocking) {
      testImmutability("useEagerLocking");
      this.useEagerLocking = useEagerLocking;
   }

   @ConfigurationAttribute(name = "useReplQueue", 
            containingElement = "async")
   public void setUseReplQueue(boolean useReplQueue) {
      testImmutability("useReplQueue");
      this.useReplQueue = useReplQueue;
   }

   public void setIsolationLevel(IsolationLevel isolationLevel) {
      testImmutability("isolationLevel");
      this.isolationLevel = isolationLevel;
   }

   @ConfigurationAttribute(name = "timeout", 
            containingElement = "stateRetrieval")
   public void setStateRetrievalTimeout(long stateRetrievalTimeout) {
      testImmutability("stateRetrievalTimeout");
      this.stateRetrievalTimeout = stateRetrievalTimeout;
   }

   public void setStateRetrievalTimeout(long stateRetrievalTimeout, TimeUnit timeUnit) {
      setStateRetrievalTimeout(timeUnit.toMillis(stateRetrievalTimeout));
   }

   @ConfigurationAttribute(name = "isolationLevel", 
            containingElement = "locking",
            allowedValues="NONE,SERIALIZABLE,REPEATABLE_READ,READ_COMMITTED,READ_UNCOMMITTED")    
   public void setIsolationLevel(String isolationLevel) {
      testImmutability("isolationLevel");
      if (isolationLevel == null) throw new ConfigurationException("Isolation level cannot be null", "IsolationLevel");
      this.isolationLevel = IsolationLevel.valueOf(uc(isolationLevel));
      if (this.isolationLevel == null) {
         log.warn("Unknown isolation level '" + isolationLevel + "', using defaults.");
         this.isolationLevel = IsolationLevel.REPEATABLE_READ;
      }
   }

   @ConfigurationAttribute(name = "enabled", 
            containingElement = "lazyDeserialization") 
   public void setUseLazyDeserialization(boolean useLazyDeserialization) {
      testImmutability("useLazyDeserialization");
      this.useLazyDeserialization = useLazyDeserialization;
   }

   @ConfigurationAttribute(name = "enabled", 
            containingElement = "l1")   
   public void setL1CacheEnabled(boolean l1CacheEnabled) {
      testImmutability("l1CacheEnabled");
      this.l1CacheEnabled = l1CacheEnabled;
   }


   @ConfigurationAttribute(name = "lifespan", 
            containingElement = "l1")   
   public void setL1Lifespan(long l1Lifespan) {
      testImmutability("l1Lifespan");
      this.l1Lifespan = l1Lifespan;
   }

   @ConfigurationAttribute(name = "onRehash", 
            containingElement = "l1")   
   public void setL1OnRehash(boolean l1OnRehash) {
      testImmutability("l1OnRehash");
      this.l1OnRehash = l1OnRehash;
   }

   @ConfigurationAttribute(name = "consistentHashClass", 
            containingElement = "hash")   
   public void setConsistentHashClass(String consistentHashClass) {
      testImmutability("consistentHashClass");
      this.consistentHashClass = consistentHashClass;
   }
   
   @ConfigurationAttribute(name = "numOwners", 
            containingElement = "hash")    
   public void setNumOwners(int numOwners) {
      testImmutability("numOwners");
      this.numOwners = numOwners;
   }

   @ConfigurationAttribute(name = "rehashWait", 
            containingElement = "hash")    
   public void setRehashWaitTime(long rehashWaitTime) {
      testImmutability("rehashWaitTime");
      this.rehashWaitTime = rehashWaitTime;
   }

   @ConfigurationAttribute(name = "asyncMarshalling", 
            containingElement = "async")
   public void setUseAsyncMarshalling(boolean useAsyncMarshalling) {
      testImmutability("useAsyncMarshalling");
      this.useAsyncMarshalling = useAsyncMarshalling;
   }

   // ------------------------------------------------------------------------------------------------------------
   //   GETTERS
   // ------------------------------------------------------------------------------------------------------------

   public boolean isUseAsyncMarshalling() {
      return useAsyncMarshalling;
   }

   public boolean isUseReplQueue() {
      return useReplQueue;
   }

   public int getReplQueueMaxElements() {
      return replQueueMaxElements;
   }

   public long getReplQueueInterval() {
      return replQueueInterval;
   }

   public boolean isExposeJmxStatistics() {
      return exposeJmxStatistics;
   }

   /**
    * @return true if invocation batching is enabled.
    * @since 4.0
    */
   public boolean isInvocationBatchingEnabled() {
      return invocationBatchingEnabled;
   }

   public boolean isFetchInMemoryState() {
      return fetchInMemoryState;
   }

   public long getLockAcquisitionTimeout() {
      return lockAcquisitionTimeout;
   }

   public long getSyncReplTimeout() {
      return syncReplTimeout;
   }

   public CacheMode getCacheMode() {
      return cacheMode;
   }

   public IsolationLevel getIsolationLevel() {
      return isolationLevel;
   }

   public String getTransactionManagerLookupClass() {
      return transactionManagerLookupClass;
   }

   public CacheLoaderManagerConfig getCacheLoaderManagerConfig() {
      return cacheLoaderManagerConfig;
   }

   public boolean isSyncCommitPhase() {
      return syncCommitPhase;
   }

   public boolean isSyncRollbackPhase() {
      return syncRollbackPhase;
   }
   
   public boolean isUseEagerLocking() {
      return useEagerLocking;
   }

   public long getStateRetrievalTimeout() {
      return stateRetrievalTimeout;
   }

   public boolean isUseLazyDeserialization() {
      return useLazyDeserialization;
   }

   public boolean isL1CacheEnabled() {
      return l1CacheEnabled;
   }

   public long getL1Lifespan() {
      return l1Lifespan;
   }

   public boolean isL1OnRehash() {
      return l1OnRehash;
   }

   public String getConsistentHashClass() {
      return consistentHashClass;
   }

   public int getNumOwners() {
      return numOwners;
   }

   public long getRehashWaitTime() {
      return rehashWaitTime;
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

      if (concurrencyLevel != that.concurrencyLevel) return false;
      if (evictionMaxEntries != that.evictionMaxEntries) return false;
      if (evictionWakeUpInterval != that.evictionWakeUpInterval) return false;
      if (expirationLifespan != that.expirationLifespan) return false;
      if (expirationMaxIdle != that.expirationMaxIdle) return false;
      if (exposeJmxStatistics != that.exposeJmxStatistics) return false;
      if (fetchInMemoryState != that.fetchInMemoryState) return false;
      if (invocationBatchingEnabled != that.invocationBatchingEnabled) return false;
      if (l1CacheEnabled != that.l1CacheEnabled) return false;
      if (l1Lifespan != that.l1Lifespan) return false;
      if (rehashWaitTime != that.rehashWaitTime) return false;
      if (l1OnRehash != that.l1OnRehash) return false;
      if (lockAcquisitionTimeout != that.lockAcquisitionTimeout) return false;
      if (numOwners != that.numOwners) return false;
      if (replQueueInterval != that.replQueueInterval) return false;
      if (replQueueMaxElements != that.replQueueMaxElements) return false;
      if (stateRetrievalTimeout != that.stateRetrievalTimeout) return false;
      if (syncCommitPhase != that.syncCommitPhase) return false;
      if (syncReplTimeout != that.syncReplTimeout) return false;
      if (rehashRpcTimeout != that.rehashRpcTimeout) return false;
      if (syncRollbackPhase != that.syncRollbackPhase) return false;
      if (useEagerLocking != that.useEagerLocking) return false;
      if (useLazyDeserialization != that.useLazyDeserialization) return false;
      if (useLockStriping != that.useLockStriping) return false;
      if (useReplQueue != that.useReplQueue) return false;
      if (writeSkewCheck != that.writeSkewCheck) return false;
      if (cacheLoaderManagerConfig != null ? !cacheLoaderManagerConfig.equals(that.cacheLoaderManagerConfig) : that.cacheLoaderManagerConfig != null)
         return false;
      if (cacheMode != that.cacheMode) return false;
      if (consistentHashClass != null ? !consistentHashClass.equals(that.consistentHashClass) : that.consistentHashClass != null)
         return false;
      if (customInterceptors != null ? !customInterceptors.equals(that.customInterceptors) : that.customInterceptors != null)
         return false;
      if (evictionStrategy != that.evictionStrategy) return false;
      if (globalConfiguration != null ? !globalConfiguration.equals(that.globalConfiguration) : that.globalConfiguration != null)
         return false;
      if (isolationLevel != that.isolationLevel) return false;
      if (transactionManagerLookupClass != null ? !transactionManagerLookupClass.equals(that.transactionManagerLookupClass) : that.transactionManagerLookupClass != null)
         return false;
      if (useAsyncMarshalling != that.useAsyncMarshalling) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = globalConfiguration != null ? globalConfiguration.hashCode() : 0;
      result = 31 * result + (useLockStriping ? 1 : 0);
      result = 31 * result + (useReplQueue ? 1 : 0);
      result = 31 * result + replQueueMaxElements;
      result = 31 * result + (int) (replQueueInterval ^ (replQueueInterval >>> 32));
      result = 31 * result + (exposeJmxStatistics ? 1 : 0);
      result = 31 * result + (fetchInMemoryState ? 1 : 0);
      result = 31 * result + (int) (lockAcquisitionTimeout ^ (lockAcquisitionTimeout >>> 32));
      result = 31 * result + (int) (syncReplTimeout ^ (syncReplTimeout >>> 32));
      result = 31 * result + (cacheMode != null ? cacheMode.hashCode() : 0);
      result = 31 * result + (int) (stateRetrievalTimeout ^ (stateRetrievalTimeout >>> 32));
      result = 31 * result + (isolationLevel != null ? isolationLevel.hashCode() : 0);
      result = 31 * result + (transactionManagerLookupClass != null ? transactionManagerLookupClass.hashCode() : 0);
      result = 31 * result + (cacheLoaderManagerConfig != null ? cacheLoaderManagerConfig.hashCode() : 0);
      result = 31 * result + (syncCommitPhase ? 1 : 0);
      result = 31 * result + (syncRollbackPhase ? 1 : 0);
      result = 31 * result + (useEagerLocking ? 1 : 0);
      result = 31 * result + (useLazyDeserialization ? 1 : 0);
      result = 31 * result + (customInterceptors != null ? customInterceptors.hashCode() : 0);
      result = 31 * result + (writeSkewCheck ? 1 : 0);
      result = 31 * result + concurrencyLevel;
      result = 31 * result + (invocationBatchingEnabled ? 1 : 0);
      result = 31 * result + (int) (evictionWakeUpInterval ^ (evictionWakeUpInterval >>> 32));
      result = 31 * result + (evictionStrategy != null ? evictionStrategy.hashCode() : 0);
      result = 31 * result + evictionMaxEntries;
      result = 31 * result + (int) (expirationLifespan ^ (expirationLifespan >>> 32));
      result = 31 * result + (int) (expirationMaxIdle ^ (expirationMaxIdle >>> 32));
      result = 31 * result + (l1CacheEnabled ? 1 : 0);
      result = 31 * result + (int) (l1Lifespan ^ (l1Lifespan >>> 32));
      result = 31 * result + (int) (rehashWaitTime ^ (rehashWaitTime >>> 32));
      result = 31 * result + (int) (rehashRpcTimeout ^ (rehashRpcTimeout >>> 32));
      result = 31 * result + (l1OnRehash ? 1 : 0);
      result = 31 * result + (consistentHashClass != null ? consistentHashClass.hashCode() : 0);
      result = 31 * result + numOwners;
      result = 31 * result + (useAsyncMarshalling ? 1 : 0);
      return result;
   }

   @Override
   public Configuration clone() {
      try {
         Configuration c = (Configuration) super.clone();
         if (cacheLoaderManagerConfig != null) {
            c.setCacheLoaderManagerConfig(cacheLoaderManagerConfig.clone());
         }
         return c;
      }
      catch (CloneNotSupportedException e) {
         throw new RuntimeException(e);
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
      return customInterceptors == null ? Collections.EMPTY_LIST : customInterceptors;
   }

   /**
    * @see #getCustomInterceptors()
    */
   public void setCustomInterceptors(List<CustomInterceptorConfig> customInterceptors) {
      testImmutability("customInterceptors");
      this.customInterceptors = customInterceptors;
   }

   public void applyOverrides(Configuration overrides) {
      // loop through all overridden elements in the incoming configuration and apply
      for (String overriddenField : overrides.overriddenConfigurationElements) {
         ReflectionUtil.setValue(this, overriddenField, ReflectionUtil.getValue(overrides, overriddenField));
      }
   }

   public void assertValid() throws ConfigurationException {
      // certain combinations are illegal, such as state transfer + DIST
      if (cacheMode.isDistributed() && fetchInMemoryState)
         throw new ConfigurationException("Cache cannot use DISTRIBUTION mode and have fetchInMemoryState set to true");
   }

   public boolean isOnePhaseCommit() {
      return !getCacheMode().isSynchronous();
   }
}
