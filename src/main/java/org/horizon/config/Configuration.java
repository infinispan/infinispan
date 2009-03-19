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
package org.horizon.config;

import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.annotations.Start;
import org.horizon.lock.IsolationLevel;
import org.horizon.util.ReflectionUtil;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates the configuration of a Cache.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
@NonVolatile
public class Configuration extends AbstractNamedCacheConfigurationBean {
   private static final long serialVersionUID = 5553791890144997466L;

   // reference to a global configuration
   private GlobalConfiguration globalConfiguration;
   private String JmxNameBase;
   private boolean useLockStriping = true;


   public GlobalConfiguration getGlobalConfiguration() {
      return globalConfiguration;
   }

   @Inject
   private void injectGlobalConfiguration(GlobalConfiguration globalConfiguration) {
      this.globalConfiguration = globalConfiguration;
   }

   public boolean isUseAsyncSerialization() {
      return useAsyncSerialization;
   }

   public boolean isStateTransferEnabled() {
      return fetchInMemoryState || (cacheLoaderManagerConfig != null && cacheLoaderManagerConfig.isFetchPersistentState());
   }

   /**
    * If JMX statistics are enabled then all 'published' JMX objects will appear under this name. This is optional, if
    * not specified an object name will be created for you by default.
    *
    * @see javax.management.ObjectName
    * @see #isExposeManagementStatistics()
    */
   public void setJmxNameBase(String jmxObjectName) {
      testImmutability("JmxNameBase");
      this.JmxNameBase = jmxObjectName;
   }

   /**
    * @see #setJmxNameBase(String)
    */
   public String getJmxNameBase() {
      return JmxNameBase;
   }

   public void setUseLockStriping(boolean useLockStriping) {
      testImmutability("useLockStriping");
      this.useLockStriping = useLockStriping;
   }

   public boolean isUseLockStriping() {
      return useLockStriping;
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
      INVALIDATION_ASYNC;

      /**
       * Returns true if the mode is invalidation, either sync or async.
       */
      public boolean isInvalidation() {
         return this == INVALIDATION_SYNC || this == INVALIDATION_ASYNC;
      }

      public boolean isSynchronous() {
         return this == REPL_SYNC || this == INVALIDATION_SYNC || this == LOCAL;
      }

      public boolean isClustered() {
         return this != LOCAL;
      }
   }

   // ------------------------------------------------------------------------------------------------------------
   //   CONFIGURATION OPTIONS
   // ------------------------------------------------------------------------------------------------------------

   private boolean useReplQueue = false;
   private int replQueueMaxElements = 1000;
   private long replQueueInterval = 5000;
   private boolean exposeManagementStatistics = true;
   @Dynamic
   private boolean fetchInMemoryState = true;
   @Dynamic
   private long lockAcquisitionTimeout = 10000;
   @Dynamic
   private long syncReplTimeout = 15000;
   private CacheMode cacheMode = CacheMode.LOCAL;
   @Dynamic
   private long stateRetrievalTimeout = 10000;
   private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
   private EvictionConfig evictionConfig = null;
   private String transactionManagerLookupClass = null;
   private CacheLoaderManagerConfig cacheLoaderManagerConfig = null;
   @Dynamic
   private boolean syncCommitPhase = false;
   @Dynamic
   private boolean syncRollbackPhase = false;
   private boolean useLazyDeserialization = false;
   private List<CustomInterceptorConfig> customInterceptors = Collections.emptyList();
   private boolean writeSkewCheck = false;
   private int concurrencyLevel = 500;
   private boolean invocationBatchingEnabled;
   private boolean useAsyncSerialization = true;

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

   public void setWriteSkewCheck(boolean writeSkewCheck) {
      testImmutability("writeSkewCheck");
      this.writeSkewCheck = writeSkewCheck;
   }

   public int getConcurrencyLevel() {
      return concurrencyLevel;
   }

   public void setConcurrencyLevel(int concurrencyLevel) {
      testImmutability("concurrencyLevel");
      this.concurrencyLevel = concurrencyLevel;
   }

   public void setReplQueueMaxElements(int replQueueMaxElements) {
      testImmutability("replQueueMaxElements");
      this.replQueueMaxElements = replQueueMaxElements;
   }

   public void setReplQueueInterval(long replQueueInterval) {
      testImmutability("replQueueInterval");
      this.replQueueInterval = replQueueInterval;
   }

   public void setReplQueueInterval(long replQueueInterval, TimeUnit timeUnit) {
      setReplQueueInterval(timeUnit.toMillis(replQueueInterval));
   }


   public void setExposeManagementStatistics(boolean useMbean) {
      testImmutability("exposeManagementStatistics");
      this.exposeManagementStatistics = useMbean;
   }

   /**
    * Enables invocation batching if set to <tt>true</tt>.  You still need to use {@link org.horizon.Cache#startBatch()}
    * and {@link org.horizon.Cache#endBatch(boolean)} to demarcate the start and end of batches.
    *
    * @param enabled if true, batching is enabled.
    * @since 4.0
    */
   public void setInvocationBatchingEnabled(boolean enabled) {
      testImmutability("invocationBatchingEnabled");
      this.invocationBatchingEnabled = enabled;
   }

   public void setFetchInMemoryState(boolean fetchInMemoryState) {
      testImmutability("fetchInMemoryState");
      this.fetchInMemoryState = fetchInMemoryState;
   }

   public void setLockAcquisitionTimeout(long lockAcquisitionTimeout) {
      testImmutability("lockAcquisitionTimeout");
      this.lockAcquisitionTimeout = lockAcquisitionTimeout;
   }

   public void setLockAcquisitionTimeout(long lockAcquisitionTimeout, TimeUnit timeUnit) {
      setLockAcquisitionTimeout(timeUnit.toMillis(lockAcquisitionTimeout));
   }

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

   public EvictionConfig getEvictionConfig() {
      return evictionConfig;
   }

   public void setEvictionConfig(EvictionConfig config) {
      testImmutability("evictionConfig");
      this.evictionConfig = config;
   }

   public void setTransactionManagerLookupClass(String transactionManagerLookupClass) {
      testImmutability("transactionManagerLookupClass");
      this.transactionManagerLookupClass = transactionManagerLookupClass;
   }

   public void setCacheLoaderManagerConfig(CacheLoaderManagerConfig cacheLoaderManagerConfig) {
      testImmutability("cacheLoaderManagerConfig");
      this.cacheLoaderManagerConfig = cacheLoaderManagerConfig;
   }

   public void setSyncCommitPhase(boolean syncCommitPhase) {
      testImmutability("syncCommitPhase");
      this.syncCommitPhase = syncCommitPhase;
   }

   public void setSyncRollbackPhase(boolean syncRollbackPhase) {
      testImmutability("syncRollbackPhase");
      this.syncRollbackPhase = syncRollbackPhase;
   }

   public void setUseReplQueue(boolean useReplQueue) {
      testImmutability("useReplQueue");
      this.useReplQueue = useReplQueue;
   }

   public void setIsolationLevel(IsolationLevel isolationLevel) {
      testImmutability("isolationLevel");
      this.isolationLevel = isolationLevel;
   }

   public void setStateRetrievalTimeout(long stateRetrievalTimeout) {
      testImmutability("stateRetrievalTimeout");
      this.stateRetrievalTimeout = stateRetrievalTimeout;
   }

   public void setStateRetrievalTimeout(long stateRetrievalTimeout, TimeUnit timeUnit) {
      setStateRetrievalTimeout(timeUnit.toMillis(stateRetrievalTimeout));
   }

   public void setIsolationLevel(String isolationLevel) {
      testImmutability("isolationLevel");
      if (isolationLevel == null) throw new ConfigurationException("Isolation level cannot be null", "IsolationLevel");
      this.isolationLevel = IsolationLevel.valueOf(uc(isolationLevel));
      if (this.isolationLevel == null) {
         log.warn("Unknown isolation level '" + isolationLevel + "', using defaults.");
         this.isolationLevel = IsolationLevel.REPEATABLE_READ;
      }
   }

   public void setUseLazyDeserialization(boolean useLazyDeserialization) {
      testImmutability("useLazyDeserialization");
      this.useLazyDeserialization = useLazyDeserialization;
   }

   public void setUseAsyncSerialization(boolean useAsyncSerialization) {
      testImmutability("useAsyncSerialization");
      this.useAsyncSerialization = useAsyncSerialization;
   }

   // ------------------------------------------------------------------------------------------------------------
   //   GETTERS
   // ------------------------------------------------------------------------------------------------------------


   public boolean isUseReplQueue() {
      return useReplQueue;
   }

   public int getReplQueueMaxElements() {
      return replQueueMaxElements;
   }

   public long getReplQueueInterval() {
      return replQueueInterval;
   }

   public boolean isExposeManagementStatistics() {
      return exposeManagementStatistics;
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

   public long getStateRetrievalTimeout() {
      return stateRetrievalTimeout;
   }

   public boolean isUseLazyDeserialization() {
      return useLazyDeserialization;
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

      if (exposeManagementStatistics != that.exposeManagementStatistics) return false;
      if (fetchInMemoryState != that.fetchInMemoryState) return false;
      if (lockAcquisitionTimeout != that.lockAcquisitionTimeout) return false;
      if (replQueueInterval != that.replQueueInterval) return false;
      if (replQueueMaxElements != that.replQueueMaxElements) return false;
      if (stateRetrievalTimeout != that.stateRetrievalTimeout) return false;
      if (syncCommitPhase != that.syncCommitPhase) return false;
      if (syncReplTimeout != that.syncReplTimeout) return false;
      if (syncRollbackPhase != that.syncRollbackPhase) return false;
      if (useLazyDeserialization != that.useLazyDeserialization) return false;
      if (useReplQueue != that.useReplQueue) return false;
      if (cacheLoaderManagerConfig != null ? !cacheLoaderManagerConfig.equals(that.cacheLoaderManagerConfig) : that.cacheLoaderManagerConfig != null)
         return false;
      if (cacheMode != that.cacheMode) return false;
      if (evictionConfig != null ? !evictionConfig.equals(that.evictionConfig) : that.evictionConfig != null)
         return false;
      if (isolationLevel != that.isolationLevel) return false;
      if (transactionManagerLookupClass != null ? !transactionManagerLookupClass.equals(that.transactionManagerLookupClass) : that.transactionManagerLookupClass != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = 0;
      result = 31 * result + (useReplQueue ? 1 : 0);
      result = 31 * result + replQueueMaxElements;
      result = 31 * result + (int) (replQueueInterval ^ (replQueueInterval >>> 32));
      result = 31 * result + (exposeManagementStatistics ? 1 : 0);
      result = 31 * result + (fetchInMemoryState ? 1 : 0);
      result = 31 * result + (int) (lockAcquisitionTimeout ^ (lockAcquisitionTimeout >>> 32));
      result = 31 * result + (int) (syncReplTimeout ^ (syncReplTimeout >>> 32));
      result = 31 * result + (cacheMode != null ? cacheMode.hashCode() : 0);
      result = 31 * result + (int) (stateRetrievalTimeout ^ (stateRetrievalTimeout >>> 32));
      result = 31 * result + (isolationLevel != null ? isolationLevel.hashCode() : 0);
      result = 31 * result + (evictionConfig != null ? evictionConfig.hashCode() : 0);
      result = 31 * result + (transactionManagerLookupClass != null ? transactionManagerLookupClass.hashCode() : 0);
      result = 31 * result + (cacheLoaderManagerConfig != null ? cacheLoaderManagerConfig.hashCode() : 0);
      result = 31 * result + (syncCommitPhase ? 1 : 0);
      result = 31 * result + (syncRollbackPhase ? 1 : 0);
      result = 31 * result + (useLazyDeserialization ? 1 : 0);

      return result;
   }

   @Override
   public Configuration clone() {
      try {
         Configuration c = (Configuration) super.clone();
         if (evictionConfig != null) {
            c.setEvictionConfig(evictionConfig.clone());
         }
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
    * Returns the {@link org.horizon.config.CustomInterceptorConfig}, if any, associated with this configuration object.
    * The custom interceptors will be added to the cache at startup in the sequence defined by this list.
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

   public boolean isUsingEviction() {
      return getEvictionConfig() != null;
   }

   public void applyOverrides(Configuration overrides) {
      // loop through all overridden elements in the incoming configuration and apply
      for (String overriddenField : overrides.overriddenConfigurationElements) {
         ReflectionUtil.setValue(this, overriddenField, ReflectionUtil.getValue(overrides, overriddenField));
      }
   }
}
