package org.infinispan.spring;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.core.io.Resource;

/**
 * <p>
 * An abstract base class for factories creating cache managers that are backed by an
 * EmbeddedCacheManager.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 */
public class AbstractEmbeddedCacheManagerFactory {

   protected static final Log logger = LogFactory.getLog(AbstractEmbeddedCacheManagerFactory.class);

   private Resource configurationFileLocation;

   protected final GlobalConfigurationOverrides globalConfigurationOverrides = new GlobalConfigurationOverrides();

   protected final ConfigurationOverrides configurationOverrides = new ConfigurationOverrides();

   // ------------------------------------------------------------------------
   // Create fully configured EmbeddedCacheManager instance
   // ------------------------------------------------------------------------

   protected EmbeddedCacheManager createBackingEmbeddedCacheManager() throws IOException {
      EmbeddedCacheManager cm;
      if (configurationFileLocation != null) {
         return createCacheManager(configurationFileLocation.getInputStream());
      } else {
         final GlobalConfigurationBuilder globalCfgBuilder = new GlobalConfigurationBuilder();
         final ConfigurationBuilder cacheCfgBuilder = new ConfigurationBuilder();
         this.globalConfigurationOverrides.applyOverridesTo(globalCfgBuilder);
         this.configurationOverrides.applyOverridesTo(cacheCfgBuilder);
         cm = createCacheManager(globalCfgBuilder, cacheCfgBuilder);
         return cm;
      }
   }

   protected EmbeddedCacheManager createCacheManager(InputStream is) throws IOException {
      return new DefaultCacheManager(is);
   }

   protected EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder) {
      return new DefaultCacheManager( globalBuilder.build(), builder.build() );
   }

   // ------------------------------------------------------------------------
   // Setter for location of configuration file
   // ------------------------------------------------------------------------

   /**
    * <p>
    * Sets the {@link org.springframework.core.io.Resource <code>location</code>} of the
    * configuration file which will be used to configure the
    * {@link org.infinispan.manager.EmbeddedCacheManager <code>EmbeddedCacheManager</code>} the
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager
    * <code>SpringEmbeddedCacheManager</code>} created by this <code>FactoryBean</code> delegates
    * to. If no location is supplied, <tt>Infinispan</tt>'s default configuration will be used.
    * </p>
    * <p>
    * Note that configuration settings defined via using explicit setters exposed by this
    * <code>FactoryBean</code> take precedence over those defined in the configuration file pointed
    * to by <code>configurationFileLocation</code>.
    * </p>
    * 
    * @param configurationFileLocation
    *           The {@link org.springframework.core.io.Resource <code>location</code>} of the
    *           configuration file which will be used to configure the
    *           {@link org.infinispan.manager.EmbeddedCacheManager
    *           <code>EmbeddedCacheManager</code>} the
    *           {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager
    *           <code>SpringEmbeddedCacheManager</code>} created by this <code>FactoryBean</code>
    *           delegates to
    */
   public void setConfigurationFileLocation(final Resource configurationFileLocation) {
      this.configurationFileLocation = configurationFileLocation;
   }

   // ------------------------------------------------------------------------
   // Setters for GlobalConfiguration properties
   // ------------------------------------------------------------------------

   /**
    * @param exposeGlobalJmxStatistics
    * @see org.infinispan.configuration.global.GlobalJmxStatisticsConfigurationBuilder#enabled(boolean)
    */
   public void setExposeGlobalJmxStatistics(final boolean exposeGlobalJmxStatistics) {
      this.globalConfigurationOverrides.setExposeGlobalJmxStatistics(exposeGlobalJmxStatistics);
   }

   /**
    * @param jmxObjectName
    * @see org.infinispan.configuration.global.GlobalJmxStatisticsConfigurationBuilder#jmxDomain(String)
    */
   public void setJmxDomain(final String jmxObjectName) {
      this.globalConfigurationOverrides.setJmxDomain(jmxObjectName);
   }

   /**
    * @param properties
    * @see org.infinispan.configuration.global.GlobalJmxStatisticsConfigurationBuilder#withProperties(java.util.Properties)
    */
   public void setMBeanServerProperties(final Properties properties) {
      this.globalConfigurationOverrides.setmBeanServerProperties(properties);
   }

   /**
    * @param mBeanServerLookupClass
    * @see org.infinispan.configuration.global.GlobalJmxStatisticsConfigurationBuilder#mBeanServerLookup(org.infinispan.jmx.MBeanServerLookup)
    */
   public void setMBeanServerLookupClass(final String mBeanServerLookupClass) {
      this.globalConfigurationOverrides.setmBeanServerLookupClass(mBeanServerLookupClass);
   }

   /**
    * @param mBeanServerLookup
    * @see org.infinispan.configuration.global.GlobalJmxStatisticsConfigurationBuilder#mBeanServerLookup(org.infinispan.jmx.MBeanServerLookup)
    */
   public void setMBeanServerLookup(final MBeanServerLookup mBeanServerLookup) {
      this.globalConfigurationOverrides.setmBeanServerLookup(mBeanServerLookup);
   }

   /**
    * @param allowDuplicateDomains
    * @see org.infinispan.configuration.global.GlobalJmxStatisticsConfigurationBuilder#allowDuplicateDomains(Boolean)
    */
   public void setAllowDuplicateDomains(final boolean allowDuplicateDomains) {
      this.globalConfigurationOverrides.setAllowDuplicateDomains(allowDuplicateDomains);
   }

   /**
    * @param cacheManagerName
    * @see org.infinispan.configuration.global.GlobalJmxStatisticsConfigurationBuilder#cacheManagerName(String)
    */
   public void setCacheManagerName(final String cacheManagerName) {
      this.globalConfigurationOverrides.setCacheManagerName(cacheManagerName);
   }

   /**
    * @param strictPeerToPeer
    * @see org.infinispan.configuration.global.TransportConfigurationBuilder#strictPeerToPeer(Boolean)
    */
   public void setStrictPeerToPeer(final boolean strictPeerToPeer) {
      this.globalConfigurationOverrides.setStrictPeerToPeer(strictPeerToPeer);
   }

   /**
    * @param asyncListenerExecutorFactoryClass
    * @see org.infinispan.configuration.global.ExecutorFactoryConfigurationBuilder#factory(org.infinispan.executors.ExecutorFactory)
    */
   public void setAsyncListenerExecutorFactoryClass(final String asyncListenerExecutorFactoryClass) {
      this.globalConfigurationOverrides.setAsyncListenerExecutorFactoryClass(asyncListenerExecutorFactoryClass);
   }

   /**
    * @param asyncTransportExecutorFactoryClass
    * @see org.infinispan.configuration.global.ExecutorFactoryConfigurationBuilder#factory(org.infinispan.executors.ExecutorFactory)
    */
   public void setAsyncTransportExecutorFactoryClass(final String asyncTransportExecutorFactoryClass) {
      this.globalConfigurationOverrides.setAsyncTransportExecutorFactoryClass(asyncTransportExecutorFactoryClass);
   }

   /**
    * @param remoteCommandsExecutorFactoryClass
    * @see org.infinispan.configuration.global.ExecutorFactoryConfigurationBuilder#factory(org.infinispan.executors.ExecutorFactory)
    */
   public void setRemoteCommandsExecutorFactoryClass(final String remoteCommandsExecutorFactoryClass) {
      this.globalConfigurationOverrides.setRemoteCommandsExecutorFactoryClass(remoteCommandsExecutorFactoryClass);
   }

   /**
    * @param evictionScheduledExecutorFactoryClass
    * @see org.infinispan.configuration.global.ExecutorFactoryConfigurationBuilder#factory(org.infinispan.executors.ExecutorFactory)
    */
   public void setEvictionScheduledExecutorFactoryClass(
            final String evictionScheduledExecutorFactoryClass) {
      this.globalConfigurationOverrides.setEvictionScheduledExecutorFactoryClass(evictionScheduledExecutorFactoryClass);
   }

   /**
    * @param replicationQueueScheduledExecutorFactoryClass
    * @see org.infinispan.configuration.global.ExecutorFactoryConfigurationBuilder#factory(org.infinispan.executors.ExecutorFactory)
    */
   public void setReplicationQueueScheduledExecutorFactoryClass(
            final String replicationQueueScheduledExecutorFactoryClass) {
      this.globalConfigurationOverrides.setReplicationQueueScheduledExecutorFactoryClass(replicationQueueScheduledExecutorFactoryClass);
   }

   /**
    * @param marshallerClass
    * @see org.infinispan.configuration.global.SerializationConfigurationBuilder#marshaller(org.infinispan.marshall.Marshaller)
    */
   public void setMarshallerClass(final String marshallerClass) {
      this.globalConfigurationOverrides.setMarshallerClass(marshallerClass);
   }

   /**
    * @param nodeName
    * @see org.infinispan.configuration.global.TransportConfigurationBuilder#nodeName(String)
    */
   public void setTransportNodeName(final String nodeName) {
      this.globalConfigurationOverrides.setTransportNodeName(nodeName);
   }

   /**
    * @param transportClass
    * @see org.infinispan.configuration.global.TransportConfigurationBuilder#transport(org.infinispan.remoting.transport.Transport)
    */
   public void setTransportClass(final String transportClass) {
      this.globalConfigurationOverrides.setTransportClass(transportClass);
   }

   /**
    * @param transportProperties
    * @see org.infinispan.configuration.global.TransportConfigurationBuilder#withProperties(java.util.Properties)
    */
   public void setTransportProperties(final Properties transportProperties) {
      this.globalConfigurationOverrides.setTransportProperties(transportProperties);
   }

   /**
    * @param clusterName
    * @see org.infinispan.configuration.global.TransportConfigurationBuilder#clusterName(String)
    */
   public void setClusterName(final String clusterName) {
      this.globalConfigurationOverrides.setClusterName(clusterName);
   }

   /**
    * @param machineId
    * @see org.infinispan.configuration.global.TransportConfigurationBuilder#machineId(String)
    */
   public void setMachineId(final String machineId) {
      this.globalConfigurationOverrides.setMachineId(machineId);
   }

   /**
    * @param rackId
    * @see org.infinispan.configuration.global.TransportConfigurationBuilder#rackId(String)
    */
   public void setRackId(final String rackId) {
      this.globalConfigurationOverrides.setRackId(rackId);
   }

   /**
    * @param siteId
    * @see org.infinispan.configuration.global.TransportConfigurationBuilder#siteId(String)
    */
   public void setSiteId(final String siteId) {
      this.globalConfigurationOverrides.setSiteId(siteId);
   }

   /**
    * @param shutdownHookBehavior
    * @see org.infinispan.configuration.global.ShutdownConfigurationBuilder#hookBehavior(org.infinispan.configuration.global.ShutdownHookBehavior)
    */
   public void setShutdownHookBehavior(final String shutdownHookBehavior) {
      this.globalConfigurationOverrides.setShutdownHookBehavior(shutdownHookBehavior);
   }

   /**
    * @param asyncListenerExecutorProperties
    * @see org.infinispan.configuration.global.ExecutorFactoryConfigurationBuilder#withProperties(java.util.Properties)
    */
   public void setAsyncListenerExecutorProperties(final Properties asyncListenerExecutorProperties) {
      this.globalConfigurationOverrides.setAsyncListenerExecutorProperties(asyncListenerExecutorProperties);
   }

   /**
    * @param asyncTransportExecutorProperties
    * @see org.infinispan.configuration.global.ExecutorFactoryConfigurationBuilder#withProperties(java.util.Properties)
    */
   public void setAsyncTransportExecutorProperties(final Properties asyncTransportExecutorProperties) {
      this.globalConfigurationOverrides.setAsyncTransportExecutorProperties(asyncTransportExecutorProperties);
   }

   /**
    * @param remoteCommandsExecutorProperties
    * @see org.infinispan.configuration.global.ExecutorFactoryConfigurationBuilder#withProperties(java.util.Properties)
    */
   public void setRemoteCommandsExecutorProperties(final Properties remoteCommandsExecutorProperties) {
      this.globalConfigurationOverrides.setRemoteCommandsExecutorProperties(remoteCommandsExecutorProperties);
   }

   /**
    * @param evictionScheduledExecutorProperties
    * @see org.infinispan.configuration.global.ExecutorFactoryConfigurationBuilder#withProperties(java.util.Properties)
    */
   public void setEvictionScheduledExecutorProperties(
            final Properties evictionScheduledExecutorProperties) {
      this.globalConfigurationOverrides.setEvictionScheduledExecutorProperties(evictionScheduledExecutorProperties);
   }

   /**
    * @param replicationQueueScheduledExecutorProperties
    * @see org.infinispan.configuration.global.ExecutorFactoryConfigurationBuilder#withProperties(java.util.Properties)
    */
   public void setReplicationQueueScheduledExecutorProperties(
            final Properties replicationQueueScheduledExecutorProperties) {
      this.globalConfigurationOverrides.setReplicationQueueScheduledExecutorProperties(replicationQueueScheduledExecutorProperties);
   }

   /**
    * @param marshallVersion
    * @see org.infinispan.configuration.global.SerializationConfigurationBuilder#version(short)
    */
   public void setMarshallVersion(final short marshallVersion) {
      this.globalConfigurationOverrides.setMarshallVersion(marshallVersion);
   }

   /**
    * @param distributedSyncTimeout
    * @see org.infinispan.configuration.global.TransportConfigurationBuilder#distributedSyncTimeout(long)
    */
   public void setDistributedSyncTimeout(final long distributedSyncTimeout) {
      this.globalConfigurationOverrides.setDistributedSyncTimeout(distributedSyncTimeout);
   }

   // ------------------------------------------------------------------------
   // Setters for Configuration
   // ------------------------------------------------------------------------

   /**
    * @param eagerDeadlockSpinDuration
    * @see org.infinispan.spring.ConfigurationOverrides#setDeadlockDetectionSpinDuration(java.lang.Long)
    */
   public void setDeadlockDetectionSpinDuration(final Long eagerDeadlockSpinDuration) {
      this.configurationOverrides.setDeadlockDetectionSpinDuration(eagerDeadlockSpinDuration);
   }

   /**
    * @param useEagerDeadlockDetection
    * @see org.infinispan.spring.ConfigurationOverrides#setEnableDeadlockDetection(java.lang.Boolean)
    */
   public void setEnableDeadlockDetection(final Boolean useEagerDeadlockDetection) {
      this.configurationOverrides.setEnableDeadlockDetection(useEagerDeadlockDetection);
   }

   /**
    * @param useLockStriping
    * @see org.infinispan.spring.ConfigurationOverrides#setUseLockStriping(java.lang.Boolean)
    */
   public void setUseLockStriping(final Boolean useLockStriping) {
      this.configurationOverrides.setUseLockStriping(useLockStriping);
   }

   /**
    * @param unsafeUnreliableReturnValues
    * @see org.infinispan.spring.ConfigurationOverrides#setUnsafeUnreliableReturnValues(java.lang.Boolean)
    */
   public void setUnsafeUnreliableReturnValues(final Boolean unsafeUnreliableReturnValues) {
      this.configurationOverrides.setUnsafeUnreliableReturnValues(unsafeUnreliableReturnValues);
   }

   /**
    * @param rehashRpcTimeout
    * @see org.infinispan.spring.ConfigurationOverrides#setRehashRpcTimeout(java.lang.Long)
    */
   public void setRehashRpcTimeout(final Long rehashRpcTimeout) {
      this.configurationOverrides.setRehashRpcTimeout(rehashRpcTimeout);
   }

   /**
    * @param writeSkewCheck
    * @see org.infinispan.spring.ConfigurationOverrides#setWriteSkewCheck(java.lang.Boolean)
    */
   public void setWriteSkewCheck(final Boolean writeSkewCheck) {
      this.configurationOverrides.setWriteSkewCheck(writeSkewCheck);
   }

   /**
    * @param concurrencyLevel
    * @see org.infinispan.spring.ConfigurationOverrides#setConcurrencyLevel(java.lang.Integer)
    */
   public void setConcurrencyLevel(final Integer concurrencyLevel) {
      this.configurationOverrides.setConcurrencyLevel(concurrencyLevel);
   }

   /**
    * @param replQueueMaxElements
    * @see org.infinispan.spring.ConfigurationOverrides#setReplQueueMaxElements(java.lang.Integer)
    */
   public void setReplQueueMaxElements(final Integer replQueueMaxElements) {
      this.configurationOverrides.setReplQueueMaxElements(replQueueMaxElements);
   }

   /**
    * @param replQueueInterval
    * @see org.infinispan.spring.ConfigurationOverrides#setReplQueueInterval(java.lang.Long)
    */
   public void setReplQueueInterval(final Long replQueueInterval) {
      this.configurationOverrides.setReplQueueInterval(replQueueInterval);
   }

   /**
    * @param replQueueClass
    * @see org.infinispan.spring.ConfigurationOverrides#setReplQueueClass(java.lang.String)
    */
   public void setReplQueueClass(final String replQueueClass) {
      this.configurationOverrides.setReplQueueClass(replQueueClass);
   }

   /**
    * @param exposeJmxStatistics
    * @see org.infinispan.spring.ConfigurationOverrides#setExposeJmxStatistics(java.lang.Boolean)
    */
   public void setExposeJmxStatistics(final Boolean exposeJmxStatistics) {
      this.configurationOverrides.setExposeJmxStatistics(exposeJmxStatistics);
   }

   /**
    * @param invocationBatchingEnabled
    * @see org.infinispan.spring.ConfigurationOverrides#setInvocationBatchingEnabled(java.lang.Boolean)
    */
   public void setInvocationBatchingEnabled(final Boolean invocationBatchingEnabled) {
      this.configurationOverrides.setInvocationBatchingEnabled(invocationBatchingEnabled);
   }

   /**
    * @param fetchInMemoryState
    * @see org.infinispan.spring.ConfigurationOverrides#setFetchInMemoryState(java.lang.Boolean)
    */
   public void setFetchInMemoryState(final Boolean fetchInMemoryState) {
      this.configurationOverrides.setFetchInMemoryState(fetchInMemoryState);
   }

   /**
    * @param alwaysProvideInMemoryState
    * @see org.infinispan.spring.ConfigurationOverrides#setAlwaysProvideInMemoryState(java.lang.Boolean)
    */
   public void setAlwaysProvideInMemoryState(final Boolean alwaysProvideInMemoryState) {
      this.configurationOverrides.setAlwaysProvideInMemoryState(alwaysProvideInMemoryState);
   }

   /**
    * @param lockAcquisitionTimeout
    * @see org.infinispan.spring.ConfigurationOverrides#setLockAcquisitionTimeout(java.lang.Long)
    */
   public void setLockAcquisitionTimeout(final Long lockAcquisitionTimeout) {
      this.configurationOverrides.setLockAcquisitionTimeout(lockAcquisitionTimeout);
   }

   /**
    * @param syncReplTimeout
    * @see org.infinispan.spring.ConfigurationOverrides#setSyncReplTimeout(java.lang.Long)
    */
   public void setSyncReplTimeout(final Long syncReplTimeout) {
      this.configurationOverrides.setSyncReplTimeout(syncReplTimeout);
   }

   /**
    * @param cacheModeString
    * @see org.infinispan.spring.ConfigurationOverrides#setCacheModeString(java.lang.String)
    */
   public void setCacheModeString(final String cacheModeString) {
      this.configurationOverrides.setCacheModeString(cacheModeString);
   }

   /**
    * @param expirationWakeUpInterval
    * @see org.infinispan.spring.ConfigurationOverrides#setExpirationWakeUpInterval(Long) (java.lang.Long)
    */
   public void setExpirationWakeUpInterval(final Long expirationWakeUpInterval) {
      this.configurationOverrides.setExpirationWakeUpInterval(expirationWakeUpInterval);
   }

   /**
    * @param evictionStrategy
    * @see org.infinispan.spring.ConfigurationOverrides#setEvictionStrategy(org.infinispan.eviction.EvictionStrategy)
    */
   public void setEvictionStrategy(final EvictionStrategy evictionStrategy) {
      this.configurationOverrides.setEvictionStrategy(evictionStrategy);
   }

   /**
    * @param evictionStrategyClass
    * @see org.infinispan.spring.ConfigurationOverrides#setEvictionStrategyClass(java.lang.String)
    */
   public void setEvictionStrategyClass(final String evictionStrategyClass) {
      this.configurationOverrides.setEvictionStrategyClass(evictionStrategyClass);
   }

   /**
    * @param evictionThreadPolicy
    * @see org.infinispan.spring.ConfigurationOverrides#setEvictionThreadPolicy(org.infinispan.eviction.EvictionThreadPolicy)
    */
   public void setEvictionThreadPolicy(final EvictionThreadPolicy evictionThreadPolicy) {
      this.configurationOverrides.setEvictionThreadPolicy(evictionThreadPolicy);
   }

   /**
    * @param evictionThreadPolicyClass
    * @see org.infinispan.spring.ConfigurationOverrides#setEvictionThreadPolicyClass(java.lang.String)
    */
   public void setEvictionThreadPolicyClass(final String evictionThreadPolicyClass) {
      this.configurationOverrides.setEvictionThreadPolicyClass(evictionThreadPolicyClass);
   }

   /**
    * @param evictionMaxEntries
    * @see org.infinispan.spring.ConfigurationOverrides#setEvictionMaxEntries(java.lang.Integer)
    */
   public void setEvictionMaxEntries(final Integer evictionMaxEntries) {
      this.configurationOverrides.setEvictionMaxEntries(evictionMaxEntries);
   }

   /**
    * @param expirationLifespan
    * @see org.infinispan.spring.ConfigurationOverrides#setExpirationLifespan(java.lang.Long)
    */
   public void setExpirationLifespan(final Long expirationLifespan) {
      this.configurationOverrides.setExpirationLifespan(expirationLifespan);
   }

   /**
    * @param expirationMaxIdle
    * @see org.infinispan.spring.ConfigurationOverrides#setExpirationMaxIdle(java.lang.Long)
    */
   public void setExpirationMaxIdle(final Long expirationMaxIdle) {
      this.configurationOverrides.setExpirationMaxIdle(expirationMaxIdle);
   }

   /**
    * @param transactionManagerLookupClass
    * @see org.infinispan.spring.ConfigurationOverrides#setTransactionManagerLookupClass(java.lang.String)
    */
   public void setTransactionManagerLookupClass(final String transactionManagerLookupClass) {
      this.configurationOverrides.setTransactionManagerLookupClass(transactionManagerLookupClass);
   }

   /**
    * @param transactionManagerLookup
    * @see org.infinispan.spring.ConfigurationOverrides#setTransactionManagerLookup(org.infinispan.transaction.lookup.TransactionManagerLookup)
    */
   public void setTransactionManagerLookup(final TransactionManagerLookup transactionManagerLookup) {
      this.configurationOverrides.setTransactionManagerLookup(transactionManagerLookup);
   }

   /**
    * @param syncCommitPhase
    * @see org.infinispan.spring.ConfigurationOverrides#setSyncCommitPhase(java.lang.Boolean)
    */
   public void setSyncCommitPhase(final Boolean syncCommitPhase) {
      this.configurationOverrides.setSyncCommitPhase(syncCommitPhase);
   }

   /**
    * @param syncRollbackPhase
    * @see org.infinispan.spring.ConfigurationOverrides#setSyncRollbackPhase(java.lang.Boolean)
    */
   public void setSyncRollbackPhase(final Boolean syncRollbackPhase) {
      this.configurationOverrides.setSyncRollbackPhase(syncRollbackPhase);
   }

   /**
    * @param useEagerLocking
    * @see org.infinispan.spring.ConfigurationOverrides#setUseEagerLocking(java.lang.Boolean)
    */
   public void setUseEagerLocking(final Boolean useEagerLocking) {
      this.configurationOverrides.setUseEagerLocking(useEagerLocking);
   }

   /**
    * @param useReplQueue
    * @see org.infinispan.spring.ConfigurationOverrides#setUseReplQueue(java.lang.Boolean)
    */
   public void setUseReplQueue(final Boolean useReplQueue) {
      this.configurationOverrides.setUseReplQueue(useReplQueue);
   }

   /**
    * @param isolationLevel
    * @see org.infinispan.spring.ConfigurationOverrides#setIsolationLevel(org.infinispan.util.concurrent.IsolationLevel)
    */
   public void setIsolationLevel(final IsolationLevel isolationLevel) {
      this.configurationOverrides.setIsolationLevel(isolationLevel);
   }

   /**
    * @param stateRetrievalTimeout
    * @see org.infinispan.spring.ConfigurationOverrides#setStateRetrievalTimeout(java.lang.Long)
    */
   public void setStateRetrievalTimeout(final Long stateRetrievalTimeout) {
      this.configurationOverrides.setStateRetrievalTimeout(stateRetrievalTimeout);
   }

   /**
    * @param stateRetrievalMaxNonProgressingLogWrites
    * @see org.infinispan.spring.ConfigurationOverrides#setStateRetrievalMaxNonProgressingLogWrites(java.lang.Integer)
    */
   public void setStateRetrievalMaxNonProgressingLogWrites(
            final Integer stateRetrievalMaxNonProgressingLogWrites) {
      this.configurationOverrides
               .setStateRetrievalMaxNonProgressingLogWrites(stateRetrievalMaxNonProgressingLogWrites);
   }

   /**
    * @param stateRetrievalInitialRetryWaitTime
    * @see org.infinispan.spring.ConfigurationOverrides#setStateRetrievalInitialRetryWaitTime(java.lang.Long)
    */
   public void setStateRetrievalInitialRetryWaitTime(final Long stateRetrievalInitialRetryWaitTime) {
      this.configurationOverrides
               .setStateRetrievalInitialRetryWaitTime(stateRetrievalInitialRetryWaitTime);
   }

   /**
    * @param stateRetrievalChunkSize
    * @see org.infinispan.spring.ConfigurationOverrides#setStateRetrievalChunkSize(java.lang.Integer)
    */
   public void setStateRetrievalChunkSize(
         final Integer stateRetrievalChunkSize) {
      this.configurationOverrides
            .setStateRetrievalChunkSize(stateRetrievalChunkSize);
   }

   /**
    * @param isolationLevelClass
    * @see org.infinispan.spring.ConfigurationOverrides#setIsolationLevelClass(java.lang.String)
    */
   public void setIsolationLevelClass(final String isolationLevelClass) {
      this.configurationOverrides.setIsolationLevelClass(isolationLevelClass);
   }

   /**
    * @param useLazyDeserialization
    * @see org.infinispan.spring.ConfigurationOverrides#setUseLazyDeserialization(java.lang.Boolean)
    */
   public void setUseLazyDeserialization(final Boolean useLazyDeserialization) {
      this.configurationOverrides.setUseLazyDeserialization(useLazyDeserialization);
   }

   /**
    * @param l1CacheEnabled
    * @see org.infinispan.spring.ConfigurationOverrides#setL1CacheEnabled(java.lang.Boolean)
    */
   public void setL1CacheEnabled(final Boolean l1CacheEnabled) {
      this.configurationOverrides.setL1CacheEnabled(l1CacheEnabled);
   }

   /**
    * @param l1Lifespan
    * @see org.infinispan.spring.ConfigurationOverrides#setL1Lifespan(java.lang.Long)
    */
   public void setL1Lifespan(final Long l1Lifespan) {
      this.configurationOverrides.setL1Lifespan(l1Lifespan);
   }

   /**
    * @param l1OnRehash
    * @see org.infinispan.spring.ConfigurationOverrides#setL1OnRehash(java.lang.Boolean)
    */
   public void setL1OnRehash(final Boolean l1OnRehash) {
      this.configurationOverrides.setL1OnRehash(l1OnRehash);
   }

   /**
    * @param consistentHashClass
    * @see org.infinispan.spring.ConfigurationOverrides#setConsistentHashClass(java.lang.String)
    */
   public void setConsistentHashClass(final String consistentHashClass) {
      this.configurationOverrides.setConsistentHashClass(consistentHashClass);
   }

   /**
    * @param numOwners
    * @see org.infinispan.spring.ConfigurationOverrides#setNumOwners(java.lang.Integer)
    */
   public void setNumOwners(final Integer numOwners) {
      this.configurationOverrides.setNumOwners(numOwners);
   }

   /**
    * @param rehashEnabled
    * @see org.infinispan.spring.ConfigurationOverrides#setRehashEnabled(java.lang.Boolean)
    */
   public void setRehashEnabled(final Boolean rehashEnabled) {
      this.configurationOverrides.setRehashEnabled(rehashEnabled);
   }

   /**
    * @param useAsyncMarshalling
    * @see org.infinispan.spring.ConfigurationOverrides#setUseAsyncMarshalling(java.lang.Boolean)
    */
   public void setUseAsyncMarshalling(final Boolean useAsyncMarshalling) {
      this.configurationOverrides.setUseAsyncMarshalling(useAsyncMarshalling);
   }

   /**
    * @param indexingEnabled
    * @see org.infinispan.spring.ConfigurationOverrides#setIndexingEnabled(java.lang.Boolean)
    */
   public void setIndexingEnabled(final Boolean indexingEnabled) {
      this.configurationOverrides.setIndexingEnabled(indexingEnabled);
   }

   /**
    * @param indexLocalOnly
    * @see org.infinispan.spring.ConfigurationOverrides#setIndexLocalOnly(java.lang.Boolean)
    */
   public void setIndexLocalOnly(final Boolean indexLocalOnly) {
      this.configurationOverrides.setIndexLocalOnly(indexLocalOnly);
   }

   /**
    * @param customInterceptors
    * @see org.infinispan.spring.ConfigurationOverrides#setCustomInterceptors(java.util.List)
    */
   public void setCustomInterceptors(final List<? extends CommandInterceptor> customInterceptors) {
      this.configurationOverrides.setCustomInterceptors(customInterceptors);
   }
}
