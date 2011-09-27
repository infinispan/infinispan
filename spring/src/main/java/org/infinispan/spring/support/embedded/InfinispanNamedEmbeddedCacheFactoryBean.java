/**
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
 *   ~
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

package org.infinispan.spring.support.embedded;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.CustomInterceptorConfig;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.ConfigurationOverrides;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

/**
 * <p>
 * A {@link org.springframework.beans.factory.FactoryBean <code>FactoryBean</code>} for creating a
 * native {@link #setCacheName(String) named} Infinispan {@link org.infinispan.Cache
 * <code>org.infinispan.Cache</code>}, delegating to a
 * {@link #setInfinispanEmbeddedCacheManager(EmbeddedCacheManager) <code>configurable</code>}
 * {@link org.infinispan.manager.EmbeddedCacheManager
 * <code>org.infinispan.manager.EmbeddedCacheManager</code>}. If no cache name is explicitly set,
 * this <code>FactoryBean</code>'s {@link #setBeanName(String) <code>beanName</code>} will be used
 * instead.
 * </p>
 * <p>
 * Beyond merely creating named <code>Cache</code> instances, this <code>FactoryBean</code> offers
 * great flexibility in configuring those <code>Caches</code>. It has setters for all non-global
 * configuration settings, i.e. all settings that are specific to a single <code>Cache</code>. The
 * configuration settings thus defined override those settings obtained from the
 * <code>EmbeddedCacheManager</code>.
 * </p>
 * <p>
 * There are different configuration {@link #setConfigurationTemplateMode(String)
 * <code>modes</code>} that control with what <code>Configuration</code> to start before further
 * customizing it as described above:
 * <ul>
 * <li>
 * <code>NONE</code>: Configuration starts with a new <code>Configuration</code> instance.
 * Subsequently, its settings are overridden with those properties that have been explicitly set on
 * this <code>FactoryBean</code>. Note that this mode may only be used if no named configuration
 * having the same name as the <code>Cache</code> to be created already exists. It is therefore
 * illegal to use this mode to create a <code>Cache</code> named, say, &quot;cacheName&quot; if the
 * configuration file used to configure the <code>EmbeddedCacheManager</code> contains a
 * configuration section named &quot;cacheName&quot;.</li>
 * <li>
 * <code>DEFAULT</code>: Configuration starts with the <code>EmbeddedCacheManager</code>'s
 * <em>default</em> <code>Configuration</code> instance, i.e. the configuration settings defined in
 * its configuration file's default section. Subsequently, its settings are overridden with those
 * properties that have been explicitly set on this <code>FactoryBean</code>. Note that this mode
 * may only be used if no named configuration having the same name as the <code>Cache</code> to be
 * created already exists. It is therefore illegal to use this mode to create a <code>Cache</code>
 * named, say, &quot;cacheName&quot; if the configuration file used to configure the
 * <code>EmbeddedCacheManager</code> contains a configuration section named &quot;cacheName&quot;.</li>
 * <li>
 * <code>NAMED</code>: Configuration starts with the <code>EmbeddedCacheManager</code>'s
 * <code>Configuration</code> instance having the same name as the <code>Cache</code> to be created.
 * For a <code>Cache</code> named, say, &quot;cacheName&quot; this is the configuration section
 * named &quot;cacheName&quot; as defined in the <code>EmbeddedCacheManager</code>'s configuration
 * file. Subsequently, its settings are overridden with those properties that have been explicitly
 * set on this <code>FactoryBean</code>. Note that this mode is only useful if such a named
 * configuration section does indeed exist. Otherwise, it is equivalent to using
 * <code>DEFAULT</code>.</li>
 * </ul>
 * </p>
 * <p>
 * In addition to creating a named <code>Cache</code> this <code>FactoryBean</code> does also
 * control that <code>Cache</code>' {@link org.infinispan.lifecycle.Lifecycle lifecycle} by shutting
 * it down when the enclosing Spring application context is closed. It is therefore advisable to
 * <em>always</em> use this <code>FactoryBean</code> when creating a named <code>Cache</code>.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 * 
 */
public class InfinispanNamedEmbeddedCacheFactoryBean<K, V> implements FactoryBean<Cache<K, V>>,
         BeanNameAware, InitializingBean, DisposableBean {

   /**
    * <p>
    * Defines how to configure a new named cache produced by this <code>FactoryBean</code>:
    * <ul>
    * <li>
    * <code>NONE</code>: Configuration starts with a new <code>Configuration</code> instance.
    * Subsequently, its settings are overridden with those properties that have been explicitly set
    * on this <code>FactoryBean</code>. Note that this mode may only be used if no named
    * configuration having the same name as the <code>Cache</code> to be created already exists. It
    * is therefore illegal to use this mode to create a <code>Cache</code> named, say,
    * &quot;cacheName&quot; if the configuration file used to configure the
    * <code>EmbeddedCacheManager</code> contains a configuration section named
    * &quot;cacheName&quot;.</li>
    * <li>
    * <code>DEFAULT</code>: Configuration starts with the <code>EmbeddedCacheManager</code>'s
    * <em>default</em> <code>Configuration</code> instance, i.e. the configuration settings defined
    * in its configuration file's default section. Subsequently, its settings are overridden with
    * those properties that have been explicitly set on this <code>FactoryBean</code>. Note that
    * this mode may only be used if no named configuration having the same name as the
    * <code>Cache</code> to be created already exists. It is therefore illegal to use this mode to
    * create a <code>Cache</code> named, say, &quot;cacheName&quot; if the configuration file used
    * to configure the <code>EmbeddedCacheManager</code> contains a configuration section named
    * &quot;cacheName&quot;.</li>
    * <li>
    * <code>NAMED</code>: Configuration starts with the <code>EmbeddedCacheManager</code>'s
    * <code>Configuration</code> instance having the same name as the <code>Cache</code> to be
    * created. For a <code>Cache</code> named, say, &quot;cacheName&quot; this is the configuration
    * section named &quot;cacheName&quot; as defined in the <code>EmbeddedCacheManager</code>'s
    * configuration file. Subsequently, its settings are overridden with those properties that have
    * been explicitly set on this <code>FactoryBean</code>. Note that this mode is only useful if
    * such a named configuration section does indeed exist. Otherwise, it is equivalent to using
    * <code>DEFAULT</code>.</li>
    * </ul>
    * </p>
    * 
    * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
    * 
    */
   enum ConfigurationTemplateMode {

      NONE,

      DEFAULT,

      NAMED
   }

   private final Log logger = LogFactory.getLog(getClass());

   private EmbeddedCacheManager infinispanEmbeddedCacheManager;

   private ConfigurationTemplateMode configurationTemplateMode = ConfigurationTemplateMode.NAMED;

   private String cacheName;

   private String beanName;

   private final ConfigurationOverrides configurationOverrides = new ConfigurationOverrides();

   private Cache<K, V> infinispanCache;

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.InitializingBean
   // ------------------------------------------------------------------------

   /**
    * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
    */
   @Override
   public void afterPropertiesSet() throws Exception {
      if (this.infinispanEmbeddedCacheManager == null) {
         throw new IllegalStateException("No Infinispan EmbeddedCacheManager has been set");
      }
      this.logger.info("Initializing named Infinispan embedded cache ...");
      final String effectiveCacheName = obtainEffectiveCacheName();
      this.infinispanCache = configureAndCreateNamedCache(effectiveCacheName);
      this.logger.info("New Infinispan embedded cache [" + this.infinispanCache + "] initialized");
   }

   private Cache<K, V> configureAndCreateNamedCache(final String cacheName) {
      switch (this.configurationTemplateMode) {
         case NONE:
            this.logger
                     .debug("ConfigurationTemplateMode is NONE: starting with a fresh Configuration");
            if (this.infinispanEmbeddedCacheManager.getCacheNames().contains(cacheName)) {
               throw new IllegalStateException(
                        "Cannot use ConfigurationTemplateMode NONE: a cache named [" + cacheName
                                 + "] has already been defined.");
            }
            final Configuration newConfiguration = new Configuration();
            final Configuration customizedNewConfiguration = this.infinispanEmbeddedCacheManager
                     .defineConfiguration(cacheName, newConfiguration);
            this.configurationOverrides.applyOverridesTo(customizedNewConfiguration);
            break;
         case NAMED:
            this.logger
                     .debug("ConfigurationTemplateMode is NAMED: starting with a named Configuration ["
                              + cacheName + "]");
            final Configuration namedConfiguration = new Configuration();
            final Configuration customizedNamedConfiguration = this.infinispanEmbeddedCacheManager
                     .defineConfiguration(cacheName, cacheName, namedConfiguration);
            this.configurationOverrides.applyOverridesTo(customizedNamedConfiguration);
            break;
         case DEFAULT:
            this.logger
                     .debug("ConfigurationTemplateMode is DEFAULT: starting with default Configuration");
            if (this.infinispanEmbeddedCacheManager.getCacheNames().contains(cacheName)) {
               throw new IllegalStateException(
                        "Cannot use ConfigurationTemplateMode DEFAULT: a cache named [" + cacheName
                                 + "] has already been defined.");
            }
            final Configuration defaultConfiguration = this.infinispanEmbeddedCacheManager
                     .getDefaultConfiguration().clone();
            final Configuration customizedDefaultConfiguration = this.infinispanEmbeddedCacheManager
                     .defineConfiguration(cacheName, defaultConfiguration);
            this.configurationOverrides.applyOverridesTo(customizedDefaultConfiguration);
            break;
         default:
            throw new IllegalStateException("Unknown ConfigurationTemplateMode: "
                     + this.configurationTemplateMode);
      }

      return this.infinispanEmbeddedCacheManager.getCache(cacheName);
   }

   private String obtainEffectiveCacheName() {
      if (StringUtils.hasText(this.cacheName)) {
         if (this.logger.isDebugEnabled()) {
            this.logger.debug("Using custom cache name [" + this.cacheName + "]");
         }
         return this.cacheName;
      } else {
         if (this.logger.isDebugEnabled()) {
            this.logger.debug("Using bean name [" + this.beanName + "] as cache name");
         }
         return this.beanName;
      }
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.FactoryBean
   // ------------------------------------------------------------------------

   /**
    * @see org.springframework.beans.factory.FactoryBean#getObject()
    */
   @Override
   public Cache<K, V> getObject() throws Exception {
      return this.infinispanCache;
   }

   /**
    * @see org.springframework.beans.factory.FactoryBean#getObjectType()
    */
   @Override
   public Class<? extends Cache> getObjectType() {
      return this.infinispanCache != null ? this.infinispanCache.getClass() : Cache.class;
   }

   /**
    * Always returns <code>true</code>.
    * 
    * @return Always <code>true</code>
    * 
    * @see org.springframework.beans.factory.FactoryBean#isSingleton()
    */
   @Override
   public boolean isSingleton() {
      return true;
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.BeanNameAware
   // ------------------------------------------------------------------------

   /**
    * @see org.springframework.beans.factory.BeanNameAware#setBeanName(java.lang.String)
    */
   @Override
   public void setBeanName(final String name) {
      this.beanName = name;
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.DisposableBean
   // ------------------------------------------------------------------------

   /**
    * Shuts down the <code>org.infinispan.Cache</code> created by this <code>FactoryBean</code>.
    * 
    * @see org.springframework.beans.factory.DisposableBean#destroy()
    * @see org.infinispan.Cache#stop()
    */
   @Override
   public void destroy() throws Exception {
      // Probably being paranoid here ...
      if (this.infinispanCache != null) {
         this.infinispanCache.stop();
      }
   }

   // ------------------------------------------------------------------------
   // Properties
   // ------------------------------------------------------------------------

   /**
    * <p>
    * Sets the {@link org.infinispan.Cache#getName() name} of the {@link org.infinispan.Cache
    * <code>org.infinispan.Cache</code>} to be created. If no explicit <code>cacheName</code> is
    * set, this <code>FactoryBean</code> will use its {@link #setBeanName(String)
    * <code>beanName</code>} as the <code>cacheName</code>.
    * </p>
    * 
    * @param cacheName
    *           The {@link org.infinispan.Cache#getName() name} of the {@link org.infinispan.Cache
    *           <code>org.infinispan.Cache</code>} to be created
    */
   public void setCacheName(final String cacheName) {
      this.cacheName = cacheName;
   }

   /**
    * <p>
    * Sets the {@link org.infinispan.manager.EmbeddedCacheManager
    * <code>org.infinispan.manager.EmbeddedCacheManager</code>} to be used for creating our
    * {@link org.infinispan.Cache <code>Cache</code>} instance. Note that this is a
    * <strong>mandatory</strong> property.
    * </p>
    * 
    * @param infinispanEmbeddedCacheManager
    *           The {@link org.infinispan.manager.EmbeddedCacheManager
    *           <code>org.infinispan.manager.EmbeddedCacheManager</code>} to be used for creating
    *           our {@link org.infinispan.Cache <code>Cache</code>} instance
    */
   public void setInfinispanEmbeddedCacheManager(
            final EmbeddedCacheManager infinispanEmbeddedCacheManager) {
      this.infinispanEmbeddedCacheManager = infinispanEmbeddedCacheManager;
   }

   /**
    * @param configurationTemplateMode
    * @throws IllegalArgumentException
    */
   public void setConfigurationTemplateMode(final String configurationTemplateMode)
            throws IllegalArgumentException {
      this.configurationTemplateMode = ConfigurationTemplateMode.valueOf(configurationTemplateMode);
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
    * @see org.infinispan.spring.ConfigurationOverrides#setExpirationWakeUpInterval(Long)
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
    * @param cacheLoaderManagerConfig
    * @see org.infinispan.spring.ConfigurationOverrides#setCacheLoaderManagerConfig(org.infinispan.config.CacheLoaderManagerConfig)
    */
   public void setCacheLoaderManagerConfig(final CacheLoaderManagerConfig cacheLoaderManagerConfig) {
      this.configurationOverrides.setCacheLoaderManagerConfig(cacheLoaderManagerConfig);
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
    * @param eagerLockSingleNode
    * @see org.infinispan.spring.ConfigurationOverrides#setEagerLockSingleNode(java.lang.Boolean)
    */
   public void setEagerLockSingleNode(final Boolean eagerLockSingleNode) {
      this.configurationOverrides.setEagerLockSingleNode(eagerLockSingleNode);
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
    * @param stateRetrievalLogFlushTimeout
    * @see org.infinispan.spring.ConfigurationOverrides#setStateRetrievalLogFlushTimeout(java.lang.Long)
    */
   public void setStateRetrievalLogFlushTimeout(final Long stateRetrievalLogFlushTimeout) {
      this.configurationOverrides.setStateRetrievalLogFlushTimeout(stateRetrievalLogFlushTimeout);
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
    * @param stateRetrievalRetryWaitTimeIncreaseFactor
    * @see org.infinispan.spring.ConfigurationOverrides#setStateRetrievalRetryWaitTimeIncreaseFactor(java.lang.Integer)
    */
   public void setStateRetrievalRetryWaitTimeIncreaseFactor(
            final Integer stateRetrievalRetryWaitTimeIncreaseFactor) {
      this.configurationOverrides
               .setStateRetrievalRetryWaitTimeIncreaseFactor(stateRetrievalRetryWaitTimeIncreaseFactor);
   }

   /**
    * @param stateRetrievalNumRetries
    * @see org.infinispan.spring.ConfigurationOverrides#setStateRetrievalNumRetries(java.lang.Integer)
    */
   public void setStateRetrievalNumRetries(final Integer stateRetrievalNumRetries) {
      this.configurationOverrides.setStateRetrievalNumRetries(stateRetrievalNumRetries);
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
    * @param rehashWaitTime
    * @see org.infinispan.spring.ConfigurationOverrides#setRehashWaitTime(java.lang.Long)
    */
   public void setRehashWaitTime(final Long rehashWaitTime) {
      this.configurationOverrides.setRehashWaitTime(rehashWaitTime);
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
   public void setCustomInterceptors(final List<CustomInterceptorConfig> customInterceptors) {
      this.configurationOverrides.setCustomInterceptors(customInterceptors);
   }
}
