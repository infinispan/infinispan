/*
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

import org.infinispan.config.Configuration.EvictionType;
import org.infinispan.config.GlobalConfiguration.TransportType;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;

/**
 * ConfigurationValidatingVisitor checks semantic validity of InfinispanConfiguration instance.
 *
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class ConfigurationValidatingVisitor extends AbstractConfigurationBeanVisitor {

    private static final Log log = LogFactory.getLog(ConfigurationValidatingVisitor.class);

    private TransportType tt = null;
    private boolean evictionEnabled = false;
    private Configuration cfg;

    @Override
    public void visitSingletonStoreConfig(SingletonStoreConfig ssc) {
        if (tt == null && ssc.isSingletonStoreEnabled()) throw new ConfigurationException("Singleton store configured without transport being configured");
    }

    @Override
    public void visitTransportType(TransportType tt) {
        this.tt = tt;
    }

    @Override
    public void visitConfiguration(Configuration cfg) {
        this.cfg= cfg;
    }

    @Override
    public void visitClusteringType(Configuration.ClusteringType clusteringType) {
        Configuration.CacheMode mode = clusteringType.mode;
        Configuration.AsyncType async = clusteringType.async;
        Configuration.StateRetrievalType state = clusteringType.stateRetrieval;
        // certain combinations are illegal, such as state transfer + DIST
        if (mode.isDistributed() && state.fetchInMemoryState)
            throw new ConfigurationException("Cache cannot use DISTRIBUTION mode and have fetchInMemoryState set to true.  Perhaps you meant to enable rehashing?");

        if (mode.isDistributed() && async.useReplQueue)
            throw new ConfigurationException("Use of the replication queue is invalid when using DISTRIBUTED mode.");

        if (mode.isSynchronous() && async.useReplQueue)
            throw new ConfigurationException("Use of the replication queue is only allowed with an ASYNCHRONOUS cluster mode.");

        // If replicated and fetch state transfer was not explicitly
        // disabled, then force enabling of state transfer
        Set<String> overriden = clusteringType.stateRetrieval.overriddenConfigurationElements;
        if (mode.isReplicated() && !state.isFetchInMemoryState()
                && !overriden.contains("fetchInMemoryState")) {
            log.debug("Cache is replicated but state transfer was not defined, so force enabling it");
            state.fetchInMemoryState(true);
        }
    }

    @Override
    public void visitL1Type(Configuration.L1Type l1Type) {
        boolean l1Enabled = l1Type.enabled;
        boolean l1OnRehash = l1Type.onRehash;

        // If L1 is disabled, L1ForRehash should also be disabled
        if (!l1Enabled && l1OnRehash) {
            Set<String> overridden = l1Type.overriddenConfigurationElements;
            if (overridden.contains("onRehash")) {
                throw new ConfigurationException("Can only move entries to L1 on rehash when L1 is enabled");
            } else {
                log.debug("L1 is disabled and L1OnRehash was not defined, disabling it");
                l1Type.onRehash(false);
            }
        }
    }

    @Override
    public void visitCacheLoaderManagerConfig(CacheLoaderManagerConfig cacheLoaderManagerConfig) {
        if (!evictionEnabled && cacheLoaderManagerConfig.isPassivation())
            log.passivationWithoutEviction();

        boolean shared = cacheLoaderManagerConfig.isShared();
        if (!shared) {
            for (CacheLoaderConfig loaderConfig : cacheLoaderManagerConfig.getCacheLoaderConfigs()) {
                if (loaderConfig instanceof CacheStoreConfig) {
                    CacheStoreConfig storeConfig = (CacheStoreConfig)loaderConfig;
                    Boolean fetchPersistentState = storeConfig.isFetchPersistentState();
                    Boolean purgeOnStartup = storeConfig.isPurgeOnStartup();
                    if (!fetchPersistentState && !purgeOnStartup && cfg.getCacheMode().isClustered()) {
                        log.staleEntriesWithoutFetchPersistentStateOrPurgeOnStartup();
                    }
                }
            }
        }
    }

    @Override
    public void visitVersioningConfigurationBean(Configuration.VersioningConfigurationBean config) {
    }

    @Override
    public void visitEvictionType(EvictionType et) {
        evictionEnabled = et.strategy.isEnabled();
        if (et.strategy.isEnabled() && et.maxEntries <= 0)
            throw new ConfigurationException("Eviction maxEntries value cannot be less than or equal to zero if eviction is enabled");
    }

    @Override
    public void visitQueryConfigurationBean(Configuration.QueryConfigurationBean qcb) {
        if ( ! qcb.enabled ) {
            return;
        }
        // Check that the query module is on the classpath.
        try {
            String clazz = "org.infinispan.query.Search";
            ClassLoader classLoader;
            if ((classLoader = cfg.getClassLoader()) == null)
                Class.forName(clazz);
            else
                classLoader.loadClass(clazz);
        } catch (ClassNotFoundException e) {
            log.warnf("Indexing can only be enabled if infinispan-query.jar is available on your classpath, and this jar has not been detected. Intended behavior may not be exhibited.");
        }
    }

    //Pedro -- validate total order
    @Override
    public void visitTransactionType(Configuration.TransactionType bean) {
        if(!bean.transactionProtocol.isTotalOrder()) {
            //no total order or not => no validation needed
            super.visitTransactionType(bean);
            return;
        }

        //in the future we can allow this??
        if(bean.transactionMode == TransactionMode.NON_TRANSACTIONAL) {
            log.warnf("Non transactional cache can't use total order protocol... changing to normal protocol");
            bean.transactionProtocol(TransactionProtocol.NORMAL);
            super.visitTransactionType(bean);
            return;
        }

        boolean isRepeatableReadEnabled = cfg.locking.isolationLevel == IsolationLevel.REPEATABLE_READ;
        boolean isWriteSkewEnabled = cfg.isWriteSkewCheck();

        //in the future it will be allowed with versioning...
        if(isRepeatableReadEnabled && isWriteSkewEnabled) {
            log.warnf("Repeatable Read isolation level and write skew check enabled not " +
                    "allowed in total order scheme... changing to normal protocol");
            bean.transactionProtocol(TransactionProtocol.NORMAL);
            super.visitTransactionType(bean);
            return;
        }

        //for now, only supports full replication
        if(!cfg.getCacheMode().isReplicated()) {
            log.warnf("the cache mode [%s] is not supported with total order shceme", cfg.getCacheMode());
            bean.transactionProtocol(TransactionProtocol.NORMAL);
            super.visitTransactionType(bean);
            return;
        }

        //eager locking no longer needed
        if(bean.isUseEagerLocking()) {
            log.warnf("Eager locking not allowed in total order scheme... it will be disable");
            super.visitTransactionType(bean);
            bean.useEagerLocking(false);
        }

    }
}
