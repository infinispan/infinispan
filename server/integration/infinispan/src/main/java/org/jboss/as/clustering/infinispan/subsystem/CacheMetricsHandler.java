/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
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

package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.InvalidationInterceptor;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.remoting.inboundhandler.BasePerCacheInboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.server.infinispan.SecurityActions;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.infinispan.util.concurrent.locks.impl.DefaultLockManager;
import org.infinispan.xsite.XSiteAdminOperations;
import org.infinispan.xsite.status.SiteStatus;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.StringListAttributeDefinition;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

/**
 * Handler which manages read-only access to cache runtime information (metrics)
 *
 * @author Tristan Tarrant (c) 2011 Red Hat Inc.
 */

public class CacheMetricsHandler extends AbstractRuntimeOnlyHandler {
    public static final CacheMetricsHandler INSTANCE = new CacheMetricsHandler();

    public enum CacheMetrics {
        CACHE_STATUS(MetricKeys.CACHE_STATUS, ModelType.STRING, true),
        VERSION(MetricKeys.VERSION, ModelType.STRING, true),
        CACHE_NAME(MetricKeys.CACHE_NAME, ModelType.STRING, true),
        // LockManager
        NUMBER_OF_LOCKS_AVAILABLE(MetricKeys.NUMBER_OF_LOCKS_AVAILABLE, ModelType.INT, true),
        NUMBER_OF_LOCKS_HELD(MetricKeys.NUMBER_OF_LOCKS_HELD, ModelType.INT, true),
        CONCURRENCY_LEVEL(MetricKeys.CONCURRENCY_LEVEL, ModelType.INT, true),
        // CacheMgmtInterceptor
        AVERAGE_READ_TIME(MetricKeys.AVERAGE_READ_TIME, ModelType.LONG, true),
        AVERAGE_WRITE_TIME(MetricKeys.AVERAGE_WRITE_TIME, ModelType.LONG, true),
        AVERAGE_REMOVE_TIME(MetricKeys.AVERAGE_REMOVE_TIME, ModelType.LONG, true),
        TIME_SINCE_START(MetricKeys.TIME_SINCE_START, ModelType.LONG, true),
        EVICTIONS(MetricKeys.EVICTIONS, ModelType.LONG, true),
        HIT_RATIO(MetricKeys.HIT_RATIO, ModelType.DOUBLE, true),
        HITS(MetricKeys.HITS, ModelType.LONG, true),
        MISSES(MetricKeys.MISSES, ModelType.LONG, true),
        NUMBER_OF_ENTRIES(MetricKeys.NUMBER_OF_ENTRIES, ModelType.INT, true),
        NUMBER_OF_ENTRIES_IN_MEMORY(MetricKeys.NUMBER_OF_ENTRIES_IN_MEMORY, ModelType.INT, true),
        DATA_MEMORY_USED(MetricKeys.DATA_MEMORY_USED, ModelType.LONG, true),
        OFF_HEAP_MEMORY_USED(MetricKeys.OFF_HEAP_MEMORY_USED, ModelType.LONG, true),
        MINIMUM_REQUIRED_NODES(MetricKeys.MINIMUM_REQUIRED_NODES, ModelType.INT, true),
        READ_WRITE_RATIO(MetricKeys.READ_WRITE_RATIO, ModelType.DOUBLE, true),
        REMOVE_HITS(MetricKeys.REMOVE_HITS, ModelType.LONG, true),
        REMOVE_MISSES(MetricKeys.REMOVE_MISSES, ModelType.LONG, true),
        STORES(MetricKeys.STORES, ModelType.LONG, true),
        TIME_SINCE_RESET(MetricKeys.TIME_SINCE_RESET, ModelType.LONG, true),
        // TxInterceptor
        COMMITS(MetricKeys.COMMITS, ModelType.LONG, true),
        PREPARES(MetricKeys.PREPARES, ModelType.LONG, true),
        ROLLBACKS(MetricKeys.ROLLBACKS, ModelType.LONG, true),
        // InvalidationInterceptor
        INVALIDATIONS(MetricKeys.INVALIDATIONS, ModelType.LONG, true),
        // PassivationInterceptor
        PASSIVATIONS(MetricKeys.PASSIVATIONS, ModelType.STRING, true),
        // ActivationInterceptor
        ACTIVATIONS(MetricKeys.ACTIVATIONS, ModelType.STRING, true),
        CACHE_LOADER_LOADS(MetricKeys.CACHE_LOADER_LOADS, ModelType.LONG, true),
        CACHE_LOADER_MISSES(MetricKeys.CACHE_LOADER_MISSES, ModelType.LONG, true),
        // CacheStoreInterceptor
        CACHE_LOADER_STORES(MetricKeys.CACHE_LOADER_STORES, ModelType.LONG, true),
        // RpcManager
        AVERAGE_REPLICATION_TIME(MetricKeys.AVERAGE_REPLICATION_TIME, ModelType.LONG, true, true),
        REPLICATION_COUNT(MetricKeys.REPLICATION_COUNT, ModelType.LONG, true, true),
        REPLICATION_FAILURES(MetricKeys.REPLICATION_FAILURES, ModelType.LONG, true, true),
        SUCCESS_RATIO(MetricKeys.SUCCESS_RATIO, ModelType.DOUBLE, true, true),
        AVG_XSITE_TIME(MetricKeys.AVG_XSITE_REPLICATION_TIME, ModelType.LONG, true, true),
        MIN_XSITE_TIME(MetricKeys.MIN_XSITE_REPLICATION_TIME, ModelType.LONG, true, true),
        MAX_XSITE_TIME(MetricKeys.MAX_XSITE_REPLICATION_TIME, ModelType.LONG, true, true),
        SYNC_XSITE_COUNT(MetricKeys.SYNC_XSITE_COUNT, ModelType.LONG, true, true),
        ASYNC_XSITE_COUNT(MetricKeys.ASYNC_XSITE_COUNT, ModelType.LONG, true, true),
        //backup site
        ONLINE_SITES(MetricKeys.SITES_ONLINE, ModelType.LIST, ModelType.STRING, false),
        OFFLINE_SITES(MetricKeys.SITES_OFFLINE, ModelType.LIST, ModelType.STRING, false),
        MIXED_SITES(MetricKeys.SITES_MIXED, ModelType.LIST, ModelType.STRING, false),
        //Inbound Handler
        SYNC_XSITE_COUNT_RECEIVED(MetricKeys.SYNC_XSITE_COUNT_RECEIVED, ModelType.LONG, true, true),
        ASYNC_XSITE_COUNT_RECEIVED(MetricKeys.ASYNC_XSITE_COUNT_RECEIVED, ModelType.LONG, true, true);

        private static final Map<String, CacheMetrics> MAP = new HashMap<>();

        static {
            for (CacheMetrics metric : CacheMetrics.values()) {
                MAP.put(metric.toString(), metric);
            }
        }

        final AttributeDefinition definition;
        final boolean clustered;

        CacheMetrics(final AttributeDefinition definition, final boolean clustered) {
            this.definition = definition;
            this.clustered = clustered;
        }

        CacheMetrics(String attributeName, ModelType type, boolean allowNull) {
            this(new SimpleAttributeDefinitionBuilder(attributeName, type, allowNull).setStorageRuntime().build(), false);
        }

        CacheMetrics(String attributeName, ModelType type, boolean allowNull, final boolean clustered) {
            this(new SimpleAttributeDefinitionBuilder(attributeName, type, allowNull).setStorageRuntime().build(), clustered);
        }

        CacheMetrics(String attributeName, ModelType outerType, ModelType innerType, boolean allowNull) {
            if (outerType != ModelType.LIST) {
                throw new IllegalArgumentException();
            }
            if (innerType != ModelType.STRING) {
                throw new IllegalArgumentException();
            }
            this.definition = new StringListAttributeDefinition.Builder(attributeName).setRequired(!allowNull).build();
            this.clustered = false;
        }

        @Override
        public final String toString() {
            return definition.getName();
        }

        public static CacheMetrics getStat(final String stringForm) {
            return MAP.get(stringForm);
        }
    }

    /*
     * Two constraints need to be dealt with here:
     * 1. There may be no started cache instance available to interrogate. Because of lazy deployment,
     * a cache instance is only started upon deployment of an application which uses that cache instance.
     * 2. The attribute name passed in may not correspond to a defined metric
     *
     * Read-only attributes have no easy way to throw an exception without negatively impacting other parts
     * of the system. Therefore in such cases, as message will be logged and a ModelNode of undefined will be returned.
     */
    @Override
    protected void executeRuntimeStep(OperationContext context, ModelNode operation) {
        final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
        final String cacheContainerName = address.getElement(address.size() - 2).getValue();
        final String cacheName = address.getLastElement().getValue();
        final String attrName = operation.require(NAME).asString();
        final ServiceController<?> controller = context.getServiceRegistry(false).getService(CacheServiceName.CACHE.getServiceName(cacheContainerName, cacheName));
        Cache<?, ?> cache = (Cache<?, ?>) controller.getValue();
        CacheMetrics metric = CacheMetrics.getStat(attrName);
        ModelNode result = new ModelNode();

        if (metric == null) {
            context.getFailureDescription().set(String.format("Unknown metric %s", attrName));
        } else if (cache == null) {
            context.getFailureDescription().set(String.format("Unavailable cache %s", attrName));
        } else {
            AdvancedCache<?, ?> aCache = cache.getAdvancedCache();
            DefaultLockManager lockManager = (DefaultLockManager) SecurityActions.getLockManager(aCache);
            RpcManagerImpl rpcManager = (RpcManagerImpl) SecurityActions.getRpcManager(aCache);
            List<AsyncInterceptor> interceptors = SecurityActions.getInterceptorChain(aCache);
            ComponentRegistry registry = SecurityActions.getComponentRegistry(aCache);
            ComponentStatus status = SecurityActions.getCacheStatus(aCache);
            BasePerCacheInboundInvocationHandler handler = (BasePerCacheInboundInvocationHandler) registry.getPerCacheInboundInvocationHandler();
            switch (metric) {
                case CACHE_STATUS:
                    result.set(status.toString());
                    break;
                case CONCURRENCY_LEVEL:
                    result.set(lockManager.getConcurrencyLevel());
                    break;
                case NUMBER_OF_LOCKS_AVAILABLE:
                    result.set(lockManager.getNumberOfLocksAvailable());
                    break;
                case NUMBER_OF_LOCKS_HELD:
                    result.set(lockManager.getNumberOfLocksHeld());
                    break;
                case AVERAGE_READ_TIME: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getAverageReadTime() : 0);
                    break;
                }
                case AVERAGE_WRITE_TIME: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getAverageWriteTime() : 0);
                    break;
                }
                case AVERAGE_REMOVE_TIME: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getAverageRemoveTime() : 0);
                    break;
                }
                case TIME_SINCE_START: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getTimeSinceStart() : 0);
                    break;
                }
                case EVICTIONS: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getEvictions() : 0);
                    break;
                }
                case HIT_RATIO: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getHitRatio() : 0);
                    break;
                }
                case HITS: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getHits() : 0);
                    break;
                }
                case MISSES: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getMisses() : 0);
                    break;
                }
                case NUMBER_OF_ENTRIES: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getNumberOfEntries() : 0);
                    break;
                }
                case NUMBER_OF_ENTRIES_IN_MEMORY: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getNumberOfEntriesInMemory() : 0);
                    break;
                }
                case DATA_MEMORY_USED: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getDataMemoryUsed() : 0);
                    break;
                }
                case OFF_HEAP_MEMORY_USED: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getOffHeapMemoryUsed() : 0);
                    break;
                }
                case MINIMUM_REQUIRED_NODES: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getRequiredMinimumNumberOfNodes() : 0);
                    break;
                }
                case READ_WRITE_RATIO: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getReadWriteRatio() : 0);
                    break;
                }
                case REMOVE_HITS: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getRemoveHits() : 0);
                    break;
                }
                case REMOVE_MISSES: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getRemoveMisses() : 0);
                    break;
                }
                case STORES: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getStores() : 0);
                    break;
                }
                case TIME_SINCE_RESET: {
                    CacheMgmtInterceptor cacheMgmtInterceptor = getFirstInterceptorWhichExtends(interceptors, CacheMgmtInterceptor.class);
                    result.set(cacheMgmtInterceptor != null ? cacheMgmtInterceptor.getTimeSinceReset() : 0);
                    break;
                }
                case AVERAGE_REPLICATION_TIME: {
                    result.set(rpcManager.getAverageReplicationTime());
                    break;
                }
                case REPLICATION_COUNT:
                    result.set(rpcManager.getReplicationCount());
                    break;
                case REPLICATION_FAILURES:
                    result.set(rpcManager.getReplicationFailures());
                    break;
                case SUCCESS_RATIO:
                    result.set(rpcManager.getSuccessRatioFloatingPoint());
                    break;
                case AVG_XSITE_TIME:
                    result.set(rpcManager.getAverageXSiteReplicationTime());
                    break;
                case MAX_XSITE_TIME:
                    result.set(rpcManager.getMaximumXSiteReplicationTime());
                    break;
                case MIN_XSITE_TIME:
                    result.set(rpcManager.getMinimumXSiteReplicationTime());
                    break;
                case SYNC_XSITE_COUNT:
                    result.set(rpcManager.getSyncXSiteCount());
                    break;
                case ASYNC_XSITE_COUNT:
                    result.set(rpcManager.getAsyncXSiteCount());
                    break;
                case SYNC_XSITE_COUNT_RECEIVED:
                    result.set(handler.getSyncXSiteRequestsReceived());
                    break;
                case ASYNC_XSITE_COUNT_RECEIVED:
                    result.set(handler.getAsyncXSiteRequestsReceived());
                    break;
                case COMMITS: {
                    TxInterceptor txInterceptor = getFirstInterceptorWhichExtends(interceptors, TxInterceptor.class);
                    result.set(txInterceptor != null ? txInterceptor.getCommits() : 0);
                    break;
                }
                case PREPARES: {
                    TxInterceptor txInterceptor = getFirstInterceptorWhichExtends(interceptors, TxInterceptor.class);
                    result.set(txInterceptor != null ? txInterceptor.getPrepares() : 0);
                    break;
                }
                case ROLLBACKS: {
                    TxInterceptor txInterceptor = getFirstInterceptorWhichExtends(interceptors, TxInterceptor.class);
                    result.set(txInterceptor != null ? txInterceptor.getRollbacks() : 0);
                    break;
                }
                case INVALIDATIONS: {
                    InvalidationInterceptor invInterceptor = getFirstInterceptorWhichExtends(interceptors, InvalidationInterceptor.class);
                    result.set(invInterceptor != null ? invInterceptor.getInvalidations() : 0);
                    break;
                }
                case PASSIVATIONS: {
                    PassivationManager manager = registry.getComponent(PassivationManager.class);
                    result.set(manager != null ? manager.getPassivations() : 0);
                    break;
                }
                case ACTIVATIONS: {
                    ActivationManager manager = registry.getComponent(ActivationManager.class);
                    result.set(manager != null ? manager.getActivationCount() : 0);
                    break;
                }
                case CACHE_LOADER_LOADS: {
                    CacheLoaderInterceptor
                          interceptor = getFirstInterceptorWhichExtends(interceptors, CacheLoaderInterceptor.class);
                    result.set(interceptor != null ? interceptor.getCacheLoaderLoads() : 0);
                    break;
                }
                case CACHE_LOADER_MISSES: {
                    CacheLoaderInterceptor interceptor = getFirstInterceptorWhichExtends(interceptors, CacheLoaderInterceptor.class);
                    result.set(interceptor != null ? interceptor.getCacheLoaderMisses() : 0);
                    break;
                }
                case CACHE_LOADER_STORES: {
                    CacheWriterInterceptor
                          interceptor = getFirstInterceptorWhichExtends(interceptors, CacheWriterInterceptor.class);
                    result.set(interceptor != null ? interceptor.getWritesToTheStores() : 0);
                    break;
                }
                case CACHE_NAME: {
                    result.set(cache.getName());
                    break;
                }
                case VERSION: {
                    result.set(SecurityActions.getCacheVersion(aCache));
                    break;
                }
                case OFFLINE_SITES:
                case ONLINE_SITES:
                case MIXED_SITES: {
                    Collection<String> sites = filterSitesByStatus(registry.getComponent(XSiteAdminOperations.class), metric);
                    if (sites.isEmpty()) {
                        result.setEmptyList();
                    } else {
                        result.set(toModelNodeCollection(sites));
                    }
                    break;
                }
                default:{
                    context.getFailureDescription().set(String.format("Unknown metric %s", metric));
                    break;
                }
            }
            context.getResult().set(result);
        }
    }

    public void registerCommonMetrics(ManagementResourceRegistration container) {
        for (CacheMetrics metric : CacheMetrics.values()) {
            if (!metric.clustered) {
                container.registerMetric(metric.definition, this);
            }
        }
    }

    public void registerClusteredMetrics(ManagementResourceRegistration container) {
        for (CacheMetrics metric : CacheMetrics.values()) {
            if (metric.clustered) {
                container.registerMetric(metric.definition, this);
            }
        }
    }

    public static <T extends AsyncInterceptor> T getFirstInterceptorWhichExtends(List<AsyncInterceptor> interceptors,
                                                                                    Class<T> interceptorClass) {
        for (AsyncInterceptor interceptor : interceptors) {
            boolean isSubclass = interceptorClass.isAssignableFrom(interceptor.getClass());
            if (isSubclass) {
                return (T) interceptor;
            }
        }
        return null;
    }

    private static Collection<String> filterSitesByStatus(XSiteAdminOperations operations, CacheMetrics metric) {
        if (operations == null) {
            return Collections.emptyList();
        }
        Map<String, SiteStatus> statusMap = operations.clusterStatus();
        if (statusMap.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> filterSites = new LinkedList<>();
        for (Map.Entry<String, SiteStatus> entry : statusMap.entrySet()) {
            switch (metric) {
                case ONLINE_SITES:
                    //online sites only
                    if (entry.getValue().isOnline()) {
                        filterSites.add(entry.getKey());
                    }
                    break;
                case OFFLINE_SITES:
                    //offline sites only
                    if (entry.getValue().isOffline()) {
                        filterSites.add(entry.getKey());
                    }
                    break;
                case MIXED_SITES:
                    //mixed sites only
                    if (!entry.getValue().isOnline() && !entry.getValue().isOffline()) {
                        filterSites.add(entry.getKey());
                    }
                    break;
                default:
                    return Collections.emptyList();
            }
        }
        return filterSites;
    }

    private static Collection<ModelNode> toModelNodeCollection(Collection<String> collection) {
        if (collection == null || collection.isEmpty()) {
            return Collections.emptyList();
        }
        Collection<ModelNode> modelNodeCollection = new ArrayList<>(collection.size());
        collection.forEach(e -> modelNodeCollection.add(new ModelNode().set(e)));
        return modelNodeCollection;
    }
}
