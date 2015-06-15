package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.remoting.transport.Address;
import org.infinispan.server.infinispan.SecurityActions;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.stats.CacheContainerStats;
import org.infinispan.Version;
import org.jboss.as.clustering.infinispan.DefaultCacheContainer;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

public class CacheContainerMetricsHandler extends AbstractRuntimeOnlyHandler {
    public static final CacheContainerMetricsHandler INSTANCE = new CacheContainerMetricsHandler();

    public enum CacheManagerMetrics {
        CACHE_MANAGER_STATUS(MetricKeys.CACHE_MANAGER_STATUS, ModelType.STRING, true),
        CLUSTER_NAME(MetricKeys.CLUSTER_NAME, ModelType.STRING, true, true),
        CLUSTER_AVAILABILITY(MetricKeys.CLUSTER_AVAILABILITY, ModelType.STRING, true, true),
        IS_COORDINATOR(MetricKeys.IS_COORDINATOR, ModelType.BOOLEAN, true, true),
        COORDINATOR_ADDRESS(MetricKeys.COORDINATOR_ADDRESS, ModelType.STRING, true, true),
        LOCAL_ADDRESS(MetricKeys.LOCAL_ADDRESS, ModelType.STRING, true, true),
        DEFINED_CACHE_NAMES(MetricKeys.DEFINED_CACHE_NAMES, ModelType.INT, true, true),
        DEFINED_CACHE_COUNT(MetricKeys.DEFINED_CACHE_COUNT, ModelType.INT, true, true),
        RUNNING_CACHE_COUNT(MetricKeys.RUNNING_CACHE_COUNT, ModelType.INT, true, true),
        CREATED_CACHE_COUNT(MetricKeys.CREATED_CACHE_COUNT, ModelType.INT, true, true),
        MEMBERS(MetricKeys.MEMBERS, ModelType.INT, true, true),
        CLUSTER_SIZE(MetricKeys.CLUSTER_SIZE, ModelType.INT, true, true),
        VERSION(MetricKeys.VERSION, ModelType.INT, true, true),

        // see org.infinispan.stats.CacheContainerStats
        AVERAGE_READ_TIME(MetricKeys.AVERAGE_READ_TIME, ModelType.LONG, true),
        AVERAGE_WRITE_TIME(MetricKeys.AVERAGE_WRITE_TIME, ModelType.LONG, true),
        AVERAGE_REMOVE_TIME(MetricKeys.AVERAGE_REMOVE_TIME, ModelType.LONG, true),
        TIME_SINCE_START(MetricKeys.TIME_SINCE_START, ModelType.LONG, true),
        EVICTIONS(MetricKeys.EVICTIONS, ModelType.LONG, true),
        HIT_RATIO(MetricKeys.HIT_RATIO, ModelType.DOUBLE, true),
        HITS(MetricKeys.HITS, ModelType.LONG, true),
        MISSES(MetricKeys.MISSES, ModelType.LONG, true),
        NUMBER_OF_ENTRIES(MetricKeys.NUMBER_OF_ENTRIES, ModelType.INT, true),
        READ_WRITE_RATIO(MetricKeys.READ_WRITE_RATIO, ModelType.DOUBLE, true),
        REMOVE_HITS(MetricKeys.REMOVE_HITS, ModelType.LONG, true),
        REMOVE_MISSES(MetricKeys.REMOVE_MISSES, ModelType.LONG, true),
        STORES(MetricKeys.STORES, ModelType.LONG, true),
        TIME_SINCE_RESET(MetricKeys.TIME_SINCE_RESET, ModelType.LONG, true);

        private static final Map<String, CacheManagerMetrics> MAP = new HashMap<String, CacheManagerMetrics>();

        static {
            for (CacheManagerMetrics metric : CacheManagerMetrics.values()) {
                MAP.put(metric.toString(), metric);
            }
        }

        final AttributeDefinition definition;
        final boolean clustered;

        private CacheManagerMetrics(final AttributeDefinition definition, final boolean clustered) {
            this.definition = definition;
            this.clustered = clustered;
        }

        private CacheManagerMetrics(String attributeName, ModelType type, boolean allowNull) {
            this(attributeName, type, allowNull, false);
        }

        private CacheManagerMetrics(String attributeName, ModelType type, boolean allowNull, boolean clustered) {
            this(new SimpleAttributeDefinitionBuilder(attributeName, type, allowNull).setStorageRuntime().build(), clustered);
        }

        @Override
        public final String toString() {
            return definition.getName();
        }

        public static CacheManagerMetrics getStat(final String stringForm) {
            return MAP.get(stringForm);
        }
    }

    /*
     * Two constraints need to be dealt with here:
     * 1. There may be no started cache container instance available to interrogate. Because of lazy deployment,
     * a cache container instance is only started upon deployment of an application which uses that cache instance.
     * 2. The attribute name passed in may not correspond to a defined metric
     *
     * Read-only attributes have no easy way to throw an exception without negatively impacting other parts
     * of the system. Therefore in such cases, as message will be logged and a ModelNode of undefined will be returned.
     */
    @Override
    protected void executeRuntimeStep(OperationContext context, ModelNode operation) throws OperationFailedException {
        final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
        final String cacheContainerName = address.getLastElement().getValue();
        final String attrName = operation.require(ModelDescriptionConstants.NAME).asString();
        final ServiceController<?> controller = context.getServiceRegistry(false).getService(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));
        DefaultCacheContainer cacheManager = (DefaultCacheContainer) controller.getValue();

        CacheManagerMetrics metric = CacheManagerMetrics.getStat(attrName);
        ModelNode result = new ModelNode();

        if (metric == null) {
            context.getFailureDescription().set(String.format("Unknown metric %s", attrName));
        } else if (cacheManager == null) {
            context.getFailureDescription().set(String.format("Unavailable cache container %s", attrName));
        } else {
            CacheContainerStats stats = cacheManager.getStats();
            switch (metric) {
                case CACHE_MANAGER_STATUS:
                    result.set(SecurityActions.getCacheManagerStatus(cacheManager).toString());
                    break;
                case IS_COORDINATOR:
                    result.set(SecurityActions.getCacheManagerIsCoordinator(cacheManager));
                    break;
                case LOCAL_ADDRESS:
                    Address localAddress = SecurityActions.getCacheManagerLocalAddress(cacheManager);
                    result.set(localAddress != null ? localAddress.toString() : "N/A");
                    break;
                case COORDINATOR_ADDRESS:
                    Address coordinatorAddress = SecurityActions.getCacheManagerCoordinatorAddress(cacheManager);
                    result.set(coordinatorAddress != null ? coordinatorAddress.toString() : "N/A");
                    break;
                case CLUSTER_AVAILABILITY:
                    result.set(SecurityActions.getCacheManagerClusterAvailability(cacheManager));
                    break;
                case CLUSTER_NAME:
                    String clusterName = SecurityActions.getCacheManagerClusterName(cacheManager);
                    result.set(clusterName != null ? clusterName : "N/A");
                    break;
                case DEFINED_CACHE_NAMES:
                    String definedCacheNames = SecurityActions.getDefinedCacheNames(cacheManager);
                    result.set(definedCacheNames != null ? definedCacheNames : "N/A");
                    break;
                case CLUSTER_SIZE:
                    List<Address> members = SecurityActions.getMembers(cacheManager);
                    result.set(members != null ? Integer.toString(members.size()) : "N/A");
                    break;
                case CREATED_CACHE_COUNT:
                    result.set(SecurityActions.getCacheCreatedCount(cacheManager));
                    break;
                case DEFINED_CACHE_COUNT:
                    result.set(SecurityActions.getDefinedCacheCount(cacheManager));
                    break;
                case MEMBERS:
                    members = SecurityActions.getMembers(cacheManager);
                    result.set(members != null ? members.toString() : "N/A");
                    break;
                case RUNNING_CACHE_COUNT:
                    result.set(SecurityActions.getRunningCacheCount(cacheManager));
                    break;
                case VERSION:
                    result.set(Version.getVersion());
                    break;
                case AVERAGE_READ_TIME:
                   result.set(stats.getAverageReadTime());
                   break;
                case AVERAGE_WRITE_TIME:
                   result.set(stats.getAverageWriteTime());
                   break;
                case AVERAGE_REMOVE_TIME:
                   result.set(stats.getAverageRemoveTime());
                   break;
                case TIME_SINCE_START:
                   result.set(stats.getTimeSinceStart());
                   break;
                case EVICTIONS:
                   result.set(stats.getEvictions());
                   break;
                case HIT_RATIO:
                   result.set(stats.getHitRatio());
                   break;
                case HITS:
                   result.set(stats.getHits());
                   break;
                case MISSES:
                   result.set(stats.getMisses());
                   break;
                case NUMBER_OF_ENTRIES:
                   result.set(stats.getCurrentNumberOfEntries());
                   break;
                case READ_WRITE_RATIO:
                   result.set(stats.getReadWriteRatio());
                   break;
                case REMOVE_HITS:
                   result.set(stats.getHits());
                   break;
                case REMOVE_MISSES:
                   result.set(stats.getRemoveMisses());
                   break;
                case STORES:
                   result.set(stats.getStores());
                   break;
                case TIME_SINCE_RESET:
                   result.set(stats.getTimeSinceStart());
                   break;
                default:
                    context.getFailureDescription().set(String.format("Unknown metric %s", metric));
                    break;
            }
            context.getResult().set(result);
        }
        context.stepCompleted();
    }

    public void registerMetrics(ManagementResourceRegistration container) {
        for (CacheManagerMetrics metric : CacheManagerMetrics.values()) {
            container.registerMetric(metric.definition, this);
        }
    }
}
