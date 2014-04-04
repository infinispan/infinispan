package org.jboss.as.clustering.infinispan.subsystem;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import org.jboss.as.controller.descriptions.StandardResourceDescriptionResolver;

/**
 * Custom resource description resolver to handle resources structured in a class hierarchy
 * which need to share resource name definitions.
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class InfinispanResourceDescriptionResolver extends StandardResourceDescriptionResolver {

    private static final Map<String, String> sharedAttributeResolver;

    public InfinispanResourceDescriptionResolver(String keyPrefix, String bundleBaseName, ClassLoader bundleLoader) {
        super(keyPrefix, bundleBaseName, bundleLoader, true, false);
    }

    @Override
    public String getResourceAttributeDescription(String attributeName, Locale locale, ResourceBundle bundle) {
        // don't apply the default bundle prefix to these attributes
        if (sharedAttributeResolver.containsKey(attributeName)) {
            return bundle.getString(getBundleKey(attributeName));
        }
        return super.getResourceAttributeDescription(attributeName, locale, bundle);
    }

    @Override
    public String getResourceAttributeValueTypeDescription(String attributeName, Locale locale, ResourceBundle bundle, String... suffixes) {
        // don't apply the default bundle prefix to these attributes
        if (sharedAttributeResolver.containsKey(attributeName)) {
            return bundle.getString(getVariableBundleKey(attributeName, suffixes));
        }
        return super.getResourceAttributeValueTypeDescription(attributeName, locale, bundle, suffixes);
    }

    @Override
    public String getOperationParameterDescription(String operationName, String paramName, Locale locale, ResourceBundle bundle) {
        // don't apply the default bundle prefix to these attributes
        if (sharedAttributeResolver.containsKey(paramName)) {
            return bundle.getString(getBundleKey(paramName));
        }
        return super.getOperationParameterDescription(operationName, paramName, locale, bundle);
    }

    @Override
    public String getOperationParameterValueTypeDescription(String operationName, String paramName, Locale locale, ResourceBundle bundle, String... suffixes) {
        // don't apply the default bundle prefix to these attributes
        if (sharedAttributeResolver.containsKey(paramName)) {
            return bundle.getString(getVariableBundleKey(paramName, suffixes));
        }
        return super.getOperationParameterValueTypeDescription(operationName, paramName, locale, bundle, suffixes);
    }

    @Override
    public String getChildTypeDescription(String childType, Locale locale, ResourceBundle bundle) {
        // don't apply the default bundle prefix to these attributes
        if (sharedAttributeResolver.containsKey(childType)) {
            return bundle.getString(getBundleKey(childType));
        }
        return super.getChildTypeDescription(childType, locale, bundle);
    }

    private String getBundleKey(final String name) {
        return getVariableBundleKey(name);
    }

    private String getVariableBundleKey(final String name, final String... variable) {
        final String prefix = sharedAttributeResolver.get(name);
        StringBuilder sb = new StringBuilder(InfinispanExtension.SUBSYSTEM_NAME);
        // construct the key prefix
        if (prefix == null) {
            sb = sb.append('.').append(name);
        } else {
            sb = sb.append('.').append(prefix).append('.').append(name);
        }
        // construct the key suffix
        if (variable != null) {
            for (String arg : variable) {
                if (sb.length() > 0)
                    sb.append('.');
                sb.append(arg);
            }
        }
        return sb.toString();
    }

    static {
        sharedAttributeResolver = new HashMap<String, String>();
        // shared cache attributes
        sharedAttributeResolver.put(CacheResource.BATCHING.getName(), "cache");
        sharedAttributeResolver.put(CacheResource.CACHE_MODULE.getName(), "cache");
        sharedAttributeResolver.put(CacheResource.INDEXING.getName(), "cache");
        sharedAttributeResolver.put(CacheResource.INDEXING_PROPERTIES.getName(), "cache");
        sharedAttributeResolver.put(CacheResource.JNDI_NAME.getName(), "cache");
        sharedAttributeResolver.put(CacheResource.NAME.getName(), "cache");
        sharedAttributeResolver.put(CacheResource.START.getName(), "cache");
        sharedAttributeResolver.put(CacheResource.STATISTICS.getName(), "cache");

        sharedAttributeResolver.put(ClusteredCacheResource.ASYNC_MARSHALLING.getName(), "clustered-cache");
        sharedAttributeResolver.put(ClusteredCacheResource.MODE.getName(), "clustered-cache");
        sharedAttributeResolver.put(ClusteredCacheResource.QUEUE_FLUSH_INTERVAL.getName(), "clustered-cache");
        sharedAttributeResolver.put(ClusteredCacheResource.QUEUE_SIZE.getName(), "clustered-cache");
        sharedAttributeResolver.put(ClusteredCacheResource.REMOTE_TIMEOUT.getName(), "clustered-cache");

        sharedAttributeResolver.put(BaseStoreResource.PROPERTIES.getName(), "loader");

        sharedAttributeResolver.put(BaseStoreResource.FETCH_STATE.getName(), "store");
        sharedAttributeResolver.put(BaseStoreResource.PASSIVATION.getName(), "store");
        sharedAttributeResolver.put(BaseStoreResource.PRELOAD.getName(), "store");
        sharedAttributeResolver.put(BaseStoreResource.PURGE.getName(), "store");
        sharedAttributeResolver.put(BaseStoreResource.READ_ONLY.getName(), "store");
        sharedAttributeResolver.put(BaseStoreResource.SHARED.getName(), "store");
        sharedAttributeResolver.put(BaseStoreResource.SINGLETON.getName(), "store");
        sharedAttributeResolver.put(BaseStoreResource.PROPERTY.getName(), "store");
        sharedAttributeResolver.put(BaseStoreResource.PROPERTIES.getName(), "store");

        sharedAttributeResolver.put(BaseJDBCStoreResource.DATA_SOURCE.getName(), "jdbc-store");
        sharedAttributeResolver.put(BaseJDBCStoreResource.BATCH_SIZE.getName(), "jdbc-store");
        sharedAttributeResolver.put(BaseJDBCStoreResource.FETCH_SIZE.getName(), "jdbc-store");
        sharedAttributeResolver.put(BaseJDBCStoreResource.PREFIX.getName(), "jdbc-store");
        sharedAttributeResolver.put(BaseJDBCStoreResource.ID_COLUMN.getName() + ".column", "jdbc-store");
        sharedAttributeResolver.put(BaseJDBCStoreResource.DATA_COLUMN.getName() + ".column", "jdbc-store");
        sharedAttributeResolver.put(BaseJDBCStoreResource.TIMESTAMP_COLUMN.getName() + ".column", "jdbc-store");
        sharedAttributeResolver.put(BaseJDBCStoreResource.ENTRY_TABLE.getName() + "table", "jdbc-store");
        sharedAttributeResolver.put(BaseJDBCStoreResource.BUCKET_TABLE.getName() + "table", "jdbc-store");

        // shared cache metrics
        sharedAttributeResolver.put(MetricKeys.AVERAGE_READ_TIME, "cache");
        sharedAttributeResolver.put(MetricKeys.AVERAGE_WRITE_TIME, "cache");
        sharedAttributeResolver.put(MetricKeys.CACHE_STATUS, "cache");
        sharedAttributeResolver.put(MetricKeys.COMMITS, "cache");
        sharedAttributeResolver.put(MetricKeys.CONCURRENCY_LEVEL, "cache");
        sharedAttributeResolver.put(MetricKeys.EVICTIONS, "cache");
        sharedAttributeResolver.put(MetricKeys.ELAPSED_TIME, "cache");
        sharedAttributeResolver.put(MetricKeys.HIT_RATIO, "cache");
        sharedAttributeResolver.put(MetricKeys.HITS, "cache");
        sharedAttributeResolver.put(MetricKeys.INVALIDATIONS, "cache");
        sharedAttributeResolver.put(MetricKeys.MISSES, "cache");
        sharedAttributeResolver.put(MetricKeys.NUMBER_OF_ENTRIES, "cache");
        sharedAttributeResolver.put(MetricKeys.NUMBER_OF_LOCKS_AVAILABLE, "cache");
        sharedAttributeResolver.put(MetricKeys.NUMBER_OF_LOCKS_HELD, "cache");
        sharedAttributeResolver.put(MetricKeys.PREPARES, "cache");
        sharedAttributeResolver.put(MetricKeys.READ_WRITE_RATIO, "cache");
        sharedAttributeResolver.put(MetricKeys.REMOVE_HITS, "cache");
        sharedAttributeResolver.put(MetricKeys.REMOVE_MISSES, "cache");
        sharedAttributeResolver.put(MetricKeys.ROLLBACKS, "cache");
        sharedAttributeResolver.put(MetricKeys.STORES, "cache");
        sharedAttributeResolver.put(MetricKeys.TIME_SINCE_RESET, "cache");

        sharedAttributeResolver.put(MetricKeys.AVERAGE_REPLICATION_TIME, "clustered-cache");
        sharedAttributeResolver.put(MetricKeys.REPLICATION_COUNT, "clustered-cache");
        sharedAttributeResolver.put(MetricKeys.REPLICATION_FAILURES, "clustered-cache");
        sharedAttributeResolver.put(MetricKeys.SUCCESS_RATIO, "clustered-cache");

        // shared loader attributes
        sharedAttributeResolver.put(MetricKeys.ACTIVATIONS, "loader");
        sharedAttributeResolver.put(MetricKeys.CACHE_LOADER_LOADS, "loader");
        sharedAttributeResolver.put(MetricKeys.CACHE_LOADER_MISSES, "loader");
        sharedAttributeResolver.put(MetricKeys.CACHE_LOADER_STORES, "loader");
        sharedAttributeResolver.put(MetricKeys.PASSIVATIONS, "loader");

       // shared children - this avoids having to describe the children for each parent resource
        sharedAttributeResolver.put(ModelKeys.TRANSPORT, null);
        sharedAttributeResolver.put(ModelKeys.SECURITY, "cache");
        sharedAttributeResolver.put(ModelKeys.LOCKING, null);
        sharedAttributeResolver.put(ModelKeys.TRANSACTION, null);
        sharedAttributeResolver.put(ModelKeys.EVICTION, null);
        sharedAttributeResolver.put(ModelKeys.EXPIRATION, null);
        sharedAttributeResolver.put(ModelKeys.STATE_TRANSFER, null);
        sharedAttributeResolver.put(ModelKeys.BACKUP, null);
        sharedAttributeResolver.put(ModelKeys.LOADER, null);
        sharedAttributeResolver.put(ModelKeys.COMPATIBILITY, null);
        sharedAttributeResolver.put(ModelKeys.CLUSTER_LOADER, null);
        sharedAttributeResolver.put(ModelKeys.STORE, null);
        sharedAttributeResolver.put(ModelKeys.FILE_STORE, null);
        sharedAttributeResolver.put(ModelKeys.REMOTE_STORE, null);
        sharedAttributeResolver.put(ModelKeys.REST_STORE, null);
        sharedAttributeResolver.put(ModelKeys.STRING_KEYED_JDBC_STORE, null);
        sharedAttributeResolver.put(ModelKeys.BINARY_KEYED_JDBC_STORE, null);
        sharedAttributeResolver.put(ModelKeys.MIXED_KEYED_JDBC_STORE, null);
        sharedAttributeResolver.put(ModelKeys.WRITE_BEHIND, null);
        sharedAttributeResolver.put(ModelKeys.PROPERTY, null);
        sharedAttributeResolver.put(ModelKeys.IMPLEMENTATION, null);
        sharedAttributeResolver.put(ModelKeys.COMPRESSION, null);
        sharedAttributeResolver.put(ModelKeys.LEVELDB_STORE, null);
    }
}
