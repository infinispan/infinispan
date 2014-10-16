package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.descriptions.StandardResourceDescriptionResolver;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

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
        sharedAttributeResolver.put(ModelKeys.BATCHING, "cache");
        sharedAttributeResolver.put(ModelKeys.MODULE, "cache");
        sharedAttributeResolver.put(ModelKeys.INDEXING, "cache");
        sharedAttributeResolver.put(ModelKeys.AUTO_CONFIG, "cache");
        sharedAttributeResolver.put(ModelKeys.INDEXING_PROPERTIES, "cache");
        sharedAttributeResolver.put(ModelKeys.JNDI_NAME, "cache");
        sharedAttributeResolver.put(ModelKeys.NAME, "cache");
        sharedAttributeResolver.put(ModelKeys.REMOTE_CACHE, "cache");
        sharedAttributeResolver.put(ModelKeys.REMOTE_SITE, "cache");
        sharedAttributeResolver.put(ModelKeys.START, "cache");
        sharedAttributeResolver.put(ModelKeys.STATISTICS, "cache");

        sharedAttributeResolver.put(ModelKeys.ASYNC_MARSHALLING, "clustered-cache");
        sharedAttributeResolver.put(ModelKeys.CACHE_AVAILABILITY, "clustered-cache");
        sharedAttributeResolver.put(ModelKeys.MODE, "clustered-cache");
        sharedAttributeResolver.put(ModelKeys.QUEUE_FLUSH_INTERVAL, "clustered-cache");
        sharedAttributeResolver.put(ModelKeys.QUEUE_SIZE, "clustered-cache");
        sharedAttributeResolver.put(ModelKeys.REMOTE_TIMEOUT, "clustered-cache");

        sharedAttributeResolver.put(ModelKeys.PROPERTIES, "loader");

        sharedAttributeResolver.put(ModelKeys.FETCH_STATE, "store");
        sharedAttributeResolver.put(ModelKeys.PASSIVATION, "store");
        sharedAttributeResolver.put(ModelKeys.PRELOAD, "store");
        sharedAttributeResolver.put(ModelKeys.PURGE, "store");
        sharedAttributeResolver.put(ModelKeys.READ_ONLY, "store");
        sharedAttributeResolver.put(ModelKeys.SHARED, "store");
        sharedAttributeResolver.put(ModelKeys.SINGLETON, "store");
        sharedAttributeResolver.put(ModelKeys.PROPERTY, "store");
        sharedAttributeResolver.put(ModelKeys.PROPERTIES, "store");

        sharedAttributeResolver.put(ModelKeys.DATASOURCE, "jdbc-store");
        sharedAttributeResolver.put(ModelKeys.DIALECT, "jdbc-store");
        sharedAttributeResolver.put(ModelKeys.BATCH_SIZE, "jdbc-store");
        sharedAttributeResolver.put(ModelKeys.FETCH_SIZE, "jdbc-store");
        sharedAttributeResolver.put(ModelKeys.PREFIX, "jdbc-store");
        sharedAttributeResolver.put(ModelKeys.ID_COLUMN + ".column", "jdbc-store");
        sharedAttributeResolver.put(ModelKeys.DATA_COLUMN + ".column", "jdbc-store");
        sharedAttributeResolver.put(ModelKeys.TIMESTAMP_COLUMN + ".column", "jdbc-store");
        sharedAttributeResolver.put(ModelKeys.ENTRY_TABLE + "table", "jdbc-store");
        sharedAttributeResolver.put(ModelKeys.BUCKET_TABLE + "table", "jdbc-store");

        // shared cache metrics
        sharedAttributeResolver.put(MetricKeys.AVERAGE_READ_TIME, "cache");
        sharedAttributeResolver.put(MetricKeys.AVERAGE_REMOVE_TIME, "cache");
        sharedAttributeResolver.put(MetricKeys.AVERAGE_WRITE_TIME, "cache");
        sharedAttributeResolver.put(MetricKeys.CACHE_NAME, "cache");
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
        sharedAttributeResolver.put(MetricKeys.VERSION, "cache");

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
        sharedAttributeResolver.put(ModelKeys.PARTITION_HANDLING, null);
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
