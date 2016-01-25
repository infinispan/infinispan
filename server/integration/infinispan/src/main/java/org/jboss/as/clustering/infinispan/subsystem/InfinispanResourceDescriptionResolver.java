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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import org.infinispan.server.commons.controller.descriptions.SubsystemResourceDescriptionResolver;
import org.jboss.as.clustering.infinispan.subsystem.ClusteredCacheMetricsHandler.ClusteredCacheMetrics;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;

/**
 * Custom resource description resolver to handle resources structured in a class hierarchy
 * which need to share resource name definitions.
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class InfinispanResourceDescriptionResolver extends SubsystemResourceDescriptionResolver {

    private Map<String, String> sharedAttributeResolver = new HashMap<>();

    InfinispanResourceDescriptionResolver() {
        this(Collections.<String>emptyList());
    }

    InfinispanResourceDescriptionResolver(String keyPrefix) {
        this(Collections.singletonList(keyPrefix));
    }

    InfinispanResourceDescriptionResolver(String... keyPrefixes) {
        this(Arrays.asList(keyPrefixes));
    }

    private InfinispanResourceDescriptionResolver(List<String> keyPrefixes) {
        super(InfinispanExtension.SUBSYSTEM_NAME, keyPrefixes, InfinispanExtension.class);
        initMap();
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
    public String getResourceAttributeDeprecatedDescription(String attributeName, Locale locale, ResourceBundle bundle) {
        if (sharedAttributeResolver.containsKey(attributeName)) {
            return bundle.getString(getVariableBundleKey(attributeName, ModelDescriptionConstants.DEPRECATED));
        }
        return super.getResourceAttributeDeprecatedDescription(attributeName, locale, bundle);
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
    public String getOperationParameterDeprecatedDescription(String operationName, String paramName, Locale locale, ResourceBundle bundle) {
        if (sharedAttributeResolver.containsKey(paramName)) {
            return bundle.getString(getVariableBundleKey(paramName, ModelDescriptionConstants.DEPRECATED));
        }
        return super.getOperationParameterDeprecatedDescription(operationName, paramName, locale, bundle);
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
        if (prefix != null) {
            sb.append('.').append(prefix);
        }
        sb.append('.').append(name);
        // construct the key suffix
        if (variable != null) {
            for (String arg : variable) {
                sb.append('.').append(arg);
            }
        }
        return sb.toString();
    }

    private void initMap() {
        sharedAttributeResolver = new HashMap<>();
        // shared cache attributes
        sharedAttributeResolver.put(ModelKeys.BATCHING, "cache");
        sharedAttributeResolver.put(ModelKeys.CONFIGURATION, "cache");
        sharedAttributeResolver.put(ModelKeys.MODULE, "cache");
        sharedAttributeResolver.put(ModelKeys.INDEXING, "cache");
        sharedAttributeResolver.put(ModelKeys.INLINE_INTERCEPTORS, "cache");
        sharedAttributeResolver.put(ModelKeys.AUTO_CONFIG, "cache");
        sharedAttributeResolver.put(ModelKeys.INDEXING_PROPERTIES, "cache");
        sharedAttributeResolver.put(ModelKeys.JNDI_NAME, "cache");
        sharedAttributeResolver.put(ModelKeys.NAME, "cache");
        sharedAttributeResolver.put(ModelKeys.REMOTE_CACHE, "cache");
        sharedAttributeResolver.put(ModelKeys.REMOTE_SITE, "cache");
        sharedAttributeResolver.put(ModelKeys.SIMPLE_CACHE, "cache");
        sharedAttributeResolver.put(ModelKeys.START, "cache");
        sharedAttributeResolver.put(ModelKeys.STATISTICS, "cache");
        sharedAttributeResolver.put(ModelKeys.STATISTICS_AVAILABLE, "cache");

        sharedAttributeResolver.put(ModelKeys.ASYNC_MARSHALLING, "clustered-cache");
        sharedAttributeResolver.put(ModelKeys.CACHE_AVAILABILITY, "clustered-cache");
        sharedAttributeResolver.put(ModelKeys.CACHE_REBALANCE, "clustered-cache");
        sharedAttributeResolver.put(ModelKeys.CACHE_REBALANCING_STATUS, "clustered-cache");
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
        sharedAttributeResolver.put(MetricKeys.TIME_SINCE_START, "cache");
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
        sharedAttributeResolver.put("thread-pool", null);

        for (ClusteredCacheMetrics key : ClusteredCacheMetricsHandler.ClusteredCacheMetrics.values()) {
           sharedAttributeResolver.put(key.definition.getName(), "clustered-cache");
        }
    }
}
