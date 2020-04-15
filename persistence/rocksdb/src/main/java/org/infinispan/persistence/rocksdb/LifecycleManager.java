package org.infinispan.persistence.rocksdb;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StatisticsConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration;
import org.infinispan.persistence.rocksdb.logging.Log;
import org.infinispan.persistence.rocksdb.metrics.StatisticsExporter;
import org.infinispan.persistence.rocksdb.metrics.StatisticsExporterImpl;
import org.infinispan.registry.InternalCacheRegistry;

@InfinispanModule(name = "cachestore-rocksdb", requiredModules = "core")
public class LifecycleManager implements ModuleLifecycle {
    private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

    @Override
    public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
        InternalCacheRegistry icr = cr.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
        if (!icr.isInternalCache(cacheName) || icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.QUERYABLE)) {
            Attribute<Boolean> attribute = configuration.statistics().attributes().attribute(StatisticsConfiguration.ENABLED);
            if(attribute != null && attribute.get() == Boolean.TRUE) {
                for (StoreConfiguration store : configuration.persistence().stores()) {
                    if(store instanceof RocksDBStoreConfiguration) {
                        cr.registerComponent(new StatisticsExporterImpl(), StatisticsExporter.class);
                    }
                }
            }
        }
    }
}
