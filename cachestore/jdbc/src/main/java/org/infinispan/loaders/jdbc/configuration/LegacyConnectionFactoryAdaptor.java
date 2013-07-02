package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.loaders.jdbc.AbstractJdbcCacheStoreConfig;

/**
 * LegacyConnectionFactoryAdaptor.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Deprecated
public interface LegacyConnectionFactoryAdaptor {
   void adapt(AbstractJdbcCacheStoreConfig config);
}
