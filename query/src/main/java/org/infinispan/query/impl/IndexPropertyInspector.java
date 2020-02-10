package org.infinispan.query.impl;

import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.LOCKING_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.METADATA_CACHENAME;

import java.util.Properties;
import java.util.Set;

import org.hibernate.search.cfg.Environment;
import org.infinispan.query.indexmanager.InfinispanIndexManager;

/**
 * Extract useful information from indexing configuration, mostly related to Infinispan Index Manager.
 *
 * @author gustavonalle
 * @since 7.0
 * @deprecated since 10.1.2. This will be removed in a future version together with The Infinispan Index Manager which
 * is deprecated.
 */
@Deprecated
public final class IndexPropertyInspector {

   private IndexPropertyInspector() {
   }

   public static String getLockingCacheName(Properties properties) {
      return getPropertyFor(LOCKING_CACHENAME, properties, DEFAULT_LOCKING_CACHENAME);
   }

   public static String getDataCacheName(Properties properties) {
      return getPropertyFor(DATA_CACHENAME, properties, DEFAULT_INDEXESDATA_CACHENAME);
   }

   public static String getMetadataCacheName(Properties properties) {
      return getPropertyFor(METADATA_CACHENAME, properties, DEFAULT_INDEXESMETADATA_CACHENAME);
   }

   public static boolean isInfinispanDirectoryInternalCache(String cacheName, Properties properties) {
      return hasInfinispanDirectory(properties)
            && (cacheName.equals(getDataCacheName(properties))
            || cacheName.equals(getMetadataCacheName(properties))
            || cacheName.equals(getLockingCacheName(properties)));
   }

   public static boolean hasInfinispanDirectory(Properties properties) {
      String indexManager = getPropertyFor(Environment.INDEX_MANAGER_IMPL_NAME, properties, null);
      String directoryProvider = getPropertyFor("directory_provider", properties, null);
      return "infinispan".equals(directoryProvider) || InfinispanIndexManager.class.getName().equals(indexManager);
   }

   private static String getPropertyFor(String suffix, Properties properties, String defaultValue) {
      Set<String> propertyNames = properties.stringPropertyNames();
      String propertyValue = null;
      for (String propertyName : propertyNames) {
         if (propertyName.endsWith(suffix)) {
            propertyValue = properties.getProperty(propertyName);
         }
      }
      return propertyValue == null ? defaultValue : propertyValue;
   }

}
