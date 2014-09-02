package org.infinispan.query.impl;

import org.hibernate.search.cfg.Environment;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.query.indexmanager.InfinispanIndexManager;

import java.util.Properties;
import java.util.Set;

import static org.hibernate.search.infinispan.InfinispanIntegration.*;

/**
 * Extract useful information from indexing configuration
 *
 * @author gustavonalle
 * @since 7.0
 */
public final class IndexPropertyInspector {

   public static String getLockingCacheName(Properties properties) {
      return getPropertyFor(LOCKING_CACHENAME, properties, DEFAULT_LOCKING_CACHENAME);
   }

   public static String getDataCacheName(Properties properties) {
      return getPropertyFor(DATA_CACHENAME, properties, DEFAULT_INDEXESDATA_CACHENAME);
   }

   public static String getMetadataCacheName(Properties properties) {
      return getPropertyFor(METADATA_CACHENAME, properties, DEFAULT_INDEXESMETADATA_CACHENAME);
   }

   public static boolean hasInfinispanDirectory(Properties properties) {
      String indexManager = getPropertyFor(Environment.INDEX_MANAGER_IMPL_NAME, properties, null);
      String directoryProvider = getPropertyFor("directory_provider", properties, null);
      return (directoryProvider != null && "infinispan".equals(directoryProvider)) ||
            (indexManager != null && indexManager.equals(InfinispanIndexManager.class.getName()));
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
