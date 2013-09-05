package org.infinispan.jcache;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionMode;

import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

/**
 * ConfigurationAdapter takes {@link javax.cache.configuration.Configuration} and creates
 * equivalent instance of {@link org.infinispan.configuration.cache.Configuration}
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class ConfigurationAdapter<K, V> {

   private javax.cache.configuration.Configuration<K, V> c;

   public ConfigurationAdapter(javax.cache.configuration.Configuration<K, V> configuration) {
      this.c = configuration;
   }

   public org.infinispan.configuration.cache.Configuration build() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      if (c.isStoreByValue())
         cb.storeAsBinary().enable().defensive(true);

      switch (c.getTransactionMode()) {
         case NONE:
            cb.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
            break;
         case LOCAL:
            cb.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
            break;
         case XA:
            //TODO
            break;
         default:
            break;
      }
      switch (c.getTransactionIsolationLevel()) {
         case NONE:
            cb.locking().isolationLevel(org.infinispan.util.concurrent.IsolationLevel.NONE);
            break;
         case READ_UNCOMMITTED:
            cb.locking().isolationLevel(
                     org.infinispan.util.concurrent.IsolationLevel.READ_UNCOMMITTED);
            break;
         case READ_COMMITTED:
            cb.locking().isolationLevel(
                     org.infinispan.util.concurrent.IsolationLevel.READ_COMMITTED);
            break;
         case REPEATABLE_READ:
            cb.locking().isolationLevel(
                     org.infinispan.util.concurrent.IsolationLevel.REPEATABLE_READ);
            break;
         case SERIALIZABLE:
            cb.locking().isolationLevel(org.infinispan.util.concurrent.IsolationLevel.SERIALIZABLE);
            break;
         default:
            break;
      }

      Factory<CacheLoader<K,V>> cacheLoaderFactory = c.getCacheLoaderFactory();
      if (cacheLoaderFactory != null) {
         // User-defined cache loader will be plugged once cache has started
         cb.persistence().addStore(JStoreAdapterConfigurationBuilder.class);
      }

      Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory = c.getCacheWriterFactory();
      if (cacheWriterFactory != null) {
         // User-defined cache writer will be plugged once cache has started
         cb.persistence().addStore(JCacheWriterAdapterConfigurationBuilder.class);
      }

      if (c.isStatisticsEnabled())
         cb.jmxStatistics().enable();

      return cb.build();
   }
}
