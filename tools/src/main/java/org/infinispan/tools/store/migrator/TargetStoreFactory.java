package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.COMPRESSION;
import static org.infinispan.tools.store.migrator.Element.INDEX_LOCATION;
import static org.infinispan.tools.store.migrator.Element.LOCATION;
import static org.infinispan.tools.store.migrator.Element.SEGMENT_COUNT;
import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.TYPE;

import java.util.Properties;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.context.Flag;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.rocksdb.configuration.CompressionType;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.tools.store.migrator.jdbc.JdbcConfigurationUtil;
import org.infinispan.tools.store.migrator.marshaller.SerializationConfigUtil;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;

class TargetStoreFactory {

   private static final String DEFAULT_CACHE_NAME = StoreMigrator.class.getName();

   static EmbeddedCacheManager getCacheManager(Properties properties) {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder().defaultCacheName(DEFAULT_CACHE_NAME);
      StoreProperties props = new StoreProperties(TARGET, properties);
      SerializationConfigUtil.configureSerialization(props, holder.getGlobalConfigurationBuilder().serialization());

      holder.getNamedConfigurationBuilders().put(DEFAULT_CACHE_NAME, new ConfigurationBuilder());

      return new DefaultCacheManager(holder, true);
   }

   static AdvancedCache getTargetCache(EmbeddedCacheManager manager, Properties properties) {
      StoreProperties props = new StoreProperties(TARGET, properties);

      ConfigurationBuilder configBuilder = new ConfigurationBuilder();
      String segmentCountString = props.get(SEGMENT_COUNT);
      // 0 means not enabled
      int segmentCount = 0;
      if (segmentCountString != null) {
         segmentCount = Integer.parseInt(segmentCountString);
         if (segmentCount < 0) {
            throw new IllegalArgumentException("Segment count must be > 0");
         }
      }

      if (segmentCount > 0) {
         configBuilder.clustering().hash().numSegments(segmentCount);
      }

      configBuilder.persistence().addStore(getInitializedStoreBuilder(props))
            .purgeOnStartup(true)
            .segmented(segmentCount > 0);
      configBuilder.invocationBatching().transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup());

      String cacheName = props.cacheName();
      manager.defineConfiguration(cacheName, configBuilder.build());
      return manager.getCache(cacheName).getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
   }

   private static StoreConfigurationBuilder getInitializedStoreBuilder(StoreProperties props) {
      PersistenceConfigurationBuilder persistenceBuilder = new ConfigurationBuilder().persistence();
      StoreType storeType = StoreType.valueOf(props.get(TYPE).toUpperCase());
      switch (storeType) {
         case LEVELDB:
         case JDBC_BINARY:
         case JDBC_MIXED:
            throw new CacheConfigurationException(String.format("%s cannot be a target store as it no longer exists", storeType));
         case JDBC_STRING:
            return JdbcConfigurationUtil.configureStore(props, new JdbcStringBasedStoreConfigurationBuilder(persistenceBuilder));
         case ROCKSDB:
            props.required(LOCATION);
            String location = props.get(LOCATION);
            RocksDBStoreConfigurationBuilder builder = new RocksDBStoreConfigurationBuilder(persistenceBuilder);
            builder.location(location).expiredLocation(location + "-expired-");
            String compressionType = props.get(COMPRESSION);
            if (compressionType != null)
               builder.compressionType(CompressionType.valueOf(compressionType.toUpperCase()));
            return builder;
         case SOFT_INDEX_FILE_STORE:
            props.required(LOCATION);
            props.required(INDEX_LOCATION);
            return new SoftIndexFileStoreConfigurationBuilder(persistenceBuilder)
                  .dataLocation(props.get(LOCATION)).indexLocation(props.get(INDEX_LOCATION));
         default:
            throw new CacheConfigurationException(String.format("Unknown store type '%s'", storeType));
      }
   }
}
