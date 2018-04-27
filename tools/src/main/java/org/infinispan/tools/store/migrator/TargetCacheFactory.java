package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.TYPE;

import java.util.Properties;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.tools.store.migrator.jdbc.JdbcConfigurationUtil;
import org.infinispan.tools.store.migrator.marshaller.SerializationConfigUtil;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;

class TargetCacheFactory {

   private static final String DEFAULT_CACHE_NAME = StoreMigrator.class.getName();

   static AdvancedCache get(Properties properties) {
      StoreProperties props = new StoreProperties(TARGET, properties);

      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.defaultCacheName(DEFAULT_CACHE_NAME);
      SerializationConfigUtil.configureSerialization(props, globalBuilder.serialization());
      GlobalConfiguration globalConfig = globalBuilder.build();

      ConfigurationBuilder configBuilder = new ConfigurationBuilder();
      configBuilder.persistence().addStore(getInitializedStoreBuilder(props));
      configBuilder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      DefaultCacheManager manager = new DefaultCacheManager(globalConfig, new ConfigurationBuilder().build());

      String cacheName = props.cacheName();
      manager.defineConfiguration(cacheName, configBuilder.build());
      return manager.getCache(cacheName).getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
   }

   private static StoreConfigurationBuilder getInitializedStoreBuilder(StoreProperties props) {
      PersistenceConfigurationBuilder persistenceBuilder = new ConfigurationBuilder().persistence();
      StoreType storeType = StoreType.valueOf(props.get(TYPE).toUpperCase());
      switch (storeType) {
         case BINARY:
         case MIXED:
            throw new CacheConfigurationException(String.format("%s cannot be a target store as it no longer exists", storeType));
         case STRING:
            return JdbcConfigurationUtil.configureStore(props, new JdbcStringBasedStoreConfigurationBuilder(persistenceBuilder));
         default:
            throw new CacheConfigurationException(String.format("Unknown store type '%s'", storeType));
      }
   }
}
