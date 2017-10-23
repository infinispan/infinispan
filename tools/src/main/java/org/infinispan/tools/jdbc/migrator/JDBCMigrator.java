package org.infinispan.tools.jdbc.migrator;

import static org.infinispan.tools.jdbc.migrator.Element.BATCH;
import static org.infinispan.tools.jdbc.migrator.Element.SIZE;

import java.io.FileReader;
import java.util.Properties;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.SerializationConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.marshall.core.MarshalledEntry;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public class JDBCMigrator {

   private static final int DEFAULT_BATCH_SIZE = 1000;
   private final String defaultCacheName = this.getClass().getName();
   private final Properties properties;

   JDBCMigrator(Properties properties) {
      this.properties = properties;
   }

   void run() throws Exception {
      String batchSizeProp = properties.getProperty(BATCH + "." + SIZE);
      int batchLimit = batchSizeProp != null ? new Integer(batchSizeProp) : DEFAULT_BATCH_SIZE;

      try (JdbcStoreReader sourceReader = initAndGetSourceReader()) {
         AdvancedCache targetCache = initAndGetTargetCache();
         // Txs used so that writes to the DB are batched. Migrator will always operate locally Tx overhead should be negligible
         TransactionManager tm = targetCache.getTransactionManager();
         int txBatchSize = 0;
         for (MarshalledEntry entry : sourceReader) {
            if (txBatchSize == 0) tm.begin();

            targetCache.put(entry.getKey(), entry.getValue());
            txBatchSize++;

            if (txBatchSize == batchLimit) {
               txBatchSize = 0;
               tm.commit();
            }
         }
         if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
      }
   }

   private JdbcStoreReader initAndGetSourceReader() {
      MigratorConfiguration config = new MigratorConfiguration(true, properties);
      return new JdbcStoreReader(config);
   }

   private AdvancedCache initAndGetTargetCache() {
      MigratorConfiguration config = new MigratorConfiguration(false, properties);
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.defaultCacheName(defaultCacheName);

      SerializationConfigurationBuilder serialBuilder = builder.serialization().marshaller(config.getMarshaller());
      config.addExternalizersToConfig(serialBuilder);
      GlobalConfiguration globalConfig = builder.build();

      Configuration cacheConfig = new ConfigurationBuilder().persistence().addStore(config.getJdbcConfigBuilder()).build();
      DefaultCacheManager targetCacheManager = new DefaultCacheManager(globalConfig, new ConfigurationBuilder().build());
      targetCacheManager.defineConfiguration(config.cacheName, cacheConfig);
      return targetCacheManager.getCache(config.cacheName).getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
   }

   public static void main(String[] args) throws Exception {
      if (args.length != 1) {
         System.err.println("Usage: JDBCMigrator migrator.properties");
         System.exit(1);
      }
      Properties properties = new Properties();
      properties.load(new FileReader(args[0]));
      new JDBCMigrator(properties).run();
   }
}
