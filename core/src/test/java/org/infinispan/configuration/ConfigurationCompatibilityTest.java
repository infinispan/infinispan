package org.infinispan.configuration;

import org.infinispan.configuration.cache.CacheStoreConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoaderConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.file.FileCacheStore;
import org.testng.annotations.Test;

@Test(groups = "functional", testName= "configuration.ConfigurationCompatibilityTest")
public class ConfigurationCompatibilityTest {

   public void testModeShapeStoreConfiguration() {
      // This code courtesy of Randall Hauch
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      LoaderConfigurationBuilder lb = configurationBuilder.loaders().addCacheLoader().cacheLoader(new FileCacheStore());
      lb.addProperty("dropTableOnExit", "false").addProperty("createTableOnStart", "true")
            .addProperty("connectionFactoryClass", "org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory")
            .addProperty("connectionUrl", "jdbc:h2:file:/abs/path/string_based_db;DB_CLOSE_DELAY=1").addProperty("driverClass", "org.h2.Driver").addProperty("userName", "sa")
            .addProperty("idColumnName", "ID_COLUMN").addProperty("idColumnType", "VARCHAR(255)").addProperty("timestampColumnName", "TIMESTAMP_COLUMN")
            .addProperty("timestampColumnType", "BIGINT").addProperty("dataColumnName", "DATA_COLUMN").addProperty("dataColumnType", "BINARY")
            .addProperty("bucketTableNamePrefix", "MODE").addProperty("cacheName", "default");
   }

   public void testAS71StoreConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.loaders().shared(false).preload(true).passivation(false);
      LoaderConfigurationBuilder storeBuilder = builder.loaders().addCacheLoader().fetchPersistentState(false).purgeOnStartup(false).purgeSynchronously(true);
      storeBuilder.singletonStore().enabled(false);
   }

   public void testAS72StoreConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      LoadersConfigurationBuilder loadersBuilder = builder.loaders().shared(false).preload(true).passivation(false);
      CacheStoreConfigurationBuilder<?, ?> storeBuilder = loadersBuilder.addStore(FileCacheStoreConfigurationBuilder.class).location("/tmp").fetchPersistentState(false)
            .purgeOnStartup(false).purgeSynchronously(true);
      storeBuilder.singletonStore().enabled(false);

   }

   public void testDocumentationCacheLoadersConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.loaders()
         .passivation(false).shared(false).preload(true)
         .addFileCacheStore()
            .fetchPersistentState(true)
            .purgerThreads(3)
            .purgeSynchronously(true)
            .ignoreModifications(false)
            .purgeOnStartup(false)
            .location(System.getProperty("java.io.tmpdir"))
            .async()
               .enabled(true)
               .flushLockTimeout(15000)
               .threadPoolSize(5)
            .singletonStore()
              .enabled(true)
              .pushStateWhenCoordinator(true)
              .pushStateTimeout(20000);
   }

}
