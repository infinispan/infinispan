package org.infinispan.query.blackbox;

import java.io.InputStream;

import org.infinispan.commons.util.FileLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.AbstractJdbcStoreConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Testing the ISPN Directory configuration with Async. JDBC CacheStore. The tests are performed for Clustered cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithAsyncDirTest")
public class ClusteredCacheWithAsyncDirTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Exception {
      cacheManagers.add(createCacheManager(1));
      cacheManagers.add(createCacheManager(2));
      waitForClusterToForm();

      cache1 = cacheManagers.get(0).getCache("JDBCBased_LocalIndex");
      cache2 = cacheManagers.get(1).getCache("JDBCBased_LocalIndex");
   }

   private EmbeddedCacheManager createCacheManager(int nodeIndex) throws Exception {
      InputStream is = new FileLookup().lookupFileStrict("async-jdbc-store-config.xml",
                                                                        Thread.currentThread().getContextClassLoader());
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());

      ConfigurationBuilderHolder holder = parserRegistry.parse(is);
      is.close();

      for (ConfigurationBuilder builder : holder.getNamedConfigurationBuilders().values()) {
         for (StoreConfigurationBuilder storeBuilder : builder.persistence().stores()) {
            if (storeBuilder instanceof AbstractJdbcStoreConfigurationBuilder) {
               AbstractJdbcStoreConfigurationBuilder jdbcStoreBuilder = (AbstractJdbcStoreConfigurationBuilder) storeBuilder;
               jdbcStoreBuilder.connectionPool()
                     .driverClass("org.h2.Driver")
                     .connectionUrl("jdbc:h2:mem:infinispan_string_based_" + nodeIndex + ";DB_CLOSE_DELAY=-1")
                     .username("sa");
            }
         }
      }
      return TestCacheManagerFactory.createClusteredCacheManager(holder);
   }

   protected boolean transactionsEnabled() {
      return true;
   }
}
