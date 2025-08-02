package org.infinispan.tools.store.migrator.jdbc;

import static org.infinispan.tools.store.migrator.Element.CONNECTION_POOL;
import static org.infinispan.tools.store.migrator.Element.CONNECTION_URL;
import static org.infinispan.tools.store.migrator.Element.DATA;
import static org.infinispan.tools.store.migrator.Element.DIALECT;
import static org.infinispan.tools.store.migrator.Element.DRIVER_CLASS;
import static org.infinispan.tools.store.migrator.Element.ID;
import static org.infinispan.tools.store.migrator.Element.LOCATION;
import static org.infinispan.tools.store.migrator.Element.NAME;
import static org.infinispan.tools.store.migrator.Element.SEGMENT;
import static org.infinispan.tools.store.migrator.Element.SEGMENT_COUNT;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.STRING;
import static org.infinispan.tools.store.migrator.Element.TABLE;
import static org.infinispan.tools.store.migrator.Element.TABLE_NAME_PREFIX;
import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.TIMESTAMP;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.Element.USERNAME;
import static org.infinispan.tools.store.migrator.StoreType.JDBC_STRING;
import static org.infinispan.tools.store.migrator.StoreType.SOFT_INDEX_FILE_STORE;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Properties;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreMigrator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 * @since 14.0
 */
@Test(testName = "org.infinispan.tools.store.migrator.marshaller.jdbc.JdbcReaderTest", groups = "functional")
public class JdbcReaderTest extends AbstractInfinispanTest {

   private static final int NUM_ENTRIES = 100;
   private static final String CACHE_NAME = "jdbc-cache";
   private static final String JDBC_URL = "jdbc:h2:mem:JdbcReaderTest;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
   private static final String SIFS_LOCATION = CommonsTestingUtil.tmpDirectory(JdbcReaderTest.class);

   @AfterClass
   public void cleanup() {
      Util.recursiveFileRemove(SIFS_LOCATION);
   }

   @Test
   public void jdbcReaderTest() throws Exception {
      // Initialize h2 JDBC database with entries
      ConfigurationBuilder builder = new ConfigurationBuilder();
      JdbcStringBasedStoreConfigurationBuilder jdbcB = builder.persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      jdbcB.table()
            .createOnStart(true)
            .dropOnExit(false)
            .tableNamePrefix("prefix")
            .idColumnName("ID_COLUMN").idColumnType("VARCHAR(255)")
            .dataColumnName("DATA_COLUMN").dataColumnType("BINARY VARYING")
            .timestampColumnName("TIMESTAMP_COLUMN").timestampColumnType("BIGINT")
            .segmented(false);
      jdbcB.connectionPool()
            .driverClass(org.h2.Driver.class)
            .connectionUrl(JDBC_URL)
            .username("sa");

      try (EmbeddedCacheManager cm = new DefaultCacheManager()) {
         Cache<Integer, Integer> cache = cm.createCache(CACHE_NAME, builder.build());
         IntStream.range(0, NUM_ENTRIES).forEach(i -> cache.put(i, i));
      }

      // Migrate JDBC store to SIFS
      Properties properties = new Properties();
      properties.put(propKey(SOURCE, Element.CACHE_NAME), CACHE_NAME);
      properties.put(propKey(SOURCE, TYPE), JDBC_STRING.toString());
      properties.put(propKey(SOURCE, DIALECT), "h2");
      properties.put(propKey(SOURCE, CONNECTION_POOL, CONNECTION_URL), JDBC_URL);
      properties.put(propKey(SOURCE, CONNECTION_POOL, DRIVER_CLASS), org.h2.Driver.class.getName());
      properties.put(propKey(SOURCE, CONNECTION_POOL, USERNAME), "sa");
      properties.put(propKey(SOURCE, TABLE, STRING, TABLE_NAME_PREFIX), "prefix");
      properties.put(propKey(SOURCE, TABLE, STRING, ID, NAME), "ID_COLUMN");
      properties.put(propKey(SOURCE, TABLE, STRING, ID, TYPE), "VARCHAR(255)");
      properties.put(propKey(SOURCE, TABLE, STRING, DATA, NAME), "DATA_COLUMN");
      properties.put(propKey(SOURCE, TABLE, STRING, DATA, TYPE), "BINARY VARYING");
      properties.put(propKey(SOURCE, TABLE, STRING, SEGMENT, NAME), "SEGMENT_COLUMN");
      properties.put(propKey(SOURCE, TABLE, STRING, SEGMENT, TYPE), "BIGINT");
      properties.put(propKey(SOURCE, TABLE, STRING, TIMESTAMP, NAME), "TIMESTAMP_COLUMN");
      properties.put(propKey(SOURCE, TABLE, STRING, TIMESTAMP, TYPE), "BIGINT");
      properties.put(propKey(SOURCE, SEGMENT_COUNT), "256");

      properties.put(propKey(TARGET, TYPE), SOFT_INDEX_FILE_STORE.toString());
      properties.put(propKey(TARGET, Element.CACHE_NAME), CACHE_NAME);
      properties.put(propKey(TARGET, LOCATION), SIFS_LOCATION);

      new StoreMigrator(properties).run();

      // Assert that SFS contains expected number of entries
      builder = new ConfigurationBuilder();
      builder.persistence().addSoftIndexFileStore().dataLocation(SIFS_LOCATION);
      try (EmbeddedCacheManager cm = new DefaultCacheManager()) {
         Cache<Integer, Integer> cache = cm.createCache(CACHE_NAME, builder.build());
         assertEquals(NUM_ENTRIES, cache.size());
         IntStream.range(0, NUM_ENTRIES).forEach(i -> assertEquals(i, (int) cache.get(i)));
      }
   }
}
