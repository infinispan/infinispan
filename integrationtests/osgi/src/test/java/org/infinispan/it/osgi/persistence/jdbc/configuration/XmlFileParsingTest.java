package org.infinispan.it.osgi.persistence.jdbc.configuration;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.it.osgi.Osgi;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.SimpleConnectionFactoryConfiguration;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.infinispan.it.osgi.util.IspnKarafOptions.*;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.junit.Assert.*;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;

/**
 * This test is a duplicate of {@link org.infinispan.persistence.jdbc.configuration.XmlFileParsingTest}.
 * We can't just extend the aforementioned test because there's a split-package (duplicated packages)
 * between the main JDBC cache store jar and its tests. Packages from one of the jars are not visible to PAX EXAM.
 *
 * @author mgencur
 */
@RunWith(PaxExam.class)
@Category(Osgi.class)
@ExamReactorStrategy(PerClass.class)
public class XmlFileParsingTest {

   private EmbeddedCacheManager cacheManager;

   @Configuration
   public Option[] config() throws Exception {
      return options(
            karafContainer(),
            featureIspnCoreDependencies(),
            featureIspnCorePlusTests(),
            featureJdbcStorePooled(),
            junitBundles(),
            keepRuntimeFolder()
      );
   }

   @After
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   @Test
   public void testStringKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <cache-container default-cache=\"default\">\n" +
            "      <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <string-keyed-jdbc-store xmlns=\"urn:infinispan:config:store:jdbc:7.0\" key-to-string-mapper=\"DummyKey2StringMapper\" shared=\"true\" " +
            "                                preload=\"true\" read-only=\"true\" fetch-state=\"true\" purge=\"true\" singleton=\"false\" >\n" +
            "         <connection-pool connection-url=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" driver=\"org.h2.Driver\"/>\n" +
            "         <string-keyed-table prefix=\"entry\" fetch-size=\"34\" batch-size=\"128\" >\n" +
            "           <id-column name=\"id\" type=\"VARCHAR\" />\n" +
            "           <data-column name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestamp-column name=\"version\" type=\"BIGINT\" />\n" +
            "         </string-keyed-table>\n" +
            "         <write-behind />\n" +
            "       </string-keyed-jdbc-store>\n" +
            "     </persistence>\n" +
            "   </local-cache></cache-container>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      JdbcStringBasedStoreConfiguration store = (JdbcStringBasedStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assertEquals(128, store.table().batchSize());
      assertEquals(34, store.table().fetchSize());
      assertEquals("BINARY", store.table().dataColumnType());
      assertEquals("version", store.table().timestampColumnName());
      assertTrue(store.async().enabled());
      assertEquals("DummyKey2StringMapper", store.key2StringMapper());
      assertTrue(store.shared());
      assertTrue(store.preload());
      PooledConnectionFactoryConfiguration connectionFactory = (PooledConnectionFactoryConfiguration) store.connectionFactory();
      assertEquals("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1", connectionFactory.connectionUrl());
      assertEquals("org.h2.Driver", connectionFactory.driverClass());
      assertEquals("dbuser", connectionFactory.username());
      assertEquals("dbpass", connectionFactory.password());
      assertTrue(store.ignoreModifications());
      assertTrue(store.fetchPersistentState());
      assertTrue(store.purgeOnStartup());
      assertFalse(store.singletonStore().enabled());
   }

   @Test
   public void testBinaryKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <cache-container default-cache=\"default\">\n" +
            "      <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <binary-keyed-jdbc-store xmlns=\"urn:infinispan:config:store:jdbc:7.0\" read-only=\"true\" singleton=\"true\">\n" +
            "         <simple-connection connection-url=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" driver=\"org.h2.Driver\"/>\n" +
            "         <binary-keyed-table prefix=\"bucket\" fetch-size=\"34\" batch-size=\"128\">\n" +
            "           <id-column name=\"id\" type=\"BINARY\" />\n" +
            "           <data-column name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestamp-column name=\"version\" type=\"BIGINT\" />\n" +
            "         </binary-keyed-table>\n" +
            "       </binary-keyed-jdbc-store>\n" +
            "     </persistence>\n" +
            "   </local-cache></cache-container>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      JdbcBinaryStoreConfiguration store = (JdbcBinaryStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assertTrue(store.ignoreModifications());
      assertEquals("bucket", store.table().tableNamePrefix());
      assertEquals(128, store.table().batchSize());
      assertEquals(34, store.table().fetchSize());
      assertEquals("BINARY", store.table().dataColumnType());
      assertEquals("version", store.table().timestampColumnName());
      assertTrue(store.singletonStore().enabled());
      SimpleConnectionFactoryConfiguration connectionFactory = (SimpleConnectionFactoryConfiguration) store.connectionFactory();
      assertEquals("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1", connectionFactory.connectionUrl());
      assertEquals("org.h2.Driver", connectionFactory.driverClass());
      assertEquals("dbuser", connectionFactory.username());
      assertEquals("dbpass", connectionFactory.password());
   }

   @Test
   public void testMixedKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <cache-container default-cache=\"default\">\n" +
            "      <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <mixed-keyed-jdbc-store xmlns=\"urn:infinispan:config:store:jdbc:7.0\" key-to-string-mapper=\"DummyKey2StringMapper\" singleton=\"true\" >\n" +
            "         <data-source jndi-url=\"java:MyDataSource\" />\n" +
            "         <string-keyed-table prefix=\"entry\" fetch-size=\"34\" batch-size=\"128\">\n" +
            "           <id-column name=\"id\" type=\"VARCHAR\" />\n" +
            "           <data-column name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestamp-column name=\"version\" type=\"BIGINT\" />\n" +
            "         </string-keyed-table>\n" +
            "         <binary-keyed-table prefix=\"bucket\" fetch-size=\"44\" batch-size=\"256\">\n" +
            "           <id-column name=\"id\" type=\"BINARY\" />\n" +
            "           <data-column name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestamp-column name=\"version\" type=\"BIGINT\" />\n" +
            "         </binary-keyed-table>\n" +
            "         <write-behind />\n" +
            "       </mixed-keyed-jdbc-store>\n" +
            "     </persistence>\n" +
            "   </local-cache></cache-container>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      JdbcMixedStoreConfiguration store = (JdbcMixedStoreConfiguration) buildCacheManagerWithCacheStore(config);

      assertEquals("entry", store.stringTable().tableNamePrefix());
      assertEquals(128, store.stringTable().batchSize());
      assertEquals(34, store.stringTable().fetchSize());
      assertEquals("BINARY", store.stringTable().dataColumnType());
      assertEquals("version", store.stringTable().timestampColumnName());

      assertEquals("bucket", store.binaryTable().tableNamePrefix());
      assertEquals(256, store.binaryTable().batchSize());
      assertEquals(44, store.binaryTable().fetchSize());
      assertEquals("BINARY", store.binaryTable().dataColumnType());
      assertEquals("version", store.binaryTable().timestampColumnName());

      assertTrue(store.async().enabled());
      assertTrue(store.singletonStore().enabled());
      assertEquals("DummyKey2StringMapper", store.key2StringMapper());
   }

   private StoreConfiguration buildCacheManagerWithCacheStore(final String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assertEquals(1, cacheManager.getDefaultCacheConfiguration().persistence().stores().size());
      return cacheManager.getDefaultCacheConfiguration().persistence().stores().get(0);
   }
}
