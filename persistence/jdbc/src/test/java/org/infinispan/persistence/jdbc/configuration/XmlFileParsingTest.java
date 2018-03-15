package org.infinispan.persistence.jdbc.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.infinispan.Version;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.jdbc.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   protected EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testStringKeyedJdbcStore() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "   <cache-container default-cache=\"default\">\n" +
            "      <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <string-keyed-jdbc-store xmlns=\"urn:infinispan:config:store:jdbc:"+ Version.getSchemaVersion() + "\" key-to-string-mapper=\"DummyKey2StringMapper\" shared=\"true\" " +
            "                                preload=\"true\" read-only=\"true\" fetch-state=\"true\" purge=\"true\" singleton=\"false\" dialect=\"H2\">\n" +
            "         <connection-pool connection-url=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" driver=\"org.h2.Driver\"/>\n" +
            "         <string-keyed-table prefix=\"entry\" fetch-size=\"34\" batch-size=\"128\" >\n" +
            "           <id-column name=\"id\" type=\"VARCHAR\" />\n" +
            "           <data-column name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestamp-column name=\"version\" type=\"BIGINT\" />\n" +
            "         </string-keyed-table>\n" +
            "         <write-behind />\n" +
            "       </string-keyed-jdbc-store>\n" +
            "     </persistence>\n" +
            "   </local-cache></cache-container>\n"
      );

      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assertEquals(1, cacheManager.getDefaultCacheConfiguration().persistence().stores().size());
      JdbcStringBasedStoreConfiguration store = (JdbcStringBasedStoreConfiguration) cacheManager.getDefaultCacheConfiguration().persistence().stores().get(0);

      assertEquals(128, store.table().batchSize());
      assertEquals(34, store.table().fetchSize());
      assertEquals("BINARY", store.table().dataColumnType());
      assertEquals("version", store.table().timestampColumnName());
      assertTrue(store.async().enabled());
      assertEquals("DummyKey2StringMapper", store.key2StringMapper());
      assertTrue(store.shared());
      assertTrue(store.preload());
      assertEquals(DatabaseType.H2, store.dialect());
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
}
