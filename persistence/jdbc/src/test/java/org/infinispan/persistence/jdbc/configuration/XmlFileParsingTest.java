package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

@Test(groups = "unit", testName = "persistence.jdbc.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testStringKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "     <persistence>\n" +
            "       <stringKeyedJdbcStore xmlns=\"urn:infinispan:config:jdbc:6.0\" key2StringMapper=\"DummyKey2StringMapper\">\n" +
            "         <connectionPool connectionUrl=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" driverClass=\"org.h2.Driver\"/>\n" +
            "         <stringKeyedTable prefix=\"entry\" fetchSize=\"34\" batchSize=\"128\" >\n" +
            "           <idColumn name=\"id\" type=\"VARCHAR\" />\n" +
            "           <dataColumn name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestampColumn name=\"version\" type=\"BIGINT\" />\n" +
            "         </stringKeyedTable>\n" +
            "         <async enabled=\"true\" />\n" +
            "       </stringKeyedJdbcStore>\n" +
            "     </persistence>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      JdbcStringBasedStoreConfiguration store = (JdbcStringBasedStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assertEquals(128, store.table().batchSize());
      assertEquals(34, store.table().fetchSize());
      assertEquals("BINARY", store.table().dataColumnType());
      assertEquals("version", store.table().timestampColumnName());
      assertTrue(store.async().enabled());
      assertEquals("DummyKey2StringMapper", store.key2StringMapper());
      PooledConnectionFactoryConfiguration connectionFactory = (PooledConnectionFactoryConfiguration) store.connectionFactory();
      assertEquals("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1", connectionFactory.connectionUrl());
      assertEquals("org.h2.Driver", connectionFactory.driverClass());
      assertEquals("dbuser", connectionFactory.username());
      assertEquals("dbpass", connectionFactory.password());
   }

   public void testBinaryKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "     <persistence>\n" +
            "       <binaryKeyedJdbcStore xmlns=\"urn:infinispan:config:jdbc:6.0\" ignoreModifications=\"true\">\n" +
            "         <simpleConnection connectionUrl=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" driverClass=\"org.h2.Driver\"/>\n" +
            "         <binaryKeyedTable prefix=\"bucket\" fetchSize=\"34\" batchSize=\"128\">\n" +
            "           <idColumn name=\"id\" type=\"BINARY\" />\n" +
            "           <dataColumn name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestampColumn name=\"version\" type=\"BIGINT\" />\n" +
            "         </binaryKeyedTable>\n" +
            "         <singleton enabled=\"true\" />\n" +
            "       </binaryKeyedJdbcStore>\n" +
            "     </persistence>\n" +
            "   </default>\n" +
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

   public void testMixedKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "     <persistence>\n" +
            "       <mixedKeyedJdbcStore xmlns=\"urn:infinispan:config:jdbc:6.0\" key2StringMapper=\"DummyKey2StringMapper\">\n" +
            "         <dataSource jndiUrl=\"java:MyDataSource\" />\n" +
            "         <stringKeyedTable prefix=\"entry\" fetchSize=\"34\" batchSize=\"128\">\n" +
            "           <idColumn name=\"id\" type=\"VARCHAR\" />\n" +
            "           <dataColumn name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestampColumn name=\"version\" type=\"BIGINT\" />\n" +
            "         </stringKeyedTable>\n" +
            "         <binaryKeyedTable prefix=\"bucket\" fetchSize=\"44\" batchSize=\"256\">\n" +
            "           <idColumn name=\"id\" type=\"BINARY\" />\n" +
            "           <dataColumn name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestampColumn name=\"version\" type=\"BIGINT\" />\n" +
            "         </binaryKeyedTable>\n" +
            "         <async enabled=\"true\" />\n" +
            "         <singleton enabled=\"true\" />\n" +
            "       </mixedKeyedJdbcStore>\n" +
            "     </persistence>\n" +
            "   </default>\n" +
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