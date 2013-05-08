/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.jdbc.configuration;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

@Test(groups = "unit", testName = "loaders.jdbc.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testStringKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "     <loaders>\n" +
            "       <stringKeyedJdbcStore xmlns=\"urn:infinispan:config:jdbc:5.3\" key2StringMapper=\"org.infinispan.loaders.jdbc.configuration.DummyKey2StringMapper\">\n" +
            "         <connectionPool connectionUrl=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" driverClass=\"org.h2.Driver\"/>\n" +
            "         <stringKeyedTable prefix=\"entry\" fetchSize=\"34\" batchSize=\"99\" >\n" +
            "           <idColumn name=\"id\" type=\"VARCHAR\" />\n" +
            "           <dataColumn name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestampColumn name=\"version\" type=\"BIGINT\" />\n" +
            "         </stringKeyedTable>\n" +
            "         <async enabled=\"true\" />\n" +
            "       </stringKeyedJdbcStore>\n" +
            "     </loaders>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      JdbcStringBasedCacheStoreConfiguration store = (JdbcStringBasedCacheStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assertEquals(99, store.table().batchSize());
      assertEquals(34, store.table().fetchSize());
      assertEquals("BINARY", store.table().dataColumnType());
      assertEquals("version", store.table().timestampColumnName());
      assertTrue(store.async().enabled());
      assertEquals("org.infinispan.loaders.jdbc.configuration.DummyKey2StringMapper", store.key2StringMapper());
      PooledConnectionFactoryConfiguration connectionFactory = (PooledConnectionFactoryConfiguration) store.connectionFactory();
      assertEquals("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1", connectionFactory.connectionUrl());
      assertEquals("org.h2.Driver", connectionFactory.driverClass());
      assertEquals("dbuser", connectionFactory.username());
      assertEquals("dbpass", connectionFactory.password());
   }

   public void testBinaryKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "     <loaders>\n" +
            "       <binaryKeyedJdbcStore xmlns=\"urn:infinispan:config:jdbc:5.2\" ignoreModifications=\"true\">\n" +
            "         <simpleConnection connectionUrl=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" driverClass=\"org.h2.Driver\"/>\n" +
            "         <binaryKeyedTable prefix=\"bucket\" fetchSize=\"34\" batchSize=\"99\">\n" +
            "           <idColumn name=\"id\" type=\"BINARY\" />\n" +
            "           <dataColumn name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestampColumn name=\"version\" type=\"BIGINT\" />\n" +
            "         </binaryKeyedTable>\n" +
            "         <singletonStore enabled=\"true\" />\n" +
            "       </binaryKeyedJdbcStore>\n" +
            "     </loaders>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      JdbcBinaryCacheStoreConfiguration store = (JdbcBinaryCacheStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assertTrue(store.ignoreModifications());
      assertEquals("bucket", store.table().tableNamePrefix());
      assertEquals(99, store.table().batchSize());
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
            "     <loaders>\n" +
            "       <mixedKeyedJdbcStore xmlns=\"urn:infinispan:config:jdbc:5.2\" key2StringMapper=\"org.infinispan.loaders.jdbc.configuration.DummyKey2StringMapper\">\n" +
            "         <dataSource jndiUrl=\"java:MyDataSource\" />\n" +
            "         <stringKeyedTable prefix=\"entry\" fetchSize=\"34\" batchSize=\"99\">\n" +
            "           <idColumn name=\"id\" type=\"VARCHAR\" />\n" +
            "           <dataColumn name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestampColumn name=\"version\" type=\"BIGINT\" />\n" +
            "         </stringKeyedTable>\n" +
            "         <binaryKeyedTable prefix=\"bucket\" fetchSize=\"44\" batchSize=\"79\">\n" +
            "           <idColumn name=\"id\" type=\"BINARY\" />\n" +
            "           <dataColumn name=\"datum\" type=\"BINARY\" />\n" +
            "           <timestampColumn name=\"version\" type=\"BIGINT\" />\n" +
            "         </binaryKeyedTable>\n" +
            "         <async enabled=\"true\" />\n" +
            "         <singletonStore enabled=\"true\" />\n" +
            "       </mixedKeyedJdbcStore>\n" +
            "     </loaders>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      JdbcMixedCacheStoreConfiguration store = (JdbcMixedCacheStoreConfiguration) buildCacheManagerWithCacheStore(config);

      assertEquals("entry", store.stringTable().tableNamePrefix());
      assertEquals(99, store.stringTable().batchSize());
      assertEquals(34, store.stringTable().fetchSize());
      assertEquals("BINARY", store.stringTable().dataColumnType());
      assertEquals("version", store.stringTable().timestampColumnName());

      assertEquals("bucket", store.binaryTable().tableNamePrefix());
      assertEquals(79, store.binaryTable().batchSize());
      assertEquals(44, store.binaryTable().fetchSize());
      assertEquals("BINARY", store.binaryTable().dataColumnType());
      assertEquals("version", store.binaryTable().timestampColumnName());

      assertTrue(store.async().enabled());
      assertTrue(store.singletonStore().enabled());
      assertEquals("org.infinispan.loaders.jdbc.configuration.DummyKey2StringMapper", store.key2StringMapper());
   }

   private CacheLoaderConfiguration buildCacheManagerWithCacheStore(final String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assertEquals(1, cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders().size());
      return cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders().get(0);
   }
}