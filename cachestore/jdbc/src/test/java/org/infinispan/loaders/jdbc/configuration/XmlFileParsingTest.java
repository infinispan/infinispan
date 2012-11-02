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

@Test(groups = "unit", testName = "loaders.jdbc.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod(alwaysRun = true)
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testStringKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "     <loaders>\n" +
            "       <stringKeyedJdbcStore xmlns=\"urn:infinispan:config:jdbc:5.2\" >\n" +
            "         <connectionPool connectionUrl=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" />\n" +
            "         <stringKeyedTable prefix=\"entry\" fetchSize=\"34\" batchSize=\"99\">\n" +
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
      assert store.table().batchSize() == 99;
      assert store.table().fetchSize() == 34;
      assert store.table().dataColumnType().equals("BINARY");
      assert store.table().timestampColumnName().equals("version");
      assert store.async().enabled();
   }

   public void testBinaryKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "     <loaders>\n" +
            "       <binaryKeyedJdbcStore xmlns=\"urn:infinispan:config:jdbc:5.2\" ignoreModifications=\"true\">\n" +
            "         <simpleConnection connectionUrl=\"jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1\" username=\"dbuser\" password=\"dbpass\" />\n" +
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
      assert store.ignoreModifications();
      assert store.table().tableNamePrefix().equals("bucket");
      assert store.table().batchSize() == 99;
      assert store.table().fetchSize() == 34;
      assert store.table().dataColumnType().equals("BINARY");
      assert store.table().timestampColumnName().equals("version");
      assert store.singletonStore().enabled();

   }

   public void testMixedKeyedJdbcStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "     <loaders>\n" +
            "       <mixedKeyedJdbcStore xmlns=\"urn:infinispan:config:jdbc:5.2\" >\n" +
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

      assert store.stringTable().tableNamePrefix().equals("entry");
      assert store.stringTable().batchSize() == 99;
      assert store.stringTable().fetchSize() == 34;
      assert store.stringTable().dataColumnType().equals("BINARY");
      assert store.stringTable().timestampColumnName().equals("version");

      assert store.binaryTable().tableNamePrefix().equals("bucket");
      assert store.binaryTable().batchSize() == 79;
      assert store.binaryTable().fetchSize() == 44;
      assert store.binaryTable().dataColumnType().equals("BINARY");
      assert store.binaryTable().timestampColumnName().equals("version");

      assert store.async().enabled();
      assert store.singletonStore().enabled();
   }

   private CacheLoaderConfiguration buildCacheManagerWithCacheStore(final String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assert cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders().size() == 1;
      return cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders().get(0);
   }
}