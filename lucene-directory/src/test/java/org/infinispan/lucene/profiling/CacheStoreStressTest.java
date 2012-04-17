/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.lucene.profiling;

import java.io.IOException;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.DirectoryIntegrityCheck;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.lucene.LuceneKey2StringMapper;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * Testcase verifying that the index is usable under stress even when a cachestore is configured.
 * See ISPN-575 (Corruption in data when using a permanent store)
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@Test(groups = "profiling", testName = "lucene.profiling.CacheStoreStressTest", sequential = true)
public class CacheStoreStressTest extends SingleCacheManagerTest {
   
   private final ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();

   private static final String indexName = "tempIndexName";
   
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration configuration = CacheTestSupport.createLegacyTestConfiguration();
      enableTestJdbcStorage(configuration);
      return TestCacheManagerFactory.createClusteredCacheManager(configuration);
   }
   
   private void enableTestJdbcStorage(Configuration configuration) {
      TableManipulation tm = UnitTestDatabaseManager.buildStringTableManipulation();
      JdbcStringBasedCacheStoreConfig jdbcStoreConfiguration = new JdbcStringBasedCacheStoreConfig(connectionFactoryConfig, tm);
      jdbcStoreConfiguration.setKey2StringMapperClass(LuceneKey2StringMapper.class.getName());
      CacheLoaderManagerConfig loaderManagerConfig = configuration.getCacheLoaderManagerConfig();
      loaderManagerConfig.setPreload(true);
      loaderManagerConfig.addCacheLoaderConfig(jdbcStoreConfiguration);
   }

   @Test
   public void stressTestOnStore() throws InterruptedException, IOException {
      cache = cacheManager.getCache();
      assert cache!=null;
      InfinispanDirectory dir = new InfinispanDirectory(cache, indexName);
      PerformanceCompareStressTest.stressTestDirectory(dir, "InfinispanClusteredWith-Store");
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, indexName, true);
   }
   
}
