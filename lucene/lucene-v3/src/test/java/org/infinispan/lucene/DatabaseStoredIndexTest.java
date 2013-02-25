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
package org.infinispan.lucene;

import java.io.IOException;
import java.util.HashMap;

import static org.infinispan.lucene.CacheTestSupport.assertTextIsFoundInIds;
import static org.infinispan.lucene.CacheTestSupport.removeByTerm;
import static org.infinispan.lucene.CacheTestSupport.writeTextToIndex;

import org.apache.lucene.store.Directory;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.infinispan.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test to verify that it's possible to use the index using a JdbcStringBasedCacheStore
 * 
 * @see org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@SuppressWarnings("unchecked")
@Test(groups = "functional", testName = "lucene.DatabaseStoredIndexTest")
public class DatabaseStoredIndexTest extends SingleCacheManagerTest {
   
   private final ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
   
   /** The INDEX_NAME */
   private static final String INDEX_NAME = "testing index";
   
   private final HashMap cacheCopy = new HashMap();
   
   public DatabaseStoredIndexTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }
   
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
      loaderManagerConfig.setPreload(Boolean.FALSE);
      loaderManagerConfig.addCacheLoaderConfig(jdbcStoreConfiguration);
   }

   @Test
   public void testIndexUsage() throws IOException {
      cache = cacheManager.getCache();
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).create();
      writeTextToIndex(dir, 0, "hello database");
      assertTextIsFoundInIds(dir, "hello", 0);
      writeTextToIndex(dir, 1, "you have to store my index segments");
      writeTextToIndex(dir, 2, "so that I can shut down all nodes");
      writeTextToIndex(dir, 3, "and restart later keeping the index around");
      assertTextIsFoundInIds(dir, "index", 1, 3);
      removeByTerm(dir, "and");
      assertTextIsFoundInIds(dir, "index", 1);
      dir.close();
      cacheCopy.putAll(cache);
      cache.stop();
      cacheManager.stop();
   }
   
   @Test(dependsOnMethods="testIndexUsage")
   public void indexWasStored() throws IOException {
      cache = cacheManager.getCache();
      assert cache.isEmpty();
      boolean failed = false;
      for (Object key : cacheCopy.keySet()) {
         if (key instanceof FileReadLockKey) {
            System.out.println("Key found in store, shouldn't have persisted this or should have cleaned up all readlocks on directory close:" + key);
            failed = true;
         }
         else {
            Object expected = cacheCopy.get(key);
            Object actual = cache.get(key);
            if (expected==null && actual==null)
               continue;
            if (expected instanceof byte[]) {
               expected = Util.printArray((byte[]) expected, false);
               actual = Util.printArray((byte[]) actual, false);
            }
            if (expected == null || ! expected.equals(actual)) {
               System.out.println("Failure on key["+key.toString()+"] expected value:\n\t"+expected+"\tactual value:\n\t"+actual);
               failed = true;
            }
         }
      }
      Assert.assertFalse(failed);
      Assert.assertEquals(cacheCopy.keySet().size(), cache.keySet().size(), "have a different number of keys");
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).create();
      assertTextIsFoundInIds(dir, "index", 1);
      dir.close();
   }
   
}
