/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.it.lucenedirectoryquery;

import static org.infinispan.lucene.CacheTestSupport.assertTextIsFoundInIds;
import static org.infinispan.lucene.CacheTestSupport.removeByTerm;
import static org.infinispan.lucene.CacheTestSupport.writeTextToIndex;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.DirectoryIntegrityCheck;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Verifies it's possible to store an index in an indexed Cache.
 * I don't think this should be encouraged, but should at least be handled appropriately.
 * 
 * @author Sanne Grinovero
 * @since 5.2
 */
@Test(groups = "functional", testName = "lucenedirectoryquery.IndexStoredIndexedCache")
public class IndexStoredIndexedCache extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      configurationBuilder
         .clustering()
         .cacheMode(CacheMode.DIST_SYNC)
         .stateTransfer()
            .fetchInMemoryState(true)
         .l1()
            .enable()
            .enableOnRehash()
         .sync()
            .replTimeout(10000)
      .transaction()
         .transactionMode(TransactionMode.TRANSACTIONAL)
      .locking()
         .lockAcquisitionTimeout(10000)
      .invocationBatching()
         .disable()
      .deadlockDetection()
         .disable()
      .jmxStatistics()
         .disable()
      .indexing()
         .enable()
            .indexLocalOnly(false)
            .addProperty("hibernate.search.default.directory_provider", "ram")
            .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(2, "lucene", configurationBuilder);
   }

   @Test
   public void testIndexWritingAndFinding() throws IOException {
      final String indexName = "indexName";
      final Cache cache0 = cache(0, "lucene");
      final Cache cache1 = cache(1, "lucene");
      Directory dirA = new InfinispanDirectory(cache0, indexName);
      Directory dirB = new InfinispanDirectory(cache1, indexName);
      writeTextToIndex(dirA, 0, "hi from node A");
      assertTextIsFoundInIds(dirA, "hi", 0);
      assertTextIsFoundInIds(dirB, "hi", 0);
      writeTextToIndex(dirB, 1, "hello node A, how are you?");
      assertTextIsFoundInIds(dirA, "hello", 1);
      assertTextIsFoundInIds(dirB, "hello", 1);
      assertTextIsFoundInIds(dirA, "node", 1, 0); // node is keyword in both documents id=0 and id=1
      assertTextIsFoundInIds(dirB, "node", 1, 0);
      removeByTerm(dirA, "from");
      assertTextIsFoundInIds(dirB, "node", 1);
      dirA.close();
      dirB.close();
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache0, "indexName");
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache1, "indexName");
   }

   @Test(description="Verifies the caches can be reused after a Directory close")
   public void testCacheReuse() throws IOException {
      testIndexWritingAndFinding();
      cache(0, "lucene").clear();
      testIndexWritingAndFinding();
   }

}
