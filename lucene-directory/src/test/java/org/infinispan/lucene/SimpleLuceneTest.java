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
package org.infinispan.lucene;

import static org.infinispan.lucene.CacheTestSupport.assertTextIsFoundInIds;
import static org.infinispan.lucene.CacheTestSupport.removeByTerm;
import static org.infinispan.lucene.CacheTestSupport.writeTextToIndex;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * SimpleLuceneTest tests the basic functionality of the Lucene Directory
 * on Infinispan: what is inserted in one node should be able to be found in
 * a second node.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "functional", testName = "lucene.SimpleLuceneTest")
@SuppressWarnings("unchecked")
public class SimpleLuceneTest extends MultipleCacheManagersTest {
   
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createTestConfiguration();
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
