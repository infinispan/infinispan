/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import static org.infinispan.lucene.CacheTestSupport.writeTextToIndex;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.lucene.store.Directory;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * The chunk size might be changed after a segment has been created.
 * This test verifies an existing index is still readable.
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@Test(groups = "functional", testName = "lucene.DynamicBufferSizeTest")
public class DynamicBufferSizeTest extends SingleCacheManagerTest {
   
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration configuration = new Configuration();
      configuration.setCacheMode(Configuration.CacheMode.LOCAL);
      configuration.setInvocationBatchingEnabled(true);
      return TestCacheManagerFactory.createCacheManager(configuration);
   }
   
   @Test
   public void roundingTest() {
      FileMetadata m = new FileMetadata();
      m.setBufferSize(10);
      m.setSize(10);
      Assert.assertEquals(1, m.getNumberOfChunks());
      m.setSize(11);
      Assert.assertEquals(2, m.getNumberOfChunks());
      m.setBufferSize(11);
      Assert.assertEquals(1, m.getNumberOfChunks());
      m.setSize(22);
      Assert.assertEquals(2, m.getNumberOfChunks());
   }
   
   @Test
   @SuppressWarnings("unchecked")
   public void testReadingFromDifferentlySizedBuffers() throws IOException {
      cache = cacheManager.getCache();
      Directory dirA = new InfinispanDirectory(cache, "indexName", 7);
      writeTextToIndex(dirA, 0, "hi from node A");
      Directory dirB = new InfinispanDirectory(cache, "indexName", 8);
      assertTextIsFoundInIds(dirB, "hi", 0);
      writeTextToIndex(dirB, 1, "index B is sharing the same index but using a differently sized chunk size");
      assertTextIsFoundInIds(dirA, "size", 1);
   }

}
