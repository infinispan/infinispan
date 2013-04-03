/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.lucene.impl;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Tests covering DirecotoryImplementor class.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "lucene.DirectoryImplementerTests")
public class DirectoryImplementerTests extends SingleCacheManagerTest {

   private static final String INDEX_NAME = "index-A";

   private static final int BUFFER_SIZE = 1024;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configuration = CacheTestSupport.createLocalCacheConfiguration();

      return TestCacheManagerFactory.createCacheManager(configuration);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "chunkSize must be a positive integer")
   public void testInitWithInvalidChunkSize() throws IOException {
      Directory dir = null;
      try {
         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).chunkSize(0).create();
      } finally {
         if (dir != null) dir.close();
      }
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testFailureOfOverrideWriteLocker() throws IOException {
      Directory dir = null;
      try {
         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).chunkSize(BUFFER_SIZE)
               .overrideWriteLocker(null)
               .create();
      } finally {
         if (dir != null) dir.close();
      }

   }

   public void testOverrideWriteLocker() throws IOException {
      Directory dir = null;
      try {
         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).chunkSize(BUFFER_SIZE)
               .overrideWriteLocker(new LockFactory() {
                  @Override
                  public Lock makeLock(String lockName) {
                     return null;
                  }

                  @Override
                  public void clearLock(String lockName) throws IOException {

                  }
               })
               .create();

         AssertJUnit.assertEquals(0, dir.listAll().length);
      } finally {
         if (dir != null) dir.close();
      }
   }

   @Test
   public void testGetIndexNameAndToString() throws IOException {
      Cache cache = cacheManager.getCache();
      Directory dir = null;

      try {

         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).chunkSize(BUFFER_SIZE).create();
         AssertJUnit.assertEquals(INDEX_NAME, ((DirectoryLuceneV4) dir).getIndexName());
         AssertJUnit.assertEquals("InfinispanDirectory{indexName=\'" + INDEX_NAME + "\'}", dir.toString());

      } finally {
         if(dir != null) dir.close();
      }
   }
}
