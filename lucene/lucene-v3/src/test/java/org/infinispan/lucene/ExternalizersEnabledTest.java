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

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.jboss.ExternalizerTable;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.marshalling.ObjectTable.Writer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Verifies the externalizers of the lucene-directory module are registered automatically
 * 
 * @author Sanne Grinovero
 * @since 5.0
 */
@Test(groups = "functional", testName = "lucene.ExternalizersEnabledTest")
public class ExternalizersEnabledTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createLocalCacheConfiguration();
      return TestCacheManagerFactory.createCacheManager(configurationBuilder);
   }

   @Test
   public void testChunkCacheKeyExternalizer() throws IOException {
      ChunkCacheKey key = new ChunkCacheKey("myIndex", "filename", 5, 1000);
      verifyExternalizerForType(key, ChunkCacheKey.Externalizer.class);
   }

   @Test
   public void testFileCacheKeyExternalizer() throws IOException {
      FileCacheKey key = new FileCacheKey("myIndex", "fileA.idx");
      verifyExternalizerForType(key, FileCacheKey.Externalizer.class);
   }

   @Test
   public void testFileListCacheKeyExternalizer() throws IOException {
      FileListCacheKey key = new FileListCacheKey("myIndex");
      verifyExternalizerForType(key, FileListCacheKey.Externalizer.class);
   }

   @Test
   public void testFileMetadataExternalizer() throws IOException {
      FileMetadata key = new FileMetadata(23);
      verifyExternalizerForType(key, FileMetadata.Externalizer.class);
   }

   @Test
   public void testFileReadLockKeyExternalizer() throws IOException {
      FileReadLockKey key = new FileReadLockKey("myIndex", "index.lock");
      verifyExternalizerForType(key, FileReadLockKey.Externalizer.class);
   }

   private void verifyExternalizerForType(Object keySampleType, Class expectedExcternalizerClass) throws IOException {
      ExternalizerTable externalizerTable = TestingUtil.extractExtTable(cacheManager);

      Writer objectWriter = externalizerTable.getObjectWriter(keySampleType);

      AssertJUnit.assertEquals("Registered Externalizers should be wrapped by a ForeignExternalizerAdapter",
            objectWriter.getClass().toString(),
            "class org.infinispan.marshall.jboss.ExternalizerTable$ForeignExternalizerAdapter");

      AssertJUnit.assertEquals("Type of delegate used by the adapter doesn't match expected: " + expectedExcternalizerClass.getClass(),
            "class " + objectWriter.toString(),
            expectedExcternalizerClass.toString());
   }

}
