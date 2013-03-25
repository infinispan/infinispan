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
package org.infinispan.lucene.readlocks;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.infinispan.Cache;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.testng.annotations.Test;

/**
 * LocalLockMergingSegmentReadLockerTest represents a quick check on the functionality
 * of {@link org.infinispan.lucene.readlocks.LocalLockMergingSegmentReadLocker}
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@SuppressWarnings("unchecked")
@Test(groups = "functional", testName = "lucene.readlocks.LocalLockMergingSegmentReadLockerTest")
public class LocalLockMergingSegmentReadLockerTest extends DistributedSegmentReadLockerTest {
   
   @Test @Override
   public void testIndexWritingAndFinding() throws IOException, InterruptedException {
      prepareEnvironment(false);

      verifyBoth(cache0,cache1);
      IndexOutput indexOutput = dirA.createOutput(filename, IOContext.DEFAULT);
      indexOutput.writeString("no need to write, nobody ever will read this");
      indexOutput.flush();
      indexOutput.close();
      assertFileExistsHavingRLCount(filename, 1, true);
      IndexInput firstOpenOnB = dirB.openInput(filename, IOContext.DEFAULT);
      assertFileExistsHavingRLCount(filename, 2, true);
      dirA.deleteFile(filename);
      assertFileExistsHavingRLCount(filename, 1, false);
      //Lucene does use clone() - lock implementation ignores it as a clone is
      //cast on locked segments and released before the close on the parent object
      IndexInput cloneOfFirstOpenOnB = (IndexInput) firstOpenOnB.clone();
      assertFileExistsHavingRLCount(filename, 1, false);
      cloneOfFirstOpenOnB.close();
      assertFileExistsHavingRLCount(filename, 1, false);
      IndexInput firstOpenOnA = dirA.openInput(filename, IOContext.DEFAULT);
      assertFileExistsHavingRLCount(filename, 2, false);
      IndexInput secondOpenOnA = dirA.openInput(filename, IOContext.DEFAULT);
      assertFileExistsHavingRLCount(filename, 2, false);
      firstOpenOnA.close();
      assertFileExistsHavingRLCount(filename, 2, false);
      secondOpenOnA.close();
      assertFileExistsHavingRLCount(filename, 1, false);
      firstOpenOnB.close();
      assertFileNotExists(filename);
      dirA.close();
      dirB.close();
      verifyBoth(cache0, cache1);
   }

   @Test
   public void testAdditionalIndexWritingAndFinding() throws IOException, InterruptedException {
      prepareEnvironment(true);

      testIndexWritingAndFinding();
   }

   @Override
   Directory createDirectory(Cache cache) {
      return DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME)
            .chunkSize(CHUNK_SIZE)
            .overrideSegmentReadLocker(new LocalLockMergingSegmentReadLocker(cache, INDEX_NAME))
            .create();
   }

   Directory createAdditionalDirectory(Cache cache) {
      return DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME)
            .chunkSize(CHUNK_SIZE)
            .overrideSegmentReadLocker(new LocalLockMergingSegmentReadLocker(cache, cache, cache, INDEX_NAME))
            .create();
   }

   private void prepareEnvironment(final boolean useDefConstructor) throws IOException {
      cache0 = cache(0, CACHE_NAME);
      cache1 = cache(1, CACHE_NAME);

      if (useDefConstructor) {
         dirA = createDirectory(cache0);
         dirB = createDirectory(cache1);
      } else {
         dirA = createAdditionalDirectory(cache0);
         dirB = createAdditionalDirectory(cache1);
      }

      CacheTestSupport.initializeDirectory(dirA);
   }

}
