package org.infinispan.lucene.readlocks;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.DirectoryIntegrityCheck;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.lucene.impl.FileListCacheValue;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests covering the functionality of NoopSegmentreadLocker.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "lucene.readlocks.NoopSegmentReadLockerTest")
public class NoopSegmentReadLockerTest extends DistributedSegmentReadLockerTest {

   @Override
   Directory createDirectory(Cache cache) {
      return DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME)
            .chunkSize(CHUNK_SIZE)
            .overrideSegmentReadLocker(new NoopSegmentReadLocker()).create();
   }

   @Test @Override
   public void testIndexWritingAndFinding() throws IOException, InterruptedException {
      verifyIgnoringFiles(cache0, cache1, Util.asSet("pending_segments_1"));
      IndexOutput indexOutput = dirA.createOutput(filename, IOContext.DEFAULT);
      indexOutput.writeString("no need to write, nobody ever will read this");
      indexOutput.close();
      assertFileExistsHavingRLCount(filename, 0, true);
      IndexInput firstOpenOnB = dirB.openInput(filename, IOContext.DEFAULT);
      assertFileExistsHavingRLCount(filename, 0, true);
      dirA.deleteFile(filename);
      assertFileExistsHavingRLCount(filename, 0, false);
      //Lucene does use clone() - lock implementation ignores it as a clone is
      //cast on locked segments and released before the close on the parent object
      IndexInput cloneOfFirstOpenOnB = (IndexInput) firstOpenOnB.clone();
      assertFileExistsHavingRLCount(filename, 0, false);
      cloneOfFirstOpenOnB.close();
      assertFileExistsHavingRLCount(filename, 0, false);
      IndexInput firstOpenOnA = dirA.openInput(filename, IOContext.DEFAULT);
      assertFileExistsHavingRLCount(filename, 0, false);
      IndexInput secondOpenOnA = dirA.openInput(filename, IOContext.DEFAULT);
      assertFileExistsHavingRLCount(filename, 0, false);
      firstOpenOnA.close();
      assertFileExistsHavingRLCount(filename, 0, false);
      secondOpenOnA.close();
      assertFileExistsHavingRLCount(filename, 0, false);
      firstOpenOnB.close();

      //As the NoopSegmentReadLocker ignores also file deletions, then verifying.
      assertFileAfterDeletion(cache0);
      assertFileAfterDeletion(cache1);

      dirA.close();
      dirB.close();
      Set<String> left_behind = Util.asSet("pending_segments_1", "readme.txt");
      verifyIgnoringFiles(cache0, left_behind);
      verifyIgnoringFiles(cache1, left_behind);
   }

   void verifyIgnoringFiles(Cache cache0, Cache cache1, Set<String> files) {
      verifyIgnoringFiles(cache0, files);
      verifyIgnoringFiles(cache1, files);
   }

   /**
    * Lucene 5 creates temporary "pending_segments_n" files during commit that get renamed to "segment_n", and since the
    * NoopSegmentReadLocker never deletes files, some garbage are left behind
    */
   void verifyIgnoringFiles(Cache cache,Set<String> ignoring) {
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, INDEX_NAME, ignoring);
   }

   private void assertFileAfterDeletion(Cache cache) {
      FileListCacheValue fileList = (FileListCacheValue) cache.get(new FileListCacheKey(INDEX_NAME, -1));
      AssertJUnit.assertNotNull(fileList);
      AssertJUnit.assertFalse(fileList.contains(filename));

      FileMetadata metadata = (FileMetadata) cache.get(new FileCacheKey(INDEX_NAME, filename, -1));
      AssertJUnit.assertNotNull(metadata);
      long totalFileSize = metadata.getSize();
      int chunkNumbers = (int)(totalFileSize / CHUNK_SIZE);

      for(int i = 0; i < chunkNumbers; i++) {
         AssertJUnit.assertNotNull(cache.get(new ChunkCacheKey(INDEX_NAME, filename, i, CHUNK_SIZE, -1)));
      }

      boolean fileNameExistsInCache = false;
      for(Object key : cache.keySet()) {
         if(key instanceof FileCacheKey) {
            FileCacheKey keyObj = (FileCacheKey) key;

            if(keyObj.getFileName().contains(filename)) {
               fileNameExistsInCache = true;
            }
         }
      }

      AssertJUnit.assertTrue(fileNameExistsInCache);
   }

}
