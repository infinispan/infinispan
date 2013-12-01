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
