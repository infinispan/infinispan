package org.infinispan.lucene.cacheloader;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Test for extra-large indexes, where an int isn't large enough to hold file sizes.
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
@Test(groups = "functional", testName = "lucene.cachestore.LargeIndexesTest")
public class LargeIndexesTest {

   private static final String INDEX_NAME = "myIndex";
   private static final String FILE_NAME = "largeFile";
   private static final long TEST_SIZE = ((long)Integer.MAX_VALUE) + 10;//something not fitting in int
   private static final int AUTO_BUFFER = 16;//ridiculously low

   private Directory createMockDirectory() throws IOException {
      Directory mockDirectory = mock(Directory.class);
      when(mockDirectory.openInput(FILE_NAME, IOContext.READ)).thenAnswer(new Answer<IndexInputMock>() {
         @Override
         public IndexInputMock answer(InvocationOnMock invocationOnMock) throws Throwable {
            return new IndexInputMock(FILE_NAME);
         }
      });
      when(mockDirectory.fileLength(FILE_NAME)).thenReturn(TEST_SIZE);
      verify(mockDirectory, never()).listAll();
      verify(mockDirectory, never()).fileExists(FILE_NAME);
      return mockDirectory;
   }

   public void testAutoChunkingOnLargeFiles() throws IOException {
      Directory mockDirectory = createMockDirectory();

      FileCacheKey k = new FileCacheKey(INDEX_NAME, FILE_NAME);
      DirectoryLoaderAdaptor adaptor = new DirectoryLoaderAdaptor(mockDirectory, INDEX_NAME, AUTO_BUFFER);
      Object loaded = adaptor.load(k);
      AssertJUnit.assertTrue(loaded instanceof FileMetadata);
      FileMetadata metadata = (FileMetadata)loaded;
      AssertJUnit.assertEquals(TEST_SIZE, metadata.getSize());
      AssertJUnit.assertEquals(AUTO_BUFFER, metadata.getBufferSize());
   }

   public void testSmallChunkLoading() throws IOException {
      Directory mockDirectory = createMockDirectory();

      DirectoryLoaderAdaptor adaptor = new DirectoryLoaderAdaptor(mockDirectory, INDEX_NAME, AUTO_BUFFER);
      Object loaded = adaptor.load(new ChunkCacheKey(INDEX_NAME, FILE_NAME, 0, AUTO_BUFFER));
      AssertJUnit.assertTrue(loaded instanceof byte[]);
      AssertJUnit.assertEquals(AUTO_BUFFER, ((byte[])loaded).length);
      loaded = adaptor.load(new ChunkCacheKey(INDEX_NAME, FILE_NAME, 5, AUTO_BUFFER));
      AssertJUnit.assertTrue(loaded instanceof byte[]);
      AssertJUnit.assertEquals(AUTO_BUFFER, ((byte[])loaded).length);
      final int lastChunk = (int)(TEST_SIZE / AUTO_BUFFER);
      final long lastChunkSize = TEST_SIZE % AUTO_BUFFER;
      AssertJUnit.assertEquals(9, lastChunkSize);
      loaded = adaptor.load(new ChunkCacheKey(INDEX_NAME, FILE_NAME, lastChunk, AUTO_BUFFER));
      AssertJUnit.assertTrue(loaded instanceof byte[]);
      AssertJUnit.assertEquals(lastChunkSize, ((byte[])loaded).length);
   }

   private static class IndexInputMock extends IndexInput {

      private boolean closed = false;
      private long position = 0;

      protected IndexInputMock(String resourceDescription) {
         super(resourceDescription);
      }

      @Override
      public void close() throws IOException {
         AssertJUnit.assertFalse(closed);
         closed = true;
      }

      @Override
      public long getFilePointer() {
         AssertJUnit.fail("should not be invoked");
         return 0;
      }

      @Override
      public void seek(long pos) throws IOException {
         position = pos;
      }

      @Override
      public long length() {
         return TEST_SIZE;
      }

      @Override
      public byte readByte() throws IOException {
         return 0;
      }

      @Override
      public void readBytes(byte[] b, int offset, int len) throws IOException {
         final long remainingFileSize = TEST_SIZE - position;
         final long expectedReadSize = Math.min(remainingFileSize, AUTO_BUFFER);
         AssertJUnit.assertEquals(expectedReadSize, b.length);
         AssertJUnit.assertEquals(0, offset);
         AssertJUnit.assertEquals(expectedReadSize, len);
      }

      public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
         return null;
      }
   }

}
