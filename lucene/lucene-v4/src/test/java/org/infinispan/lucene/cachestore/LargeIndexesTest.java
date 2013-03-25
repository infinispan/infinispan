/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.lucene.cachestore;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.lucene.cachestore.InternalDirectoryContract;
import org.junit.Assert;
import org.testng.annotations.Test;

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

   public void testAutoChunkingOnLargeFiles() throws CacheLoaderException {
      FileCacheKey k = new FileCacheKey(INDEX_NAME, FILE_NAME);
      DirectoryLoaderAdaptor adaptor = new DirectoryLoaderAdaptor(new InternalDirectoryContractImpl(), INDEX_NAME, AUTO_BUFFER);
      Object loaded = adaptor.load(k);
      Assert.assertTrue(loaded instanceof FileMetadata);
      FileMetadata metadata = (FileMetadata)loaded;
      Assert.assertEquals(23, metadata.getLastModified());
      Assert.assertEquals(TEST_SIZE, metadata.getSize());
      Assert.assertEquals(AUTO_BUFFER, metadata.getBufferSize());
   }

   public void testSmallChunkLoading() throws CacheLoaderException {
      DirectoryLoaderAdaptor adaptor = new DirectoryLoaderAdaptor(new InternalDirectoryContractImpl(), INDEX_NAME, AUTO_BUFFER);
      Object loaded = adaptor.load(new ChunkCacheKey(INDEX_NAME, FILE_NAME, 0, AUTO_BUFFER));
      Assert.assertTrue(loaded instanceof byte[]);
      Assert.assertEquals(AUTO_BUFFER, ((byte[])loaded).length);
      loaded = adaptor.load(new ChunkCacheKey(INDEX_NAME, FILE_NAME, 5, AUTO_BUFFER));
      Assert.assertTrue(loaded instanceof byte[]);
      Assert.assertEquals(AUTO_BUFFER, ((byte[])loaded).length);
      final int lastChunk = (int)(TEST_SIZE / AUTO_BUFFER);
      final long lastChunkSize = TEST_SIZE % AUTO_BUFFER;
      Assert.assertEquals(9, lastChunkSize);
      loaded = adaptor.load(new ChunkCacheKey(INDEX_NAME, FILE_NAME, lastChunk, AUTO_BUFFER));
      Assert.assertTrue(loaded instanceof byte[]);
      Assert.assertEquals(lastChunkSize, ((byte[])loaded).length);
   }

   //Simple home-made mock:
   private static class InternalDirectoryContractImpl implements InternalDirectoryContract {

      @Override
      public String[] listAll() throws IOException {
         Assert.fail("should not be invoked");
         return null;//compiler thinks we could reach this
      }

      @Override
      public long fileLength(String fileName) throws IOException {
         Assert.assertEquals(FILE_NAME, fileName);
         return TEST_SIZE;
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public long fileModified(String fileName) {
         Assert.assertEquals(FILE_NAME, fileName);
         return 23;
      }

      @Override
      public IndexInput openInput(String fileName) {
         Assert.assertEquals(FILE_NAME, fileName);
         return new IndexInputMock(fileName);
      }

      @Override
      public boolean fileExists(String fileName) {
         Assert.fail("should not be invoked");
         return false;
      }
   }

   private static class IndexInputMock extends IndexInput {

      protected IndexInputMock(String resourceDescription) {
         super(resourceDescription);
      }

      private boolean closed = false;
      private long position = 0;

      @Override
      public void close() throws IOException {
         Assert.assertFalse(closed);
         closed = true;
      }

      @Override
      public long getFilePointer() {
         Assert.fail("should not be invoked");
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
         Assert.assertEquals(expectedReadSize, b.length);
         Assert.assertEquals(0, offset);
         Assert.assertEquals(expectedReadSize, len);
      }
   }

}
