package org.infinispan.lucene.impl;

import java.io.IOException;

import org.apache.lucene.store.BufferedIndexOutput;
import org.infinispan.AdvancedCache;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileMetadata;

/**
 * Since Lucene 4.8 segments using the default codec are expected
 * to store a crc32 based checksum.
 * I'd love to create the checksum on flush only, but the bytes need
 * to be appended to the CRC32 instance in the same order as they
 * are appended in other Lucene Directory implementations.
 *
 * @since 7.0
 * @author Sanne Grinovero
 */
public final class CheckSummingIndexOutput extends BufferedIndexOutput {

   private final InfinispanIndexOutput delegateOutput;

   public CheckSummingIndexOutput(AdvancedCache<FileCacheKey, FileMetadata> metadataCache, AdvancedCache<ChunkCacheKey, Object> chunksCache, FileCacheKey fileKey, int bufferSize, FileListOperations fileList) {
      super();
      delegateOutput = new InfinispanIndexOutput(metadataCache, chunksCache, fileKey, bufferSize, fileList);
   }

   @Override
   protected void flushBuffer(byte[] b, int offset, int length) throws IOException {
      delegateOutput.writeBytes(b, offset, length);
   }

   @Override
   public long length() throws IOException {
      return delegateOutput.length();
   }

   @Override
   public void close() throws IOException {
      super.close();
      delegateOutput.close();
   }

   @Override
   public void seek(long pos) throws IOException {
      super.seek(pos);
      delegateOutput.seek(pos);
   }

   @Override
   public void setLength(long length) throws IOException {
      delegateOutput.setLength(length);
   }

}
