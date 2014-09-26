package org.infinispan.lucene.impl;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

/**
 * Wraps an InfinispanIndexInput to expose only a slice of it.
 * Such slices are dependent on the parent IndexInput and will not handle readlocks.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
final class SlicingInfinispanIndexInput extends IndexInput {

   private final long offset;
   private final long length;
   private InfinispanIndexInput delegate;

   public SlicingInfinispanIndexInput(String sliceDescription, long offset, long length, InfinispanIndexInput delegate) {
      super(sliceDescription);
      this.offset = offset;
      this.length = length;
      this.delegate = delegate;
      delegate.seek(offset);
   }

   @Override
   public void close() throws IOException {
      delegate.close();
   }

   @Override
   public long getFilePointer() {
      return delegate.getFilePointer() - offset;
   }

   @Override
   public void seek(long pos) throws IOException {
      delegate.seek(pos + offset);
   }

   @Override
   public long length() {
      return length;
   }

   public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new SlicingInfinispanIndexInput(sliceDescription, offset + this.offset, length, delegate.copyAndReset());
   }

   @Override
   public IndexInput clone() {
      SlicingInfinispanIndexInput clone = (SlicingInfinispanIndexInput) super.clone();
      //This is important! the cloned IndexInput needs to be able to move the current read position
      //independently from the original one, but starting at the same read position at the moment of cloning.
      clone.delegate = delegate.clone();
      return clone;
   }

   @Override
   public byte readByte() throws IOException {
      return delegate.readByte();
   }

   @Override
   public void readBytes(byte[] b, int offset, int len) throws IOException {
      delegate.readBytes(b, offset, len);
   }

}
