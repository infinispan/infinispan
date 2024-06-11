package org.infinispan.client.hotrod.impl.operations;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import io.netty.buffer.ByteBuf;

public class ByteBufSwappableInputStream extends InputStream {
   private ByteBuf buffer;
   private int unavailableBytes;

   public void setBuffer(ByteBuf buffer, int readLimit) {
      this.buffer = buffer;
      // We only want to allow reading up to the readLimit from the ByteBuf.
      // We can't use buffer.writeIndex since it is a replaying buffer
      this.unavailableBytes = buffer.readableBytes() - readLimit;
   }

   public int available() throws IOException {
      return buffer.readableBytes() - unavailableBytes;
   }

   public void mark(int readlimit) {
      this.buffer.markReaderIndex();
   }

   public boolean markSupported() {
      return true;
   }

   public int read() throws IOException {
      int available = this.available();
      return available == 0 ? -1 : this.buffer.readByte() & 255;
   }

   public int read(byte[] b, int off, int len) throws IOException {
      int available = this.available();
      if (available == 0) {
         return -1;
      } else {
         len = Math.min(available, len);
         this.buffer.readBytes(b, off, len);
         return len;
      }
   }

   public void reset() throws IOException {
      this.buffer.resetReaderIndex();
   }

   public long skip(long n) throws IOException {
      return n > 2147483647L ? (long)this.skipBytes(Integer.MAX_VALUE) : (long)this.skipBytes((int)n);
   }

   public byte readByte() throws IOException {
      int available = this.available();
      if (available == 0) {
         throw new EOFException();
      } else {
         return this.buffer.readByte();
      }
   }

   public int skipBytes(int n) throws IOException {
      int nBytes = Math.min(this.available(), n);
      this.buffer.skipBytes(nBytes);
      return nBytes;
   }
}
