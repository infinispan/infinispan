package org.infinispan.client.hotrod.impl.transport.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
* // TODO: Document this
*/
class ByteBufferOutputStream extends OutputStream {

   private ByteBuffer buffer;

   ByteBufferOutputStream(ByteBuffer buffer) {
      this.buffer = buffer;
   }

   private void grow(int minCapacityIncrease) {
      buffer = grow(buffer, minCapacityIncrease);
   }

   @Override
   public void write(int b) throws IOException {
      if (!buffer.hasRemaining()) {
         grow(1);
      }
      buffer.put((byte) b);
   }

   @Override
   public void write(byte[] b, int off, int len) throws IOException {
      if (buffer.remaining() < len) {
         grow(len - buffer.remaining());
      }
      buffer.put(b, off, len);
   }

   private static ByteBuffer grow(ByteBuffer buffer, int minCapacityIncrease) {
      ByteBuffer tmp = ByteBuffer.allocate(Math.max(buffer.capacity() << 1, buffer.capacity() + minCapacityIncrease));
      buffer.flip();
      tmp.put(buffer);
      return tmp;
   }

   public ByteBuffer getBuffer() {
      return buffer;
   }
}
