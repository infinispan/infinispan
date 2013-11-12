package org.infinispan.io;

import org.infinispan.Cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;

/**
 * @author Marko Luksa
 */
public class WritableGridFileChannel implements WritableByteChannel {

   private final GridOutputStream gridOutputStream;
   private final WritableByteChannel delegate;

   WritableGridFileChannel(GridFile file, Cache<String, byte[]> cache, boolean append) {
      this.gridOutputStream = new GridOutputStream(file, append, cache);
      this.delegate = Channels.newChannel(gridOutputStream);
   }

   @Override
   public int write(ByteBuffer src) throws IOException {
      checkOpen();
      return delegate.write(src);
   }

   public void flush() throws IOException {
      gridOutputStream.flush();
   }

   @Override
   public boolean isOpen() {
      return delegate.isOpen();
   }

   @Override
   public void close() throws IOException {
      delegate.close();
   }

   private void checkOpen() throws ClosedChannelException {
      if (!isOpen()) {
         throw new ClosedChannelException();
      }
   }
}
