package org.infinispan.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;

import org.infinispan.Cache;

/**
 * @author Marko Luksa
 */
public class ReadableGridFileChannel implements ReadableByteChannel {

   private final GridInputStream gridInputStream;
   private final ReadableByteChannel delegate;

   ReadableGridFileChannel(GridFile file, Cache<String, byte[]> cache) {
      this.gridInputStream = new GridInputStream(file, cache);
      this.delegate = Channels.newChannel(gridInputStream);
   }

   @Override
   public int read(ByteBuffer dst) throws IOException {
      checkOpen();
      return delegate.read(dst);
   }

   public long position() throws IOException {
      checkOpen();
      return gridInputStream.position();
   }

   public void position(long newPosition) throws IOException {
      checkOpen();
      gridInputStream.position(newPosition);
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

   public long size() throws IOException {
      return gridInputStream.getFileSize();
   }
}
