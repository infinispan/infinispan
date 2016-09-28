package org.infinispan.client.hotrod.impl.protocol;

import java.io.IOException;
import java.io.InputStream;

import org.infinispan.client.hotrod.VersionedMetadata;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;

import net.jcip.annotations.NotThreadSafe;

/**
 * Implements an {@link InputStream} around a {@link Transport}.
 * The transport is marked as <b>busy</b> so that it won't be released automatically
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@NotThreadSafe
class TransportInputStream extends InputStream implements VersionedMetadata {
   private final Transport transport;
   private final int totalLength;
   private final Runnable afterClose;
   private final VersionedMetadata versionedMetadata;
   private int totalPosition;
   private byte[] buffer;
   private int bufferPosition;
   private int bufferLimit;

   TransportInputStream(Transport transport, VersionedMetadata versionedMetadata, Runnable afterClose) {
      this.transport = transport;
      totalLength = transport.readVInt();
      buffer = new byte[TcpTransport.SOCKET_STREAM_BUFFER];
      this.versionedMetadata = versionedMetadata;
      this.afterClose = afterClose;
   }

   @Override
   public long getVersion() {
      return versionedMetadata.getVersion();
   }

   @Override
   public long getCreated() {
      return versionedMetadata.getCreated();
   }

   @Override
   public int getLifespan() {
      return versionedMetadata.getLifespan();
   }

   @Override
   public long getLastUsed() {
      return versionedMetadata.getLastUsed();
   }

   @Override
   public int getMaxIdle() {
      return versionedMetadata.getMaxIdle();
   }

   private void fill() {
      bufferLimit = Math.min(TcpTransport.SOCKET_STREAM_BUFFER, totalLength - totalPosition);
      if (bufferLimit > 0) {
         transport.readByteArray(buffer, bufferLimit);
         totalPosition += bufferLimit;
      }
      bufferPosition = 0;
   }

   @Override
   public int read() throws IOException {
      if (bufferPosition >= bufferLimit) {
         fill();
         if (bufferPosition >= bufferLimit)
            return -1;
      }
      return buffer[bufferPosition++] & 0xff;
   }

   private int read1(byte[] b, int off, int len) {
      int available = bufferLimit - bufferPosition;
      if (available <= 0) {
         fill();
         available = bufferLimit - bufferPosition;
         if (available <= 0) return -1;
      }
      int amount = (available < len) ? available : len;
      System.arraycopy(buffer, bufferPosition, b, off, amount);
      bufferPosition += amount;
      return amount;
   }

   @Override
   public synchronized int read(byte b[], int off, int len)
         throws IOException
   {
      if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
         throw new IndexOutOfBoundsException();
      } else if (len == 0) {
         return 0;
      }

      int n = 0;
      for (;;) {
         int nread = read1(b, off + n, len - n);
         if (nread <= 0)
            return (n == 0) ? nread : n;
         n += nread;
         if (n >= len)
            return n;
         return 0;
      }
   }

   @Override
   public void close() throws IOException {
      super.close();
      if (afterClose != null) {
         afterClose.run();
      }
   }
}
