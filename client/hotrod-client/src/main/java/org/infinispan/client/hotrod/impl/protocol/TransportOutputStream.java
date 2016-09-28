package org.infinispan.client.hotrod.impl.protocol;

import java.io.IOException;
import java.io.OutputStream;

import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;

import net.jcip.annotations.NotThreadSafe;

/**
 * Implements an {@link OutputStream} around a {@link Transport}.
 * The transport is marked as <b>busy</b> so that it won't be released automatically
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@NotThreadSafe
class TransportOutputStream extends OutputStream {
   private final Transport transport;
   private final Runnable afterClose;
   private byte buf[];
   private int count;

   public TransportOutputStream(Transport transport, Runnable afterClose) {
      this.transport = transport;
      buf = new byte[TcpTransport.SOCKET_STREAM_BUFFER];
      this.afterClose = afterClose;
   }

   private void internalFlush() throws IOException {
      if (count > 0) {
         transport.writeArray(buf, 0, count);
         transport.flush();
         count = 0;
      }
   }

   public void write(int b) throws IOException {
      if (count >= buf.length) {
         internalFlush();
      }
      buf[count++] = (byte)b;
   }

   public void write(byte b[], int off, int len) throws IOException {
      if (len >= buf.length) {
          internalFlush();
         transport.writeArray(b, off, len);
         return;
      }
      if (len > buf.length - count) {
         internalFlush();
      }
      System.arraycopy(b, off, buf, count, len);
      count += len;
   }

   public void flush() throws IOException {
      internalFlush();
   }

   @Override
   public void close() throws IOException {
      flush();
      transport.writeVInt(0); // 0-sized buffer to indicate end of data
      transport.flush();
      if (afterClose != null) {
         afterClose.run();
      }
   }
}
