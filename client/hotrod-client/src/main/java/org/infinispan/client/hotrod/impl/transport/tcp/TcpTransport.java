package org.infinispan.client.hotrod.impl.transport.tcp;

import org.infinispan.client.hotrod.impl.AbstractTransport;
import org.infinispan.client.hotrod.impl.transport.TransportException;
import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class TcpTransport extends AbstractTransport {

   private static Log log = LogFactory.getLog(TcpTransport.class);

   private String host;
   private int port;
   private Socket socket;

   public void writeVInt(int length) {
      try {
         VHelper.writeVInt(length, socket.getOutputStream());
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public void writeVLong(long l) {
      try {
         VHelper.writeVLong(l, socket.getOutputStream());
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public long readVLong() {
      try {
         return VHelper.readVLong(socket.getInputStream());
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public int readVInt() {
      try {
         return VHelper.readVInt(socket.getInputStream());
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public TcpTransport(String host, int port) {
      this.host = host;
      this.port = port;
   }

   public void connect() {
      try {
         SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
         socket = socketChannel.socket();
      } catch (IOException e) {
         throw new TransportException("Problems establishing initial connection", e);
      }
   }

   protected void writeBytes(byte[] toAppend) {
      try {
         socket.getOutputStream().write(toAppend);
      } catch (IOException e) {
         throw new TransportException("Problems writing data to stream", e);
      }
   }

   @Override
   public void writeByte(short toWrite) {
      try {
         socket.getOutputStream().write(toWrite);
      } catch (IOException e) {
         throw new TransportException("Problems writing data to stream", e);
      }
   }

   public void flush() {
      try {
         socket.getOutputStream().flush();
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public short readByte() {
      int resultInt;
      try {
         resultInt = socket.getInputStream().read();
      } catch (IOException e) {
         throw new TransportException(e);
      }
      if (resultInt == -1) {
         throw new TransportException("End of stream reached!");
      }
      return (short) resultInt;
   }

   public void release() {
      try {
         socket.close();
      } catch (IOException e) {
         log.warn("Issues closing socket:" + e.getMessage());
      }
   }

   public byte[] readByteArray(final int size) {
      byte[] result = new byte[size];
      boolean done = false;
      int offset = 0;
      do {
         int read;
         try {
            int len = size - offset;
            if (log.isTraceEnabled()) {
               log.trace("Offset: " + offset + ", len=" + len + ", size=" + size);
            }
            read = socket.getInputStream().read(result, offset, len);
         } catch (IOException e) {
            throw new TransportException(e);
         }
         if (read == -1) {
            throw new RuntimeException("End of stream reached!");
         }
         if (read + offset == size) {
            done = true;
         } else {
            offset += read;
            if (offset > result.length) throw new IllegalStateException("Assertion!");
         }
      } while (!done);
      return result;
   }
}
