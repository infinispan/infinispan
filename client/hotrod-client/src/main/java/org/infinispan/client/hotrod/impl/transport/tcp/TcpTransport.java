package org.infinispan.client.hotrod.impl.transport.tcp;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.client.hotrod.impl.transport.AbstractTransport;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.io.UnsignedNumeric.*;

/**
 * Transport implementation based on TCP.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe
public class TcpTransport extends AbstractTransport {

   //needed for debugging
   private static AtomicLong ID_COUNTER = new AtomicLong(0);

   private static Log log = LogFactory.getLog(TcpTransport.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Socket socket;
   private final InetSocketAddress serverAddress;
   private final long id = ID_COUNTER.incrementAndGet();

   public TcpTransport(InetSocketAddress serverAddress, TransportFactory transportFactory) {
      super(transportFactory);
      this.serverAddress = serverAddress;
      try {
         SocketChannel socketChannel = SocketChannel.open(serverAddress);
         socket = socketChannel.socket();
         socket.setTcpNoDelay(transportFactory.isTcpNoDelay());
      } catch (IOException e) {
         String message = "Could not connect to server: " + serverAddress;
         log.error(message, e);
         throw new TransportException(message, e);
      }
   }

   public void writeVInt(int vInt) {
      try {
         writeUnsignedInt(socket.getOutputStream(), vInt);
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public void writeVLong(long l) {
      try {
         writeUnsignedLong(socket.getOutputStream(), l);
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public long readVLong() {
      try {
         return readUnsignedLong(socket.getInputStream());
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public int readVInt() {
      try {
         return readUnsignedInt(socket.getInputStream());
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   protected void writeBytes(byte[] toAppend) {
      try {
         socket.getOutputStream().write(toAppend);
         if (trace)
            log.trace("Wrote " + toAppend.length + " bytes");
      } catch (IOException e) {
         throw new TransportException("Problems writing data to stream", e);
      }
   }

   @Override
   public void writeByte(short toWrite) {
      try {
         socket.getOutputStream().write(toWrite);
         if (trace)
            log.trace("Wrote byte " + toWrite);

      } catch (IOException e) {
         throw new TransportException("Problems writing data to stream", e);
      }
   }

   public void flush() {
      try {
         socket.getOutputStream().flush();
         if (trace)
            log.trace("Flushed socket: " + socket);

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
            if (trace) {
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
      if (trace) {
         log.trace("Successfully read array with size: " + size);
      }
      return result;
   }

   public InetSocketAddress getServerAddress() {
      return serverAddress;
   }

   @Override
   public String toString() {
      return "TcpTransport{" +
              "socket=" + socket +
              ", serverAddress=" + serverAddress +
              ", id =" + id +
              "} ";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TcpTransport that = (TcpTransport) o;

      if (serverAddress != null ? !serverAddress.equals(that.serverAddress) : that.serverAddress != null) return false;
      if (socket != null ? !socket.equals(that.socket) : that.socket != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = socket != null ? socket.hashCode() : 0;
      result = 31 * result + (serverAddress != null ? serverAddress.hashCode() : 0);
      return result;
   }

   public void destroy() {
      try {
         socket.close();
         if (trace) {
            log.trace("Successfully closed socket: " + socket);
         }
      } catch (IOException e) {
         log.warn("Issues closing transport: " + this, e);
      }
   }

   public boolean isValid() {
      return !socket.isClosed();
   }

   public long getId() {
      return id;
   }
}
