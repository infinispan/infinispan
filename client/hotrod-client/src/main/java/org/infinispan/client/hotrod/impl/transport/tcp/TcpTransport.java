package org.infinispan.client.hotrod.impl.transport.tcp;

import org.infinispan.client.hotrod.impl.AbstractTransport;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class TcpTransport extends AbstractTransport {

   private static Log log = LogFactory.getLog(TcpTransport.class);

   private Socket socket;
   private InetSocketAddress serverAddress;

   public void writeVInt(int vInt) {
      try {
         VHelper.writeVInt(vInt, socket.getOutputStream());
         if (log.isTraceEnabled())
            log.trace("VInt wrote " + vInt);
         
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public void writeVLong(long l) {
      try {
         VHelper.writeVLong(l, socket.getOutputStream());
         if (log.isTraceEnabled())
            log.trace("VLong wrote " + l);        

      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public long readVLong() {
      try {
         long result = VHelper.readVLong(socket.getInputStream());
         if (log.isTraceEnabled())
            log.trace("VLong read " + result);
         return result;
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public int readVInt() {
      try {
         int result = VHelper.readVInt(socket.getInputStream());
         if (log.isTraceEnabled())
            log.trace("VInt read " + result);
         return result;
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public TcpTransport(InetSocketAddress serverAddress) {
      this.serverAddress = serverAddress;
      try {
         SocketChannel socketChannel = SocketChannel.open(serverAddress);
         socket = socketChannel.socket();
      } catch (IOException e) {
         String message = "Could not connect to server: " + serverAddress;
         log.error(message, e);
         throw new TransportException(message, e);
      }
   }

   protected void writeBytes(byte[] toAppend) {
      try {
         socket.getOutputStream().write(toAppend);
         if (log.isTraceEnabled())
            log.trace("Wrote " + toAppend.length + " bytes");
      } catch (IOException e) {
         throw new TransportException("Problems writing data to stream", e);
      }
   }

   @Override
   public void writeByte(short toWrite) {
      try {
         socket.getOutputStream().write(toWrite);
         if (log.isTraceEnabled())
            log.trace("Wrote byte " + toWrite);

      } catch (IOException e) {
         throw new TransportException("Problems writing data to stream", e);
      }
   }

   public void flush() {
      try {
         socket.getOutputStream().flush();
         if (log.isTraceEnabled())
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
      if (log.isTraceEnabled()) {
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
         if (log.isTraceEnabled()) {
            log.trace("Successfully closed socket: " + socket);
         }
      } catch (IOException e) {
         log.warn("Issues closing transport: " + this, e);
      }
   }
}
