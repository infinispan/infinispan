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

   private Socket socket;

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

   public TcpTransport(Socket socket) {
      this.socket = socket;
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

   public Socket getSocket() {
      return socket;
   }
}
