package org.infinispan.client.hotrod.impl.transport.tcp;

import org.infinispan.client.hotrod.impl.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportException;
import org.infinispan.client.hotrod.impl.transport.VHelper;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Logger;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class TcpTransport implements Transport {

   public static final Logger log = Logger.getLogger(TcpTransport.class.getName());

   private String host;
   private int port;
   private Socket socket;

   public void appendUnsignedByte(short requestMagic) {
      try {
         socket.getOutputStream().write(requestMagic);
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

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
         socket = new Socket(host, port);
      } catch (IOException e) {
         throw new TransportException("Problems establishing initial connection", e);
      }
   }

   public void writeBytesArray(byte... toAppend) {
      try {
         writeVInt(toAppend.length);
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
         log.warning("Issues closing socket:" + e.getMessage());
      }
   }

   public byte[] readByteArray(byte[] bufferToFill) {
      int size;
      try {
         size = socket.getInputStream().read(bufferToFill);
      } catch (IOException e) {
         throw new TransportException(e);
      }
      if (size == -1) {
         throw new RuntimeException("End of stream reached!");
      }
      if (size != bufferToFill.length) {
         throw new TransportException("Expected " + bufferToFill.length + " bytes but only could read " + size + " bytes!");
      }
      return bufferToFill;
   }

   @Override
   public String readString() {
      long strLength = readVLong();
      byte[] strContent = readByteArray(new byte[(int)strLength]);
      return new String(strContent);//todo take care of encoding here
   }
}
