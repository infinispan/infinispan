package org.infinispan.client.hotrod.impl.transport.tcp;

import static org.infinispan.commons.io.SignedNumeric.writeSignedInt;
import static org.infinispan.commons.io.UnsignedNumeric.readUnsignedInt;
import static org.infinispan.commons.io.UnsignedNumeric.readUnsignedLong;
import static org.infinispan.commons.io.UnsignedNumeric.writeUnsignedInt;
import static org.infinispan.commons.io.UnsignedNumeric.writeUnsignedLong;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.security.sasl.SaslClient;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.transport.AbstractTransport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.Util;

/**
 * Transport implementation based on TCP.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TcpTransport extends AbstractTransport {
   public static final int SOCKET_STREAM_BUFFER = 8 * 1024;

   //needed for debugging
   private static AtomicLong ID_COUNTER = new AtomicLong(0);

   private static final Log log = LogFactory.getLog(TcpTransport.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Socket socket;
   private final SocketChannel socketChannel;
   private InputStream socketInputStream;
   private OutputStream socketOutputStream;
   private final SocketAddress serverAddress;
   private final long id = ID_COUNTER.incrementAndGet();

   private volatile boolean invalid;

   private SaslClient saslClient;

   public TcpTransport(SocketAddress originalServerAddress, TransportFactory transportFactory) {
      super(transportFactory);
      InetSocketAddress inetSocketAddress = (InetSocketAddress) originalServerAddress;
      this.serverAddress = originalServerAddress;
      try {
         if (inetSocketAddress.isUnresolved())
            inetSocketAddress = new InetSocketAddress(inetSocketAddress.getHostString(), inetSocketAddress.getPort());

         IoSupplier<Socket> socketF;
         if (transportFactory.getSSLContext() != null) {
            socketChannel = null; // We don't use a SocketChannel in the SSL case
            SSLContext sslContext = transportFactory.getSSLContext();
            SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
            socketF = () -> {
               Socket socket = sslSocketFactory.createSocket();
               setSniHostName(socket, transportFactory.getSniHostName());
               return socket;
            };
         } else {
            socketChannel = SocketChannel.open();
            socketF = socketChannel::socket;
         }
         socket = connectSocket(socketF, transportFactory, inetSocketAddress);
         socket.setTcpNoDelay(transportFactory.isTcpNoDelay());
         socket.setKeepAlive(transportFactory.isTcpKeepAlive());
         socket.setSoTimeout(transportFactory.getSoTimeout());
         socketInputStream = new BufferedInputStream(socket.getInputStream(), SOCKET_STREAM_BUFFER);
         // ensure we don't send a packet for every output byte
         socketOutputStream = new BufferedOutputStream(socket.getOutputStream(), SOCKET_STREAM_BUFFER);
      } catch (Exception e) {
         String message = String.format("Could not connect to server: %s", serverAddress);
         log.tracef(e, "Could not connect to server: %s", serverAddress);
         throw new TransportException(message, e, serverAddress);
      }
   }

   public Socket connectSocket(IoSupplier<Socket> socketF,
         TransportFactory transportFactory,
         InetSocketAddress inetSocketAddress) throws IOException {
      int port;
      int localport;
      Socket socket;
      do {
         socket = socketF.get();
         socket.connect(inetSocketAddress, transportFactory.getConnectTimeout());
         port = socket.getPort();
         localport = socket.getLocalPort();
         if (port == localport) {
            log.debugf("Socket port(%d) and localport(%d) same, disconnect and try again", port, localport);
            socket.close();
         }
      } while (port == localport);

      return socket;
   }

   private void setSniHostName(Socket socket, String sniHostName) {
      if(sniHostName != null) {
         SSLSocket sslSocket = (SSLSocket) socket;
         SSLParameters sslParameters = sslSocket.getSSLParameters();
         sslParameters.setServerNames(Arrays.asList(new SNIHostName(sniHostName)));
         sslSocket.setSSLParameters(sslParameters);
      }
   }

   void setSaslClient(SaslClient saslClient) {
      this.saslClient = saslClient;
      try {
         this.socketInputStream = new SaslInputStream(socket.getInputStream(), saslClient);
         this.socketOutputStream = new SaslOutputStream(socket.getOutputStream(), saslClient);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   public void writeVInt(int vInt) {
      try {
         writeUnsignedInt(socketOutputStream, vInt);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   public void writeSignedVInt(int vInt) {
      try {
         writeSignedInt(socketOutputStream, vInt);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   public void writeVLong(long l) {
      try {
         writeUnsignedLong(socketOutputStream, l);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   public long readVLong() {
      try {
         return readUnsignedLong(socketInputStream);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   public int readVInt() {
      try {
         return readUnsignedInt(socketInputStream);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   protected void writeBytes(byte[] toAppend) {
      writeBytes(toAppend, 0, toAppend.length);
   }

   @Override
   protected void writeBytes(byte[] toAppend, int offset, int count) {
      try {
         for (int o = offset; o < offset + count; o += SOCKET_STREAM_BUFFER) {
            socketOutputStream.write(toAppend, o, Math.min(offset + count - o, SOCKET_STREAM_BUFFER));
         }
         if (trace) {
            log.tracef("Wrote %d bytes", count);
         }
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(
               "Problems writing data to stream", e, serverAddress);
      }
   }

   @Override
   public void writeByte(short toWrite) {
      try {
         socketOutputStream.write(toWrite);
         if (trace) {
            log.tracef("Wrote byte %d", toWrite);
         }

      } catch (IOException e) {
         invalid = true;
         throw new TransportException(
               "Problems writing data to stream", e, serverAddress);
      }
   }

   @Override
   public void flush() {
      try {
         socketOutputStream.flush();
         if (trace) {
            log.tracef("Flushed socket: %s", socket);
         }

      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   public short readByte() {
      int resultInt;
      try {
         resultInt = socketInputStream.read();
         if (trace)
            log.tracef("Read byte %d from socket input in %s", resultInt, socket);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
      if (resultInt == -1) {
         throw new TransportException("End of stream reached!", serverAddress);
      }
      return (short) resultInt;
   }

   @Override
   public void release() {
      destroy();
   }

   @Override
   public void readByteArray(byte[] result, int size) {
      boolean done = false;
      int offset = 0;
      do {
         int read;
         try {
            int len = Math.min(size - offset, SOCKET_STREAM_BUFFER);
            if (trace) {
               log.tracef("Offset: %d, len=%d, size=%d", offset, len, size);
            }
            read = socketInputStream.read(result, offset, len);
         } catch (IOException e) {
            invalid = true;
            throw new TransportException(e, serverAddress);
         }
         if (read == -1) {
            throw new RuntimeException("End of stream reached!");
         }
         if (read + offset == size) {
            done = true;
         } else {
            offset += read;
            if (offset > result.length) {
               throw new IllegalStateException("Assertion!");
            }
         }
      } while (!done);
      if (trace) {
         log.tracef("Successfully read array with size: %d", size);
      }
   }

   @Override
   public byte[] readByteArray(final int size) {
      byte[] result = new byte[size];
      readByteArray(result, size);
      return result;
   }

   public SocketAddress getServerAddress() {
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
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      TcpTransport that = (TcpTransport) o;

      if (serverAddress != null ? !serverAddress.equals(that.serverAddress) : that.serverAddress != null) {
         return false;
      }
      if (socket != null ? !socket.equals(that.socket) : that.socket != null) {
         return false;
      }

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
         if (socketInputStream != null) socketInputStream.close();
         if (socketOutputStream != null) socketOutputStream.close();
         if (socketChannel != null) socketChannel.close();
         if (socket != null) socket.close();
         if (saslClient != null) saslClient.dispose();
         if (trace) {
            log.tracef("Successfully closed socket: %s", socket);
         }
      } catch (IOException e) {
         invalid = true;
         log.errorClosingSocket(this, e);
         // Just in case an exception is thrown, make sure they're fully closed
         Util.close(socketInputStream, socketOutputStream, socketChannel);
         Util.close(socket);
      }
   }

   @Override
   public boolean isValid() {
      return !socket.isClosed() && !invalid;
   }

   public long getId() {
      return id;
   }

   @Override
   public byte[] dumpStream() {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      try {
         socket.setSoTimeout(5000);
         // Read 32kb at most
         for (int i = 0; i < 32768; i++) {
            int b = socketInputStream.read();
            if (b < 0) {
               break;
            }
            os.write(b);
         }
      } catch (IOException e) {
         // Ignore
      } finally {
        Util.close(socket);
      }
      return os.toByteArray();
   }

   @Override
   public SocketAddress getRemoteSocketAddress() {
      return socket.isClosed() ? null : serverAddress;
   }

   @Override
   public void invalidate() {
      invalid = true;
   }

   private interface IoSupplier<T> {
      T get() throws IOException;
   }

}
