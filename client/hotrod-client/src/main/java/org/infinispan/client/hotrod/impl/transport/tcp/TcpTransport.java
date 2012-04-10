/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.client.hotrod.impl.transport.tcp;

import static org.infinispan.io.UnsignedNumeric.readUnsignedInt;
import static org.infinispan.io.UnsignedNumeric.readUnsignedLong;
import static org.infinispan.io.UnsignedNumeric.writeUnsignedInt;
import static org.infinispan.io.UnsignedNumeric.writeUnsignedLong;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.transport.AbstractTransport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.util.Util;

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

   private static final Log log = LogFactory.getLog(TcpTransport.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Socket socket;
   private final SocketChannel socketChannel;
   private final InputStream socketInputStream;
   private final BufferedOutputStream socketOutputStream;
   private final InetSocketAddress serverAddress;
   private final long id = ID_COUNTER.incrementAndGet();

   private volatile boolean invalid;

   public TcpTransport(InetSocketAddress serverAddress, TransportFactory transportFactory) {
      super(transportFactory);
      this.serverAddress = serverAddress;
      try {
         socketChannel = SocketChannel.open();
         socket = socketChannel.socket();
         socket.connect(serverAddress, transportFactory.getConnectTimeout());
         socket.setTcpNoDelay(transportFactory.isTcpNoDelay());
         socket.setSoTimeout(transportFactory.getSoTimeout());
         socketInputStream = new BufferedInputStream(socket.getInputStream(), socket.getReceiveBufferSize());
         // ensure we don't send a packet for every output byte
         socketOutputStream = new BufferedOutputStream(socket.getOutputStream(), socket.getSendBufferSize());
      } catch (IOException e) {
         String message = String.format("Could not connect to server: %s", serverAddress);
         log.couldNotConnectToServer(serverAddress, e);
         throw new TransportException(message, e);
      }
   }

   @Override
   public void writeVInt(int vInt) {
      try {
         writeUnsignedInt(socketOutputStream, vInt);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e);
      }
   }

   @Override
   public void writeVLong(long l) {
      try {
         writeUnsignedLong(socketOutputStream, l);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e);
      }
   }

   @Override
   public long readVLong() {
      try {
         return readUnsignedLong(socketInputStream);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e);
      }
   }

   @Override
   public int readVInt() {
      try {
         return readUnsignedInt(socketInputStream);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e);
      }
   }

   @Override
   protected void writeBytes(byte[] toAppend) {
      try {
         socketOutputStream.write(toAppend);
         if (trace) {
            log.tracef("Wrote %d bytes", toAppend.length);
         }
      } catch (IOException e) {
         invalid = true;
         throw new TransportException("Problems writing data to stream", e);
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
         throw new TransportException("Problems writing data to stream", e);
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
         throw new TransportException(e);
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
         throw new TransportException(e);
      }
      if (resultInt == -1) {
         throw new TransportException("End of stream reached!");
      }
      return (short) resultInt;
   }

   @Override
   public void release() {
      try {
         socket.close();
      } catch (IOException e) {
         invalid = true;
         log.errorClosingSocket(this, e);
      }
   }

   @Override
   public byte[] readByteArray(final int size) {
      byte[] result = new byte[size];
      boolean done = false;
      int offset = 0;
      do {
         int read;
         try {
            int len = size - offset;
            if (trace) {
               log.tracef("Offset: %d, len=%d, size=%d", offset, len, size);
            }
            read = socketInputStream.read(result, offset, len);
         } catch (IOException e) {
            invalid = true;
            throw new TransportException(e);
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
         try {
            socket.close();
         } catch (IOException e) {
            // Ignore
         }
      }
      return os.toByteArray();
   }
}
