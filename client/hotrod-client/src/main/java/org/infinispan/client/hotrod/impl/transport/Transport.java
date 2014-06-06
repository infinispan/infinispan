package org.infinispan.client.hotrod.impl.transport;

import java.net.SocketAddress;

/**
 * Transport abstraction.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface Transport {

   TransportFactory getTransportFactory();

   void writeArray(byte[] toAppend);

   void writeByte(short toWrite);

   void writeVInt(int vint);

   void writeVLong(long l);

   long readVLong();

   int readVInt();

   void flush();

   short readByte();

   @Deprecated
   void release();

   /**
    * reads an vint which is size; then an array having that size.
    */
   byte[] readArray();

   String readString();

   byte[] readByteArray(int size);

   long readLong();

   void writeLong(long longValue);

   int readUnsignedShort();

   int read4ByteInt();

   void writeString(String string);

   byte[] dumpStream();

   /**
    * Returns the address of the endpoint this transport is connected to, or
    * <code>null</code> if it is unconnected.
    *
    * @return a <code>SocketAddress</code> reprensenting the remote endpoint
    *         of this transport, or <code>null</code> if it is not connected
    *         yet.
    */
   SocketAddress getRemoteSocketAddress();

   /**
    * Invalidates transport instance.
    */
   void invalidate();

}
