package org.infinispan.client.hotrod.impl.transport;

/**
 * Transport abstraction.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface Transport {

   public TransportFactory getTransportFactory();

   public void writeArray(byte[] toAppend);

   public void writeByte(short toWrite);

   public void writeVInt(int vint);

   public void writeVLong(long l);

   public long readVLong();

   public int readVInt();

   public void flush();

   public short readByte();

   public void release();

   /**
    * reads an vint which is size; then an array having that size.
    */
   public byte[] readArray();

   String readString();

   byte[] readByteArray(int size);

   long readLong();

   void writeLong(long longValue);

   int readUnsignedShort();

   int read4ByteInt();

   void writeString(String string);
}
