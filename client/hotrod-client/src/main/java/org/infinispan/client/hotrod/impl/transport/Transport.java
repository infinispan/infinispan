package org.infinispan.client.hotrod.impl.transport;

import net.jcip.annotations.NotThreadSafe;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
@NotThreadSafe
public interface Transport {

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
}
