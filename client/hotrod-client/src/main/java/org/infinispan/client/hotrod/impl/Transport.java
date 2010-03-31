package org.infinispan.client.hotrod.impl;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public interface Transport {

   public void writeByteArray(byte[] toAppend);

   public void writeByte(short toWrite);

   public void writeVInt(int length);

   public void writeVLong(long l);

   public long readVLong();

   public int readVInt();

   public void flush();

   public short readByte();

   public void release();

   /**
    * reads an vint which is size; then an array having that size.
    */
   public byte[] readByteArray();

   String readString();
}
