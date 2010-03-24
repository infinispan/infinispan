package hotrod.impl;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public interface Transport {

   public void writeByteArray(byte... toAppend);

   /**
    * Treats the tailing byte as an unsigned byte.
    */
   public void appendUnsignedByte(short requestMagic);

   public void writeVInt(int length);

   public void writeVLong(long l);

   public long readVLong();

   public int readVInt();

   public void flush();

   public byte readByte();

   public void release();

   /**
    * reads an vint which is size; then an array having that size.
    * @param bufferToFill
    */
   public byte[] readByteArray(byte[] bufferToFill);
}
