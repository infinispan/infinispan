package org.infinispan.client.hotrod.impl;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class AbstractTransport implements Transport {

   public byte[] readArray() {
      int responseLength = readVInt();
      return readByteArray(responseLength);
   }

   @Override
   public String readString() {
      byte[] strContent = readArray();
      return new String(strContent);//todo take care of encoding here
   }

   @Override
   public long readLong() {
      byte[] longBytes = readByteArray(8);
      long result = 0;
      for (int i = 0; i < 8; i++) {
         result <<= 8;
         result ^= (long) longBytes[i] & 0xFF;
      }
      return result;
   }

   @Override
   public void writeLong(long longValue) {
      byte[] b = new byte[8];
      for (int i = 0; i < 8; i++) {
         b[7 - i] = (byte) (longValue >>> (i * 8));
      }
      writeBytes(b);
   }

   public void writeArray(byte[] toAppend) {
      writeVInt(toAppend.length);
      writeBytes(toAppend);
   }

   protected abstract void writeBytes(byte[] toAppend);
}
