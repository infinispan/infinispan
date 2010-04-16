package org.infinispan.client.hotrod.impl;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class AbstractTransport implements Transport {

   private static Log log = LogFactory.getLog(AbstractTransport.class);

   public byte[] readArray() {
      int responseLength = readVInt();
      return readByteArray(responseLength);
   }

   @Override
   public String readString() {
      byte[] strContent = readArray();
      String readString = new String(strContent);
      if (log.isTraceEnabled()) {
         log.trace("Read string is: " + readString);
      }
      return readString;//todo take care of encoding here
   }

   @Override
   public long readLong() {
      //todo - optimize this not to create the longBytes on every call, but reuse it/cache it as class is NOT thread safe
      byte[] longBytes = readByteArray(8);
      long result = 0;
      for (byte longByte : longBytes) {
         result <<= 8;
         result ^= (long) longByte & 0xFF;
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

   @Override
   public int readUnsignedShort() {
      byte[] shortBytes = readByteArray(2);
      int result = 0;
      for (byte longByte : shortBytes) {
         result <<= 8;
         result ^= (long) longByte & 0xFF;
      }
      return result;
   }

   public void writeArray(byte[] toAppend) {
      writeVInt(toAppend.length);
      writeBytes(toAppend);
   }

   protected abstract void writeBytes(byte[] toAppend);
}
