package org.infinispan.client.hotrod.impl.transport;

import org.infinispan.io.UnsignedNumeric;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class VHelper {
   public static int readVInt(InputStream is) {
      try {
         return UnsignedNumeric.readUnsignedInt(is);
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public static void writeVInt(int toWrite, OutputStream os) {
      try {
         UnsignedNumeric.writeUnsignedInt(os, toWrite);
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }

   public static long readVLong(InputStream is) {
      try {
         return UnsignedNumeric.readUnsignedLong(is);
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }


   public static void writeVLong(long toWrite, OutputStream os) {
      try {
         UnsignedNumeric.writeUnsignedLong(os, toWrite);
      } catch (IOException e) {
         throw new TransportException(e);
      }
   }
}
