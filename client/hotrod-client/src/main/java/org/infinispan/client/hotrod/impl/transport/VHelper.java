package org.infinispan.client.hotrod.impl.transport;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Helper for handling v-operations.
 *
 * @author Mircea.Markus@jboss.com
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

   public static Object newInstance(String clazz) {
      try {
         return Util.getInstance(clazz);
      } catch (Exception e) {
         throw new HotRodClientException("Could not instantiate class: " + clazz, e);
      }
   }
}
