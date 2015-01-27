package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamConstants;

/**
 * @author Galder Zamarreño
 */
public final class MarshallerUtil {

   private static final Log log = LogFactory.getLog(MarshallerUtil.class, Log.class);

   private MarshallerUtil() {}

   @SuppressWarnings("unchecked")
   public static <T> T bytes2obj(Marshaller marshaller, byte[] bytes) {
      if (bytes == null) return null;
      try {
         Object ret = marshaller.objectFromByteBuffer(bytes);

         // No extra configuration is required for client when using compatibility mode,
         // and no different marshaller should be required to deal with standard serialization.
         // So, if the unmarshalled object is still a byte[], it could be a standard
         // serialized object, so check for stream magic
         if (ret instanceof byte[] && isJavaSerialized((byte[]) ret)) {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream((byte[]) ret))) {
               return (T) ois.readObject();
            } catch (Exception ee) {
               if (log.isDebugEnabled())
                  log.debugf("Standard deserialization not in use for %s", Util.printArray(bytes));
            }
         }

         return (T) ret;
      } catch (Exception e) {
         throw log.unableToUnmarshallBytes(Util.toHexString(bytes), e);
      }
   }

   private static boolean isJavaSerialized(byte[] bytes) {
      if (bytes.length > 2) {
         short magic = (short) ((bytes[1] & 0xFF) + (bytes[0] << 8));
         return magic == ObjectStreamConstants.STREAM_MAGIC;
      }

      return false;
   }

   static short getShort(byte[] b, int off) {
      return (short) ((b[off + 1] & 0xFF) + (b[off] << 8));
   }

   public static byte[] obj2bytes(Marshaller marshaller, Object o, boolean isKey, int estimateKeySize, int estimateValueSize) {
      try {
         return marshaller.objectToByteBuffer(o, isKey ? estimateKeySize : estimateValueSize);
      } catch (IOException ioe) {
         throw new HotRodClientException(
               "Unable to marshall object of type [" + o.getClass().getName() + "]", ioe);
      } catch (InterruptedException ie) {
         Thread.currentThread().interrupt();
         return null;
      }
   }

}
