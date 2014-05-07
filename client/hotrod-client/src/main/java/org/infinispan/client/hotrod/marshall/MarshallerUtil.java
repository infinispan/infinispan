package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;

import java.io.IOException;

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
         return (T) marshaller.objectFromByteBuffer(bytes);
      } catch (Exception e) {
         throw log.unableToUnmarshallBytes(Util.toHexString(bytes), e);
      }
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
