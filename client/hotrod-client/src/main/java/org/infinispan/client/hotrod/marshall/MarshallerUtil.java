package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;

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

}
