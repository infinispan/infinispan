package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.Marshaller;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author gustavonalle
 * @since 8.0
 */
public final class CodecUtils {

   private CodecUtils() {
   }

   public static boolean isIntCompatible(long value) {
      int narrowed = (int) value;
      return narrowed == value;
   }

   public static int toSeconds(long duration, TimeUnit timeUnit) {
      int seconds = (int) timeUnit.toSeconds(duration);
      long inverseDuration = timeUnit.convert(seconds, TimeUnit.SECONDS);

      if (duration > inverseDuration) {
         //Round up.
         seconds++;
      }
      return seconds;
   }

   static <T> T readUnmarshallByteArray(Transport transport, short status, List<String> whitelist) {
      byte[] bytes = transport.readArray();
      Marshaller marshaller = transport.getTransportFactory().getMarshaller();
      return MarshallerUtil.bytes2obj(marshaller, bytes, status, whitelist);
   }

}
