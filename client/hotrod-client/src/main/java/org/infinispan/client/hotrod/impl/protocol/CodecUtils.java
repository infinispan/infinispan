package org.infinispan.client.hotrod.impl.protocol;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.Marshaller;

import io.netty.buffer.ByteBuf;

/**
 * @author gustavonalle
 * @since 8.0
 */
public final class CodecUtils {

   private CodecUtils() {
   }

   static boolean isGreaterThan4bytes(long value) {
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

   static <T> T readUnmarshallByteArray(ByteBuf buf, short status, ClassWhiteList whitelist, Marshaller marshaller) {
      byte[] bytes = ByteBufUtil.readArray(buf);
      return MarshallerUtil.bytes2obj(marshaller, bytes, status, whitelist);
   }

}
