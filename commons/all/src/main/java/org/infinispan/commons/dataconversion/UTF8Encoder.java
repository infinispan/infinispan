package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.logging.Log.CONTAINER;

import org.infinispan.commons.util.Util;

/**
 * Encoder to/from UTF-8 content using the java string encoding mechanism.
 *
 * @since 9.1
 * @deprecated Since 12.1, to be removed in a future version.
 */
@Deprecated
public class UTF8Encoder implements Encoder {

   public static final UTF8Encoder INSTANCE = new UTF8Encoder();
   private static final MediaType UTF8 = MediaType.fromString("text/plain; charset=utf-8");

   @Override
   public Object toStorage(Object content) {
      if (content instanceof String) {
         return String.class.cast(content).getBytes(UTF_8);
      }

      throw CONTAINER.unsupportedConversion(Util.toStr(content), UTF8);
   }

   @Override
   public Object fromStorage(Object stored) {
      return new String((byte[]) stored, UTF_8);
   }

   @Override
   public boolean isStorageFormatFilterable() {
      return false;
   }

   @Override
   public MediaType getStorageFormat() {
      return MediaType.TEXT_PLAIN;
   }

   @Override
   public short id() {
      return EncoderIds.UTF8;
   }
}
