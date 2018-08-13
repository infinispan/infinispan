package org.infinispan.commons.dataconversion;

import java.nio.charset.StandardCharsets;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Encoder to/from UTF-8 content using the java string encoding mechanism.
 *
 * @since 9.1
 */
public class UTF8Encoder implements Encoder {

   private static final Log log = LogFactory.getLog(UTF8Encoder.class);

   public static final UTF8Encoder INSTANCE = new UTF8Encoder();

   @Override
   public Object toStorage(Object content) {
      if (content instanceof String) {
         return String.class.cast(content).getBytes(StandardCharsets.UTF_8);
      }

      throw log.unsupportedContent(content);
   }

   @Override
   public Object fromStorage(Object stored) {
      return new String((byte[]) stored, StandardCharsets.UTF_8);
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
