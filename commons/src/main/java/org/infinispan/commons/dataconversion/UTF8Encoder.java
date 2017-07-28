package org.infinispan.commons.dataconversion;

import java.nio.charset.Charset;

/**
 * Encoder to/from UTF-8 content using the java string encoding mechanism.
 *
 * @since 9.1
 */
public class UTF8Encoder implements Encoder {

   private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

   public static final UTF8Encoder INSTANCE = new UTF8Encoder();

   @Override
   public Object toStorage(Object content) {
      if (content instanceof String) return String.class.cast(content).getBytes(CHARSET_UTF8);
      throw new EncodingException("Cannot encode " + content);
   }

   @Override
   public Object fromStorage(Object stored) {
      return new String((byte[]) stored, CHARSET_UTF8);
   }

   @Override
   public boolean isStorageFormatFilterable() {
      return false;
   }

   @Override
   public short id() {
      return EncoderIds.UTF8;
   }

}
