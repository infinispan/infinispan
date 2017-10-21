package org.infinispan.commons.dataconversion;

/**
 * Encoder that does not change the content.
 *
 * @since 9.1
 */
public class IdentityEncoder implements Encoder {

   public static final IdentityEncoder INSTANCE = new IdentityEncoder();

   @Override
   public Object toStorage(Object content) {
      return content;
   }

   @Override
   public Object fromStorage(Object content) {
      return content;
   }

   @Override
   public boolean isStorageFormatFilterable() {
      return false;
   }

   @Override
   public MediaType getStorageFormat() {
      return null;
   }

   @Override
   public short id() {
      return EncoderIds.IDENTITY;
   }

}
