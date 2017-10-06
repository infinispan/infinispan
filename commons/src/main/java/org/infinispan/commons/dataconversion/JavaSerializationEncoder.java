package org.infinispan.commons.dataconversion;

import org.infinispan.commons.marshall.JavaSerializationMarshaller;

/**
 * Encoder based on the default java serialization.
 *
 * @since 9.1
 */
public class JavaSerializationEncoder extends MarshallerEncoder {

   public static final JavaSerializationEncoder INSTANCE = new JavaSerializationEncoder();

   private JavaSerializationEncoder() {
      super(new JavaSerializationMarshaller());
   }

   @Override
   public short id() {
      return EncoderIds.JAVA_SERIALIZATION;
   }
}
