package org.infinispan.commons.dataconversion;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;

/**
 * Encoder based on the default java serialization.
 *
 * @since 9.1
 */
public class JavaSerializationEncoder extends MarshallerEncoder {

   public JavaSerializationEncoder(ClassWhiteList classWhiteList) {
      super(new JavaSerializationMarshaller(classWhiteList));
   }

   @Override
   public MediaType getStorageFormat() {
      return MediaType.APPLICATION_SERIALIZED_OBJECT;
   }

   @Override
   public short id() {
      return EncoderIds.JAVA_SERIALIZATION;
   }
}
