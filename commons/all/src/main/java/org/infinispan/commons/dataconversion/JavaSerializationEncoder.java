package org.infinispan.commons.dataconversion;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;

/**
 * Encoder based on the default java serialization.
 *
 * @since 9.1
 * @deprecated Since 11.0, will be removed in 14.0. Set the storage media type and use transcoding instead.
 */
@Deprecated
public class JavaSerializationEncoder extends MarshallerEncoder {

   public JavaSerializationEncoder(ClassAllowList classAllowList) {
      super(new JavaSerializationMarshaller(classAllowList));
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
