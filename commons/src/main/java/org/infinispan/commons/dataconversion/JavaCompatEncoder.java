package org.infinispan.commons.dataconversion;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;

/**
 * Encoder that uses the default java serialization to store objects unmarshalled.
 *
 * @since 9.2
 */
public class JavaCompatEncoder extends CompatModeEncoder {

   public JavaCompatEncoder(ClassWhiteList classWhiteList) {
      super(new JavaSerializationMarshaller(classWhiteList));
   }

   @Override
   public short id() {
      return EncoderIds.JAVA_COMPAT;
   }
}
