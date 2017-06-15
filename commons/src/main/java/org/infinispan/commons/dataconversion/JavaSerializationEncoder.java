package org.infinispan.commons.dataconversion;

import org.infinispan.commons.marshall.JavaSerializationMarshaller;

/**
 * Encoder based on the default java serialization.
 *
 * @since 9.1
 */
public class JavaSerializationEncoder extends MarshallerEncoder {

   public JavaSerializationEncoder() {
      super(new JavaSerializationMarshaller());
   }
}
