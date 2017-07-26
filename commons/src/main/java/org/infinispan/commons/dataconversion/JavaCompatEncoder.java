package org.infinispan.commons.dataconversion;

import org.infinispan.commons.marshall.JavaSerializationMarshaller;

/**
 * Encoder that uses the default java serialization to store objects unmarshalled.
 *
 * @since 9.2
 */
public class JavaCompatEncoder extends CompatModeEncoder {

   public static final JavaCompatEncoder INSTANCE = new JavaCompatEncoder();

   private JavaCompatEncoder() {
      super(new JavaSerializationMarshaller());
   }
}
