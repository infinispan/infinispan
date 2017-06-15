package org.infinispan.server.memcached;

import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;

/**
 * @since 9.1
 */
public class MemcachedCompatEncoder extends CompatModeEncoder {

   public MemcachedCompatEncoder(Marshaller marshaller) {
      super(marshaller == null ? new JavaSerializationMarshaller() : marshaller);
   }

}
