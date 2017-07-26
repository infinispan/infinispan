package org.infinispan.server.memcached;

import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;

/**
 * @since 9.1
 * @deprecated Use {@link org.infinispan.commons.dataconversion.JavaCompatEncoder} instead.
 */
public class MemcachedCompatEncoder extends CompatModeEncoder {

   public static final MemcachedCompatEncoder INSTANCE = new MemcachedCompatEncoder();

   MemcachedCompatEncoder() {
      super(new JavaSerializationMarshaller());
   }

}
