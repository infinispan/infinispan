package org.infinispan.it.compatibility;

import org.infinispan.commons.dataconversion.MarshallerEncoder;

/**
 * @since 9.1
 */
public class MemcachedEncoder extends MarshallerEncoder {

   public MemcachedEncoder() {
      super(new SpyMemcachedCompatibleMarshaller());
   }

   @Override
   public short id() {
      return 1000;
   }
}
