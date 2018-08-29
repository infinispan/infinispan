package org.infinispan.it.endpoints;

import org.infinispan.commons.dataconversion.MarshallerEncoder;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 9.1
 */
public class MemcachedEncoder extends MarshallerEncoder {

   public MemcachedEncoder() {
      super(new SpyMemcachedMarshaller());
   }

   @Override
   public MediaType getStorageFormat() {
      return MediaType.APPLICATION_SERIALIZED_OBJECT;
   }

   @Override
   public short id() {
      return 1000;
   }
}
