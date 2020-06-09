package org.infinispan.jboss.marshalling.dataconversion;

import org.infinispan.commons.dataconversion.EncoderIds;
import org.infinispan.commons.dataconversion.MarshallerEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;

/**
 * @since 9.1
 * @deprecated Since 11.0, will be removed in 14.0. Set the storage media type and use transcoding instead.
 */
@Deprecated
public class GenericJbossMarshallerEncoder extends MarshallerEncoder {

   public GenericJbossMarshallerEncoder(GenericJBossMarshaller marshaller) {
      super(marshaller);
   }

   public GenericJbossMarshallerEncoder(ClassLoader classLoader) {
      super(new GenericJBossMarshaller(classLoader));
   }

   @Override
   public MediaType getStorageFormat() {
      return MediaType.APPLICATION_JBOSS_MARSHALLING;
   }

   @Override
   public short id() {
      return EncoderIds.GENERIC_MARSHALLER;
   }
}
