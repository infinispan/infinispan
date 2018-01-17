package org.infinispan.commons.dataconversion;

import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;

/**
 * @since 9.1
 */
public class GenericJbossMarshallerEncoder extends MarshallerEncoder {

   public static final GenericJbossMarshallerEncoder INSTANCE = new GenericJbossMarshallerEncoder();

   private GenericJbossMarshallerEncoder() {
      this(GenericJbossMarshallerEncoder.class.getClassLoader());
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
