package org.infinispan.jboss.marshalling.dataconversion;

import java.io.IOException;

import org.infinispan.commons.dataconversion.DefaultTranscoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;

/**
 * Extension of {@link DefaultTranscoder} that utilises {@link GenericJBossMarshaller} for byte conversion.
 *
 * @since 10.0
 */
// TODO register with EncoderRegitry when module present
// TODO do we need this?
public final class JBossMarshallerEnabledTranscoder extends DefaultTranscoder {

   private final GenericJBossMarshaller jbossMarshaller;

   public JBossMarshallerEnabledTranscoder(GenericJBossMarshaller marshaller, JavaSerializationMarshaller javaMarshaller) {
      super(javaMarshaller);
      this.jbossMarshaller = marshaller;
   }

   @Override
   protected Object convertJavaToOctetStream(Object content, MediaType contentType) throws InterruptedException, IOException {
      return StandardConversions.convertJavaToOctetStream(content, contentType, jbossMarshaller);
   }

   @Override
   protected Object tryDeserialize(byte[] content) {
      try {
         return jbossMarshaller.objectFromByteBuffer(content);
      } catch (IOException | ClassNotFoundException e1) {
         return super.tryDeserialize(content);
      }
   }
}
