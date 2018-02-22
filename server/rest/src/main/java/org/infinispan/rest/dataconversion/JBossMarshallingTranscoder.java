package org.infinispan.rest.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JBOSS_MARSHALLING;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.StandardConversions.decodeObjectContent;
import static org.infinispan.commons.dataconversion.StandardConversions.decodeOctetStream;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.GenericJbossMarshallerEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Transcode between application/x-jboss-marshalling and commons formats
 *
 * @since 9.2
 */
public class JBossMarshallingTranscoder extends OneToManyTranscoder {

   protected final static Log logger = LogFactory.getLog(JBossMarshallingTranscoder.class, Log.class);
   private final Encoder encoder;

   public JBossMarshallingTranscoder(EncoderRegistry encoderRegistry) {
      super(APPLICATION_JBOSS_MARSHALLING, APPLICATION_OCTET_STREAM, TEXT_PLAIN, APPLICATION_OBJECT);
      encoder = encoderRegistry.getEncoder(GenericJbossMarshallerEncoder.class, null);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(MediaType.APPLICATION_JBOSS_MARSHALLING)) {
         Object decoded = content;
         if (contentType.match(APPLICATION_OBJECT)) {
            decoded = decodeObjectContent(content, contentType);
         }
         if (contentType.match(APPLICATION_OCTET_STREAM)) {
            decoded = decodeOctetStream(content, destinationType);
         }
         return encoder.toStorage(decoded);
      }
      if (destinationType.match(MediaType.APPLICATION_OCTET_STREAM)) {
         return encoder.fromStorage(content);
      }
      if (destinationType.match(MediaType.TEXT_PLAIN)) {
         String fromStorage = encoder.fromStorage(content).toString();
         return fromStorage.getBytes(destinationType.getCharset());
      }
      throw logger.unsupportedContent(content);
   }

}
