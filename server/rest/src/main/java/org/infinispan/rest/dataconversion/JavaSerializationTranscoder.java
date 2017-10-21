package org.infinispan.rest.dataconversion;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;

/**
 * @since 9.2
 */
public class JavaSerializationTranscoder implements Transcoder {

   private final Set<MediaType> supported;
   private static final JavaSerializationMarshaller marshaller = new JavaSerializationMarshaller();

   public JavaSerializationTranscoder() {
      supported = new HashSet<>();
      supported.add(MediaType.APPLICATION_OBJECT);
      supported.add(MediaType.APPLICATION_SERIALIZED_OBJECT);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(MediaType.APPLICATION_SERIALIZED_OBJECT)) {
            return marshaller.objectToByteBuffer(content);
         }
         if (destinationType.match(MediaType.APPLICATION_OBJECT)) {
            return marshaller.objectFromByteBuffer((byte[]) content);
         }
      } catch (InterruptedException | IOException | ClassNotFoundException e) {
         throw new EncodingException("Cannot transcode " + content, e);
      }
      return null;
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supported;
   }
}
