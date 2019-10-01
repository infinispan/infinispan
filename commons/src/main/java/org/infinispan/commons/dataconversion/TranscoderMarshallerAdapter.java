package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;

/**
 * Base class for transcoder between application/x-java-object and byte[] produced by a marshaller.
 *
 * @since 9.3
 */
public class TranscoderMarshallerAdapter extends OneToManyTranscoder {

   protected final static Log logger = LogFactory.getLog(TranscoderMarshallerAdapter.class, Log.class);


   private final Marshaller marshaller;

   public TranscoderMarshallerAdapter(Marshaller marshaller) {
      super(marshaller.mediaType(), MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_UNKNOWN);
      this.marshaller = marshaller;
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.equals(MediaType.APPLICATION_UNKNOWN) || contentType.equals(MediaType.APPLICATION_UNKNOWN)) {
            return content;
         }
         if (destinationType.match(marshaller.mediaType())) {
            return marshaller.objectToByteBuffer(content);
         }
         if (destinationType.match(MediaType.APPLICATION_OBJECT)) {
            return marshaller.objectFromByteBuffer((byte[]) content);
         }
      } catch (InterruptedException | IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
      throw CONTAINER.unsupportedContent(content);
   }

}
