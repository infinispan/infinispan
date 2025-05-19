package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;

/**
 * Base class for transcoder between application/x-java-object and byte[] produced by a marshaller.
 *
 * @since 9.3
 */
public class TranscoderMarshallerAdapter extends OneToManyTranscoder {

   protected static final Log logger = LogFactory.getLog(TranscoderMarshallerAdapter.class, Log.class);


   private final Marshaller marshaller;

   public TranscoderMarshallerAdapter(Marshaller marshaller) {
      super(marshaller.mediaType(), MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_UNKNOWN);
      this.marshaller = marshaller;
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.equals(MediaType.APPLICATION_UNKNOWN) || contentType.equals(MediaType.APPLICATION_UNKNOWN)) {
            return content;
         }
         if (destinationType.match(marshaller.mediaType())) {
            return contentType.equals(marshaller.mediaType()) ? content : marshaller.objectToByteBuffer(content);
         }
         if (destinationType.match(MediaType.APPLICATION_OBJECT)) {
            return marshaller.objectFromByteBuffer((byte[]) content);
         }
      } catch (InterruptedException | IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
      throw CONTAINER.unsupportedConversion(Util.toStr(content), contentType, destinationType);
   }

}
