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

   protected static final Log logger = LogFactory.getLog(TranscoderMarshallerAdapter.class);


   private final Marshaller marshaller;

   public TranscoderMarshallerAdapter(Marshaller marshaller) {
      super(marshaller.mediaType(), MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_UNKNOWN, MediaType.TEXT_PLAIN);
      this.marshaller = marshaller;
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.equals(MediaType.APPLICATION_UNKNOWN) || contentType.equals(MediaType.APPLICATION_UNKNOWN)) {
            return content;
         }
         if (destinationType.match(marshaller.mediaType())) {
            if (contentType.equals(marshaller.mediaType())) return content;
            if (contentType.match(MediaType.TEXT_PLAIN)) {
               return marshaller.objectToByteBuffer(StandardConversions.convertTextToObject(content, contentType));
            }
            return marshaller.objectToByteBuffer(content);
         }
         if (destinationType.match(MediaType.APPLICATION_OBJECT)) {
            return marshaller.objectFromByteBuffer((byte[]) content);
         }
         if (destinationType.match(MediaType.TEXT_PLAIN)) {
            Object obj = marshaller.objectFromByteBuffer((byte[]) content);
            return obj.toString().getBytes(destinationType.getCharset());
         }
      } catch (InterruptedException | IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
      throw CONTAINER.unsupportedConversion(Util.toStr(content), contentType, destinationType);
   }

}
