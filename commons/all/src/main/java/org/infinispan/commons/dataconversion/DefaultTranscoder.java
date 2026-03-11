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
 * @since 9.2
 */
public final class DefaultTranscoder extends OneToManyTranscoder {

   protected static final Log logger = LogFactory.getLog(DefaultTranscoder.class);


   private final Marshaller marshaller;

   public DefaultTranscoder(Marshaller marshaller) {
      super(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OBJECT);
      this.marshaller = marshaller;
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(mainType)) {
            return contentType.equals(mainType) ? content : marshaller.objectToByteBuffer(content);
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
