package org.infinispan.encoding.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.AbstractTranscoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * <p>
 * Performs conversions where there is no direct transcoder, but there are two transcoders available:
 * <ul>
 *    <li>one from source media type to <b>application/x-java-object</b>
 *    <li>one from <b>application/x-java-object</b> to the destination media type
 * </ul>
 * </p>
 *
 * @since 11.0
 */
public class TwoStepTranscoder extends AbstractTranscoder {

   private static final Log logger = LogFactory.getLog(TwoStepTranscoder.class, Log.class);
   private final Transcoder transcoder1;
   private final Transcoder transcoder2;
   private final HashSet<MediaType> supportedMediaTypes;

   public TwoStepTranscoder(Transcoder transcoder1, Transcoder transcoder2) {
      this.transcoder1 = transcoder1;
      this.transcoder2 = transcoder2;

      supportedMediaTypes = new HashSet<>(this.transcoder1.getSupportedMediaTypes());
      supportedMediaTypes.addAll(transcoder2.getSupportedMediaTypes());
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      if (transcoder1.supportsConversion(contentType, APPLICATION_OBJECT)
            && transcoder2.supportsConversion(APPLICATION_OBJECT, destinationType)) {
         Object object = transcoder1.transcode(content, contentType, APPLICATION_OBJECT);
         return transcoder2.transcode(object, APPLICATION_OBJECT, destinationType);
      }
      if (transcoder2.supportsConversion(contentType, APPLICATION_OBJECT)
            && transcoder1.supportsConversion(APPLICATION_OBJECT, destinationType)) {
         Object object = transcoder2.transcode(content, contentType, APPLICATION_OBJECT);
         return transcoder1.transcode(object, APPLICATION_OBJECT, destinationType);
      }

      throw logger.unsupportedContent(TwoStepTranscoder.class.getSimpleName(), content);
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedMediaTypes;
   }

   @Override
   public boolean supportsConversion(MediaType mediaType, MediaType other) {
      return (transcoder1.supportsConversion(mediaType, APPLICATION_OBJECT) &&
            transcoder2.supportsConversion(APPLICATION_OBJECT, other)) ||
            (transcoder2.supportsConversion(mediaType, APPLICATION_OBJECT) &&
                  transcoder1.supportsConversion(APPLICATION_OBJECT, other));
   }
}
