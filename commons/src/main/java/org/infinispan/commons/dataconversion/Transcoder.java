package org.infinispan.commons.dataconversion;

import java.util.Set;

/**
 * @since 9.2
 */
public interface Transcoder {

   /**
    * Transcodes content between two different {@link MediaType}.
    *
    * @param content         Content to transcode.
    * @param contentType     The {@link MediaType} of the content.
    * @param destinationType The target {@link MediaType} to convert.
    * @return the transcoded content.
    */
   Object transcode(Object content, MediaType contentType, MediaType destinationType);

   /**
    * @return all the {@link MediaType} handled by this Transcoder.
    */
   Set<MediaType> getSupportedMediaTypes();

   /**
    * @return true if the transcoder supports the conversion between supplied {@link MediaType}.
    */
   default boolean supportsConversion(MediaType mediaType, MediaType other) {
      return !mediaType.match(other) && supports(mediaType) && supports(other);
   }

   default boolean supports(MediaType mediaType) {
      return getSupportedMediaTypes().stream().anyMatch(m -> m.match(mediaType));
   }

}
