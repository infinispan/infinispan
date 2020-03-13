package org.infinispan.commons.dataconversion;

import java.util.Set;

/**
 * Converts content between two or more {@link MediaType}s.
 *
 * <p>Note: A transcoder must be symmetric: if it can convert from media type X to media type Y,
 * it must also be able to convert from Y to X.</p>
 *
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
    * @return {@code true} if the transcoder supports the conversion between the supplied {@link MediaType}s.
    */
   default boolean supportsConversion(MediaType mediaType, MediaType other) {
      return !mediaType.match(other) && supports(mediaType) && supports(other);
   }

   /**
    * @return {@code true} iff the transcoder supports the conversion to and from the given {@link MediaType}.
    */
   default boolean supports(MediaType mediaType) {
      return getSupportedMediaTypes().stream().anyMatch(m -> m.match(mediaType));
   }

}
