package org.infinispan.rest.resources;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.util.Arrays;
import java.util.Optional;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.exceptions.UnacceptableDataFormatException;

/**
 * since 10.0
 */
final class MediaTypeUtils {

   private MediaTypeUtils() {
   }

   /**
    * Negotiates the {@link MediaType} to be used during the request execution, restricting to some allowed types.
    *
    * @param restRequest the {@link RestRequest} with headers
    * @param accepted the accepted MediaTypes
    * @return one of the accepted MediaTypes if present in the 'Accept' header, or the first provided otherwise
    */
   static MediaType negotiateMediaType(RestRequest restRequest, MediaType... accepted) {
      String acceptHeader = restRequest.getAcceptHeader();
      if (accepted.length == 0) throw new IllegalArgumentException("Accepted should be provided");

      if (acceptHeader.equals(MediaType.MATCH_ALL_TYPE)) return accepted[0];

      Optional<MediaType> found = MediaType.parseList(acceptHeader).filter(c -> Arrays.stream(accepted).anyMatch(m -> m.match(c))).findFirst();
      return found.orElseThrow(() -> Log.REST.unsupportedDataFormat(acceptHeader));
   }

   /**
    * Negotiates the {@link MediaType} to be used during the request execution
    *
    * @param cache the {@link AdvancedCache} associated with the request
    * @param restRequest the {@link RestRequest} with the headers
    * @return The negotiated MediaType
    * @throws UnacceptableDataFormatException if no suitable {@link MediaType} could be found.
    */
   static MediaType negotiateMediaType(AdvancedCache<?, ?> cache, EncoderRegistry registry, RestRequest restRequest) throws UnacceptableDataFormatException {
      try {
         String accept = restRequest.getAcceptHeader();
         MediaType storageMedia = cache.getValueDataConversion().getStorageMediaType();
         Optional<MediaType> negotiated = MediaType.parseList(accept)
               .filter(media -> registry.isConversionSupported(storageMedia, media))
               .findFirst();

         return negotiated.map(m -> {
            if (!m.matchesAll()) return m;
            MediaType storageMediaType = cache.getValueDataConversion().getStorageMediaType();

            if (storageMediaType == null) return m;
            if (storageMediaType.equals(MediaType.APPLICATION_OBJECT)) return TEXT_PLAIN;
            if (storageMediaType.match(MediaType.APPLICATION_PROTOSTREAM)) return APPLICATION_JSON;
            return m;
         }).orElseThrow(() -> Log.REST.unsupportedDataFormat(accept));
      } catch (EncodingException e) {
         throw new UnacceptableDataFormatException();
      }
   }

}
