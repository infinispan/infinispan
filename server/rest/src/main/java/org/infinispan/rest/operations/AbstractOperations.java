package org.infinispan.rest.operations;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.operations.exceptions.UnacceptableDataFormatException;

import io.netty.handler.codec.http.HttpResponseStatus;

abstract class AbstractOperations {

   static final MurmurHash3 hashFunc = MurmurHash3.getInstance();

   final RestCacheManager<Object> restCacheManager;
   final RestServerConfiguration restServerConfiguration;
   private final Set<String> supported = new HashSet<>();

   AbstractOperations(RestServerConfiguration configuration, RestCacheManager<Object> cacheManager) {
      this.restServerConfiguration = configuration;
      this.restCacheManager = cacheManager;
      EncoderRegistry encoderRegistry = restCacheManager.encoderRegistry();
      supported.addAll(encoderRegistry.getSupportedMediaTypes());
   }

   RestResponseException createResponseException(Throwable exception) {
      Throwable rootCauseException = getRootCauseException(exception);

      return new RestResponseException(HttpResponseStatus.INTERNAL_SERVER_ERROR, rootCauseException.getMessage(), rootCauseException);
   }

   private Throwable getRootCauseException(Throwable re) {
      if (re == null) return null;
      Throwable cause = re.getCause();
      if (cause instanceof RuntimeException)
         return getRootCauseException(cause);
      else
         return re;
   }

   MediaType getMediaType(InfinispanRequest request) throws UnacceptableDataFormatException {
      Optional<String> maybeContentType = request.getAcceptContentType();
      if (maybeContentType.isPresent()) {
         try {
            String contents = maybeContentType.get();
            if (contents.equals("*/*")) return null;
            for (String content : contents.split(" *, *")) {
               MediaType mediaType = MediaType.fromString(content);
               if (supported.contains(mediaType.getTypeSubtype())) {
                  return mediaType;
               }
            }
            throw new UnacceptableDataFormatException();
         } catch (EncodingException e) {
            throw new UnacceptableDataFormatException();
         }
      }
      return null;
   }

}
