package org.infinispan.rest.operations;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.util.Optional;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.encoding.DataConversion;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.exceptions.UnacceptableDataFormatException;
import org.infinispan.util.logging.LogFactory;

import io.netty.handler.codec.http.HttpResponseStatus;

abstract class AbstractOperations {

   private final static Log logger = LogFactory.getLog(AbstractOperations.class, Log.class);
   static final MurmurHash3 hashFunc = MurmurHash3.getInstance();

   final RestCacheManager<Object> restCacheManager;
   final RestServerConfiguration restServerConfiguration;

   AbstractOperations(RestServerConfiguration configuration, RestCacheManager<Object> cacheManager) {
      this.restServerConfiguration = configuration;
      this.restCacheManager = cacheManager;
   }

   RestResponseException createResponseException(Throwable exception) {
      Throwable rootCauseException = getRootCauseException(exception);

      return new RestResponseException(HttpResponseStatus.INTERNAL_SERVER_ERROR, rootCauseException.getMessage(), rootCauseException);
   }

   Throwable getRootCauseException(Throwable re) {
      if (re == null) return null;
      Throwable cause = re.getCause();
      if (cause != null)
         return getRootCauseException(cause);
      else
         return re;
   }

   MediaType tryNarrowMediaType(MediaType negotiated, AdvancedCache<?, ?> cache) {
      if (!negotiated.matchesAll()) return negotiated;
      MediaType storageMediaType = cache.getValueDataConversion().getStorageMediaType();

      if(storageMediaType == null) return negotiated;
      if (storageMediaType.equals(MediaType.APPLICATION_OBJECT)) return TEXT_PLAIN;
      if (storageMediaType.match(MediaType.APPLICATION_PROTOSTREAM)) return APPLICATION_JSON;

      return negotiated;
   }

   MediaType negotiateMediaType(String accept, String cacheName) throws UnacceptableDataFormatException {
      try {
         AdvancedCache<?, ?> cache = restCacheManager.getCache(cacheName);
         DataConversion valueDataConversion = cache.getValueDataConversion();

         Optional<MediaType> negotiated = MediaType.parseList(accept)
               .filter(valueDataConversion::isConversionSupported)
               .findFirst();

         return negotiated.map(m -> tryNarrowMediaType(m, cache))
               .orElseThrow(() -> logger.unsupportedDataFormat(accept));

      } catch (EncodingException e) {
         throw new UnacceptableDataFormatException();
      }
   }

}
