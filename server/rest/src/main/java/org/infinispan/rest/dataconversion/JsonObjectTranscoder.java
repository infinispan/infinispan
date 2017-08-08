package org.infinispan.rest.dataconversion;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @since 9.2
 */
public class JsonObjectTranscoder implements Transcoder {

   protected final static Log logger = LogFactory.getLog(JsonObjectTranscoder.class, Log.class);
   private final MediaType jsonMediaType;
   private final MediaType objectMediaType;

   private static class JsonMapperHolder {
      static final ObjectMapper jsonMapper = new ObjectMapper();
   }

   private static final Set<MediaType> supportedTypes = new HashSet<>();

   public JsonObjectTranscoder() {
      jsonMediaType = MediaType.APPLICATION_JSON;
      objectMediaType = MediaType.APPLICATION_OBJECT;
      supportedTypes.add(jsonMediaType);
      supportedTypes.add(objectMediaType);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(jsonMediaType)) {
         try {
            return JsonMapperHolder.jsonMapper.writeValueAsString(content);
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }
      if (destinationType.match(objectMediaType)) {
         try {
            Optional<String> classType = destinationType.getParameter("type");
            if (!classType.isPresent()) {
               throw new EncodingException("No type specified!");
            }
            return JsonMapperHolder.jsonMapper.readValue((String) content, Class.forName(classType.get()));

         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
      }
      return null;
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }
}
