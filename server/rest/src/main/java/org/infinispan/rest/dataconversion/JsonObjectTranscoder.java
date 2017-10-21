package org.infinispan.rest.dataconversion;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @since 9.2
 */
public class JsonObjectTranscoder implements Transcoder {

   protected final static Log logger = LogFactory.getLog(JsonObjectTranscoder.class, Log.class);

   private final ObjectMapper jsonMapper = new ObjectMapper().enableDefaultTypingAsProperty(ObjectMapper.DefaultTyping.NON_FINAL, "_type");

   private static final Set<MediaType> supportedTypes = new HashSet<>();

   public JsonObjectTranscoder() {
      supportedTypes.add(MediaType.APPLICATION_JSON);
      supportedTypes.add(MediaType.APPLICATION_OBJECT);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(MediaType.APPLICATION_JSON)) {
         try {
            return jsonMapper.writeValueAsString(content);
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }
      if (destinationType.match(MediaType.APPLICATION_OBJECT)) {
         try {
            if (content instanceof byte[]) {
               return jsonMapper.readValue((byte[]) content, Object.class);
            }
            return jsonMapper.readValue((String) content, Object.class);
         } catch (IOException e) {
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
