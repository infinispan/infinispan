package org.infinispan.rest.dataconversion;

import static org.infinispan.rest.JSONConstants.TYPE;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;
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

   private final ObjectMapper jsonMapper = new ObjectMapper().setDefaultTyping(
         new ObjectMapper.DefaultTypeResolverBuilder(ObjectMapper.DefaultTyping.NON_FINAL) {
            {
               init(JsonTypeInfo.Id.CLASS, null);
               inclusion(JsonTypeInfo.As.PROPERTY);
               typeProperty(TYPE);
            }

            @Override
            public boolean useForType(JavaType t) {
               return !t.isContainerType() && super.useForType(t);
            }
         });

   private static final Set<MediaType> supportedTypes = new HashSet<>();

   public JsonObjectTranscoder() {
      supportedTypes.add(MediaType.APPLICATION_JSON);
      supportedTypes.add(MediaType.APPLICATION_OBJECT);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(MediaType.APPLICATION_JSON)) {
         try {
            if (content instanceof byte[]) {
               String contentAsString = new String((byte[]) content, destinationType.getCharset());
               return jsonMapper.writeValueAsString(contentAsString);
            }
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
