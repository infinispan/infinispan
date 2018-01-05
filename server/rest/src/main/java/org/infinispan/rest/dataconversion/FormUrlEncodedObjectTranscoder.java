package org.infinispan.rest.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class FormUrlEncodedObjectTranscoder implements Transcoder {

   private static final Log logger = LogFactory.getLog(FormUrlEncodedObjectTranscoder.class, Log.class);

   private static final Set<MediaType> SUPPORTED_TYPES = new HashSet<>();

   static {
      SUPPORTED_TYPES.add(APPLICATION_OBJECT);
      SUPPORTED_TYPES.add(APPLICATION_WWW_FORM_URLENCODED);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(APPLICATION_OBJECT)) {
            String decoded = URLDecoder.decode(asString(content), UTF_8.toString());
            return content instanceof byte[] ? decoded.getBytes(UTF_8) : decoded;
         }
         if (destinationType.match(APPLICATION_WWW_FORM_URLENCODED)) {
            return URLEncoder.encode(asString(content), UTF_8.toString());
         }
      } catch (UnsupportedEncodingException e) {
         throw logger.errorTranscoding(e);
      }
      throw logger.cannotFindTranscoder(contentType, destinationType);
   }

   private String asString(Object content) {
      return content instanceof byte[] ? new String((byte[]) content, UTF_8) : content.toString();
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return SUPPORTED_TYPES;
   }
}
