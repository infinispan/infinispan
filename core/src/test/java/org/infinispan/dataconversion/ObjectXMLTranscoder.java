package org.infinispan.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;

/**
 * @since 9.2
 */
class ObjectXMLTranscoder implements Transcoder {

   private Set<MediaType> supported;

   private TestXMLParser parser = new TestXMLParser();

   ObjectXMLTranscoder() {
      supported = new HashSet<>();
      supported.add(APPLICATION_XML);
      supported.add(APPLICATION_OBJECT);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_OBJECT)) {
         try {
            return parser.parse((String) content);
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
      if (destinationType.match(APPLICATION_XML)) {
         try {
            return parser.serialize((Map<String, String>) content);
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
      return null;
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supported;
   }
}
