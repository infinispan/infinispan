package org.infinispan.rest.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;

/**
 * Passthrough transcoder for objects represented as byte[].
 */
public class OctetStreamToObject implements Transcoder {

   private static final Set<MediaType> supportedTypes = new HashSet<>();

   public OctetStreamToObject() {
      supportedTypes.add(APPLICATION_OBJECT);
      supportedTypes.add(APPLICATION_OCTET_STREAM);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      return content;
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }


}
