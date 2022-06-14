package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_ZIP;
import static org.infinispan.commons.logging.Log.CONTAINER;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.util.Util;

/**
 * @author Ryan Emerson
 * @since 14.0
 */
public class ZipTranscoder extends AbstractTranscoder {

   private static final Set<MediaType> supportedTypes = new HashSet<>();
   static {
      supportedTypes.add(APPLICATION_OCTET_STREAM);
      supportedTypes.add(APPLICATION_OBJECT);
      supportedTypes.add(APPLICATION_UNKNOWN);
      supportedTypes.add(APPLICATION_ZIP);
   }

   @Override
   protected Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      if (content instanceof byte[]) {
         return content;
      }
      throw CONTAINER.unsupportedConversion(Util.toStr(content), contentType, destinationType);
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }
}
