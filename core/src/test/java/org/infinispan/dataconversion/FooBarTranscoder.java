package org.infinispan.dataconversion;

import static java.util.Arrays.asList;
import static org.infinispan.commons.dataconversion.MediaType.fromString;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;

/**
 * @since 9.2
 */
public class FooBarTranscoder implements Transcoder {

   private final Set<MediaType> supportedTypes;

   public FooBarTranscoder() {
      supportedTypes = new HashSet<>(asList(fromString("application/foo"), fromString("application/bar")));
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      switch (destinationType.toString()) {
         case "application/foo":
            return content.toString().replaceAll("bar", "foo");
         case "application/bar":
            return content.toString().replaceAll("foo", "bar");
         default:
            throw new EncodingException("Not supported!");
      }
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }
}
