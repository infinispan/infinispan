package org.infinispan.notifications.cachelistener;

import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.marshall.core.EncoderRegistryImpl;

class FakeEncoderRegistry extends EncoderRegistryImpl {
   @Override
   public Object convert(Object o, MediaType from, MediaType to) {
      return o;
   }

   @Override
   public Transcoder getTranscoder(MediaType mediaType, MediaType another) {
      return new Transcoder() {
         @Override
         public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
            return content;
         }

         @Override
         public Set<MediaType> getSupportedMediaTypes() {
            return Collections.emptySet();
         }
      };
   }
}
