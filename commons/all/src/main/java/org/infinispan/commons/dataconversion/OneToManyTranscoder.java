package org.infinispan.commons.dataconversion;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for {@link Transcoder} that converts between a single format and multiple other formats and back.
 */
public abstract class OneToManyTranscoder extends AbstractTranscoder {

   protected final MediaType mainType;

   protected final Set<MediaType> supportedTypes = new HashSet<>();

   public OneToManyTranscoder(MediaType mainType, MediaType... supportedConversions) {
      this.mainType = mainType;
      this.supportedTypes.add(mainType);
      Collections.addAll(supportedTypes, supportedConversions);
   }

   private boolean in(MediaType mediaType, Set<MediaType> set) {
      return set.stream().anyMatch(s -> s.match(mediaType));
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }

   @Override
   public boolean supportsConversion(MediaType mediaType, MediaType other) {
      return mediaType.match(mainType) && in(other, supportedTypes) ||
            other.match(mainType) && in(mediaType, supportedTypes);
   }

}
