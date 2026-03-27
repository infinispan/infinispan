package org.infinispan.commons.dataconversion;

/**
 * Handles (ignores) conversions for {@link MediaType#APPLICATION_UNKNOWN}.
 *
 * @since 16.2
 * @deprecated Will be removed in a future version together with {@link MediaType#APPLICATION_UNKNOWN}.
 */
public final class UnknownTranscoder extends OneToManyTranscoder {

   public static final UnknownTranscoder INSTANCE = new UnknownTranscoder();

   private UnknownTranscoder() {
      super(MediaType.APPLICATION_UNKNOWN, MediaType.MATCH_ALL);
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      return content;
   }
}
