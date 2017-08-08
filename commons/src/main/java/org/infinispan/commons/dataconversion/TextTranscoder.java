package org.infinispan.commons.dataconversion;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @since 9.2
 */
public class TextTranscoder implements Transcoder {

   public static final TextTranscoder INSTANCE = new TextTranscoder();

   private final Set<MediaType> supported = new HashSet<>();

   public TextTranscoder() {
      supported.add(MediaType.TEXT_PLAIN);
      supported.add(MediaType.APPLICATION_JSON);
      supported.add(MediaType.APPLICATION_XML);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      Optional<String> optDestinationCharset = destinationType.getParameter("charset");
      if (!optDestinationCharset.isPresent()) return content;
      if (content instanceof byte[]) {
         Charset sourceCharset = contentType.getParameter("charset").map(Charset::forName).orElse(Charset.defaultCharset());
         Charset destinationCharset = Charset.forName(optDestinationCharset.get());

         byte[] byteContent = (byte[]) content;
         return TranscodingUtils.convertCharset(byteContent, sourceCharset, destinationCharset);
      }
      throw new EncodingException("Failed to transcode, unsupported content " + content);
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supported;
   }

   @Override
   public boolean supportsConversion(MediaType mediaType, MediaType other) {
      if (mediaType.match(other)) return supported.stream().anyMatch(m -> m.match(mediaType));
      return mediaType.match(MediaType.TEXT_PLAIN) && supported.stream().anyMatch(m -> m.match(other)) ||
            other.match(MediaType.TEXT_PLAIN) && supported.stream().anyMatch(m -> m.match(mediaType));
   }
}
