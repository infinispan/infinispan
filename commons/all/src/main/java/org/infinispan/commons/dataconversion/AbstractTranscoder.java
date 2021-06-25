package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.infinispan.commons.util.Util;

/**
 * Class to inherit when implementing transcoders, will handle pre and post processing of the content.
 *
 * @since 13.0
 */
public abstract class AbstractTranscoder implements Transcoder {

   private static final List<MediaTypeCodec> CODECS = new ArrayList<>();

   static {
      CODECS.add(new JavaMediaTypeCodec());
      CODECS.add(new UrlFormCodec());
      CODECS.add(new RFC4648Codec());
   }

   /**
    * Decodes content before doing the transcoding.
    *
    * @param content the content.
    * @param contentType the  {@link MediaType} describing the content.
    * @return an Object with the content decoded or the content itself if no decoding needed.
    * @throws UnsupportedEncodingException if an invalid encoding or type is provided.
    */
   protected Object decodeContent(Object content, MediaType contentType) throws UnsupportedEncodingException {
      if (content == null) return null;

      Objects.requireNonNull(contentType, "contentType cannot be null!");

      for (MediaTypeCodec coded : CODECS) {
         content = coded.decodeContent(content, contentType);
      }

      return content;
   }

   /**
    * Encode the content after transcoding if necessary.
    *
    * @param content The content to encode.
    * @param destinationType The destination {@link MediaType}
    * @return The value encoded or unchanged if no encoding is needed.
    */
   protected Object encodeContent(Object content, MediaType destinationType) throws UnsupportedEncodingException {
      if (content == null) return null;

      for (MediaTypeCodec codec : CODECS) {
         content = codec.encodeContent(content, destinationType);
      }

      return content;
   }


   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         Object decoded = decodeContent(content, contentType);
         Object result = doTranscode(decoded, contentType, destinationType);
         return encodeContent(result, destinationType);
      } catch (UnsupportedEncodingException e) {
         throw CONTAINER.errorTranscoding(Util.toStr(content), contentType, destinationType, e);
      }
   }

   protected abstract Object doTranscode(Object decoded, MediaType contentType, MediaType destinationType);
}
