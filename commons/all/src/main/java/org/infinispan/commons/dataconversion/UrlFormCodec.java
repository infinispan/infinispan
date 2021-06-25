package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * Codec to handle URLEncoded content.
 *
 * @since 13.0
 */
class UrlFormCodec implements MediaTypeCodec {
   @Override
   public Object decodeContent(Object content, MediaType contentType) throws UnsupportedEncodingException {
      if (contentType.match(APPLICATION_WWW_FORM_URLENCODED)) {
         if (content instanceof byte[]) {
            content = URLDecoder.decode(new String((byte[]) content), contentType.getCharset().toString());
         } else {
            content = URLDecoder.decode(content.toString(), contentType.getCharset().toString());
         }
         return content.toString().getBytes(UTF_8);
      }
      return content;
   }

   @Override
   public Object encodeContent(Object content, MediaType destinationType) throws UnsupportedEncodingException {
      if (destinationType.match(APPLICATION_WWW_FORM_URLENCODED)) {
         if (content instanceof String) {
            content = URLEncoder.encode(content.toString(), destinationType.getCharset().toString()).getBytes(destinationType.getCharset());
         } else {
            content = URLEncoder.encode(new String((byte[]) content), destinationType.getCharset().toString()).getBytes(destinationType.getCharset());
         }
      }
      return content;
   }
}
