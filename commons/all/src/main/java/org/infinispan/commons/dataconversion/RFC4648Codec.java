package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.JavaStringCodec.BYTE_ARRAY;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.BASE_64;
import static org.infinispan.commons.dataconversion.MediaType.HEX;

import java.util.Base64;
import java.util.Optional;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Handles base16 and base64 data encodings as specified in the <a href="https://www.ietf.org/rfc/rfc4648.txt">RFC 4648</a>.
 *
 * It used when the {@link MediaType} contains an <code>encoding</code> param, e.g.
 * <br/>
 * <code>application/octet-stream; encoding=hex</code>.
 * <br>
 * Valid encodings are "hex" (base16) and "base64".
 *
 * @since 13.0
 */
class RFC4648Codec implements MediaTypeCodec {
   private static final Log log = LogFactory.getLog(RFC4648Codec.class);

   @Override
   public Object decodeContent(Object content, MediaType contentType) {
      Optional<String> optionalEncoding = contentType.getParameter("encoding");
      if (optionalEncoding.isPresent()) {
         String enc = optionalEncoding.get();
         if (content instanceof byte[]) {
            return decode(new String((byte[]) content, contentType.getCharset()), enc);
         } else if (content instanceof String) {
            return decode(content.toString(), enc);
         }
         throw new EncodingException("Cannot decode binary content " + content);
      } else {
         if (content instanceof String && (contentType.match(MediaType.APPLICATION_OCTET_STREAM) || hasJavaByteArrayType(contentType))) {
            return decode(content.toString(), HEX);
         }
         return content;
      }
   }

   private boolean hasJavaByteArrayType(MediaType contentType) {
      return contentType.match(APPLICATION_OBJECT) && contentType.getClassType() != null &&
            contentType.getClassType().equals(BYTE_ARRAY.getName());
   }

   private Object decode(String content, String codec) {
      switch (codec) {
         case HEX:
            return Base16Codec.decode(content);
         case BASE_64:
            return Base64.getDecoder().decode(content);
         default:
            throw log.encodingNotSupported(codec);
      }

   }

   private Object encode(byte[] content, String codec) {
      switch (codec) {
         case HEX:
            return Base16Codec.encode(content);
         case BASE_64:
            return Base64.getEncoder().encode(content);
         default:
            throw log.encodingNotSupported(codec);
      }
   }

   @Override
   public Object encodeContent(Object content, MediaType destinationType) {
      Optional<String> optionalEncoding = destinationType.getParameter("encoding");
      if (optionalEncoding.isPresent()) {
         String enc = optionalEncoding.get();
         if (content instanceof byte[]) {
            content = encode((byte[]) content, enc);
         } else if (content instanceof String) {
            content = encode(content.toString().getBytes(UTF_8), enc);
         }
         return content;
      }
      boolean binaryTargetForString = destinationType.isBinary() && content instanceof String;
      return binaryTargetForString ? content.toString().getBytes(UTF_8) : content;
   }

}
