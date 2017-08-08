package org.infinispan.commons.dataconversion;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * @since 9.2
 */
public final class TranscodingUtils {

   public static byte[] convertCharset(byte[] content, Charset fromCharset, Charset toCharset) {
      if (content == null || content.length == 0) return null;
      if (fromCharset == null || toCharset == null) {
         throw new IllegalArgumentException("Charset cannot be null!");
      }
      CharBuffer inputContent = fromCharset.decode(ByteBuffer.wrap(content));
      ByteBuffer result = toCharset.encode(inputContent);
      return Arrays.copyOf(result.array(), result.limit());
   }

}
