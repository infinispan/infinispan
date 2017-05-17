package org.infinispan.rest.operations.mediatypes.impl;

import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.rest.operations.exceptions.ServerInternalException;
import org.infinispan.rest.operations.mediatypes.Charset;
import org.infinispan.rest.operations.mediatypes.OutputPrinter;

/**
 * {@link OutputPrinter} for HTML values.
 *
 * @author Sebastian Łaskawiec
 */
public class HTMLOutputPrinter implements OutputPrinter {

   private static final String HEAD_AS_TEXT = "<html><body>";
   private static final String TAIL_AS_TEXT = "</body></html>";

   @Override
   public byte[] print(String cacheName, CacheSet<?> keys, Charset charset) {
      return keys.stream()
            .map(b -> Escaper.escapeHtml(b.toString()))
            .map(s -> "<a href=\"" + cacheName + "/" + s + "\">" + s + "</a>")
            .collect(Collectors.joining("<br/>", HEAD_AS_TEXT, TAIL_AS_TEXT))
            .getBytes(charset.getJavaCharset());
   }

   @Override
   public byte[] print(Object value, Charset charset) throws ServerInternalException {
      byte[] valueAsBytes = value.toString().getBytes(charset.getJavaCharset());
      byte[] headByes = HEAD_AS_TEXT.getBytes(charset.getJavaCharset());
      byte[] tailBytes = TAIL_AS_TEXT.getBytes(charset.getJavaCharset());

      byte[] result = new byte[headByes.length + valueAsBytes.length + tailBytes.length];
      System.arraycopy(headByes, 0, result, 0, headByes.length);
      System.arraycopy(valueAsBytes, 0, result, headByes.length, valueAsBytes.length);
      System.arraycopy(tailBytes, 0, result, headByes.length + valueAsBytes.length, tailBytes.length);
      return result;
   }
}
