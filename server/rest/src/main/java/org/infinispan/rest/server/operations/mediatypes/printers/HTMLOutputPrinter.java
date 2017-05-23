package org.infinispan.rest.server.operations.mediatypes.printers;

import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.rest.server.operations.exceptions.ServerInternalException;
import org.infinispan.rest.server.operations.mediatypes.Charset;
import org.infinispan.rest.server.operations.mediatypes.OutputPrinter;

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
   public byte[] print(byte[] value, Charset charset) throws ServerInternalException {
      value = new String(value).getBytes(charset.getJavaCharset());
      byte[] headByes = HEAD_AS_TEXT.getBytes(charset.getJavaCharset());
      byte[] tailBytes = TAIL_AS_TEXT.getBytes(charset.getJavaCharset());

      byte[] result = new byte[headByes.length + value.length + tailBytes.length];
      System.arraycopy(headByes, 0, result, 0, headByes.length);
      System.arraycopy(value, 0, result, headByes.length, value.length);
      System.arraycopy(tailBytes, 0, result, headByes.length + value.length, tailBytes.length);
      return result;
   }
}
