package org.infinispan.rest.server.operations.mediatypes.printers;

import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.rest.server.operations.mediatypes.Charset;
import org.infinispan.rest.server.operations.mediatypes.OutputPrinter;

public class TextOutputPrinter implements OutputPrinter {

   @Override
   public byte[] print(String cacheName, CacheSet<?> keys, Charset charset) {
      return keys.stream()
            .map(s -> s.toString())
            .collect(Collectors.joining("\n", "", ""))
            .getBytes(charset.getJavaCharset());
   }

   @Override
   public byte[] print(byte[] value, Charset charset) {
      return value;
   }
}
