package org.infinispan.rest.operations.mediatypes.impl;

import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.rest.operations.mediatypes.Charset;
import org.infinispan.rest.operations.mediatypes.OutputPrinter;

/**
 * {@link OutputPrinter} for binary values.
 *
 * @author Sebastian Łaskawiec
 */
public class BinaryOutputPrinter implements OutputPrinter {

   @Override
   public byte[] print(String cacheName, CacheSet<?> keys, Charset charset) {
      return keys.stream()
            .map(Object::toString)
            .collect(() -> Collectors.joining(",", "[", "]"))
            .getBytes(charset.getJavaCharset());
   }
}
