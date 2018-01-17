package org.infinispan.rest.operations.mediatypes.impl;

import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.commons.dataconversion.StandardConversions;
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
            .map(k -> (byte[]) k)
            .map(StandardConversions::bytesToHex)
            .collect(Collectors.joining("\n", "", ""))
            .getBytes(charset.getJavaCharset());
   }
}
