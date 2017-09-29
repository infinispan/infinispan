package org.infinispan.rest.operations.mediatypes.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.rest.operations.exceptions.ServerInternalException;
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
            .map(b -> b.toString())
            .collect(() -> Collectors.joining(",", "[", "]"))
            .getBytes(charset.getJavaCharset());
   }

   @Override
   public byte[] print(Object value, Charset charset) throws ServerInternalException {
      if (value instanceof byte[]) {
         return (byte[]) value;
      } else if (value instanceof Serializable) {
         try {
            return SerializationUtil.toByteArray(value);
         } catch (IOException e) {
            throw new ServerInternalException(e);
         }
      }
      return value.toString().getBytes(charset.getJavaCharset());
   }
}
