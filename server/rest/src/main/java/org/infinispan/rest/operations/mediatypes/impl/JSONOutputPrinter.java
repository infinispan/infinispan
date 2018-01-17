package org.infinispan.rest.operations.mediatypes.impl;

import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.mediatypes.Charset;
import org.infinispan.rest.operations.mediatypes.OutputPrinter;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link OutputPrinter} for JSON values.
 *
 * @author Sebastian Łaskawiec
 */
public class JSONOutputPrinter implements OutputPrinter {

   protected final static Log logger = LogFactory.getLog(JSONOutputPrinter.class, Log.class);

   @Override
   public byte[] print(String cacheName, CacheSet<?> keys, Charset charset) {
      return keys.stream()
            .map(this::asString)
            .collect(() -> Collectors.joining(",", "keys=[", "]"))
            .getBytes(charset.getJavaCharset());
   }

}
