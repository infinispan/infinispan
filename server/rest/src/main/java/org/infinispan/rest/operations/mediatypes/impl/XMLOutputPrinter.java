package org.infinispan.rest.operations.mediatypes.impl;

import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.mediatypes.Charset;
import org.infinispan.rest.operations.mediatypes.OutputPrinter;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link OutputPrinter} for xml values.
 *
 * @author Sebastian Łaskawiec
 */
public class XMLOutputPrinter implements OutputPrinter {

   protected final static Log logger = LogFactory.getLog(JSONOutputPrinter.class, Log.class);

   @Override
   public byte[] print(String cacheName, CacheSet<?> keys, Charset charset) {
      return keys.stream()
            .map(this::asString)
            .map(Escaper::escapeXml)
            .map(s -> "<key>" + s + "</key>")
            .collect(() -> Collectors.joining("", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><keys>", "</keys>"))
            .getBytes(charset.getJavaCharset());
   }

}
