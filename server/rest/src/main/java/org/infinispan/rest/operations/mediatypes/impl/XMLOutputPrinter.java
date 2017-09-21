package org.infinispan.rest.operations.mediatypes.impl;

import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.exceptions.ServerInternalException;
import org.infinispan.rest.operations.mediatypes.Charset;
import org.infinispan.rest.operations.mediatypes.OutputPrinter;
import org.infinispan.util.logging.LogFactory;

import com.thoughtworks.xstream.XStream;

/**
 * {@link OutputPrinter} for xml values.
 *
 * @author Sebastian Łaskawiec
 */
public class XMLOutputPrinter implements OutputPrinter {

   protected final static Log logger = LogFactory.getLog(JSONOutputPrinter.class, Log.class);

   private static class XStreamholder {
      public static final XStream XStream = new XStream();
   }

   @Override
   public byte[] print(String cacheName, CacheSet<?> keys, Charset charset) {
      return keys.stream()
            .map(b -> Escaper.escapeXml(b.toString()))
            .map(s -> "<key>" + s + "</key>")
            .collect(() -> Collectors.joining("", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><keys>", "</keys>"))
            .getBytes(charset.getJavaCharset());
   }

   @Override
   public byte[] print(Object value, Charset charset) throws ServerInternalException {
      try {
         return XStreamholder.XStream.toXML(value).getBytes(charset.getJavaCharset());
      } catch (Exception e) {
         throw new ServerInternalException(e);
      }
   }
}
