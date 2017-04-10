package org.infinispan.rest.server.operations.mediatypes.printers;

import java.io.IOException;
import java.util.stream.Collectors;

import org.infinispan.CacheSet;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.server.operations.exceptions.ServerInternalException;
import org.infinispan.rest.server.operations.mediatypes.Charset;
import org.infinispan.rest.server.operations.mediatypes.OutputPrinter;
import org.infinispan.stream.CacheCollectors;
import org.infinispan.util.logging.LogFactory;

import com.thoughtworks.xstream.XStream;

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
            .collect(CacheCollectors.serializableCollector(() -> Collectors.joining("", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><keys>", "</keys>")))
            .getBytes(charset.getJavaCharset());
   }

   @Override
   public byte[] print(byte[] value, Charset charset) throws ServerInternalException {
      Object objectToBeRendered;
      try {
         objectToBeRendered = DeserializationUtil.toObject(value);
      } catch (IOException | ClassNotFoundException e) {
         //This is either not a serializable object or the server has no clue how
         //to deserialize it
         logger.debug("Could not deserialize object from cache. Falling back to String representation.", e);
         objectToBeRendered = new String(value, charset.getJavaCharset());
      }

      try {
         return XStreamholder.XStream.toXML(objectToBeRendered).getBytes(charset.getJavaCharset());
      } catch (Exception e) {
         throw new ServerInternalException(e);
      }
   }
}
