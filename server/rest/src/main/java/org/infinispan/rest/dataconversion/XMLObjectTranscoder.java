package org.infinispan.rest.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;

import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.mediatypes.impl.JSONOutputPrinter;
import org.infinispan.util.logging.LogFactory;

import com.thoughtworks.xstream.XStream;

/**
 * @since 9.2
 */
public class XMLObjectTranscoder implements Transcoder {

   protected final static Log logger = LogFactory.getLog(JSONOutputPrinter.class, Log.class);
   private static final Set<MediaType> supportedTypes = new HashSet<>();

   private static class XStreamHolder {
      static final XStream XStream = new XStream();
   }

   public XMLObjectTranscoder() {
      supportedTypes.add(APPLICATION_XML);
      supportedTypes.add(APPLICATION_OBJECT);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_XML)) {
         Charset charset = destinationType.getCharset();
         if (content instanceof byte[]) {
            String contentAsString = new String((byte[]) content, charset);
            return XStreamHolder.XStream.toXML(contentAsString);
         }
         return XStreamHolder.XStream.toXML(content).getBytes(charset);
      }
      if (destinationType.match(APPLICATION_OBJECT)) {
         if (content instanceof byte[]) {
            return content;
         }
         return XStreamHolder.XStream.fromXML(new StringReader((String) content));
      }
      return null;
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }
}
