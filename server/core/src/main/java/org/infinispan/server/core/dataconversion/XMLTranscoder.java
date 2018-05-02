package org.infinispan.server.core.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.marshall.core.ExternallyMarshallable;
import org.infinispan.server.core.logging.Log;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.XStreamException;

/**
 * Basic XML transcoder supporting conversions from XML to commons formats.
 *
 * @since 9.2
 */
public class XMLTranscoder extends OneToManyTranscoder {

   private static final Log logger = LogFactory.getLog(XMLTranscoder.class, Log.class);

   private static final SAXParserFactory SAXFACTORY = SAXParserFactory.newInstance();

   private static class XStreamHolder {
      static final XStream XStream = new XStream();
   }

   public XMLTranscoder() {
      super(APPLICATION_XML, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM, TEXT_PLAIN);
      XStreamHolder.XStream.addPermission(ExternallyMarshallable::isAllowed);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_XML)) {
         if (contentType.match(APPLICATION_OBJECT)) {
            Object decoded = StandardConversions.decodeObjectContent(content, contentType);
            return XStreamHolder.XStream.toXML(decoded);
         }
         if (contentType.match(TEXT_PLAIN)) {
            String inputText = StandardConversions.convertTextToObject(content, contentType);
            if (isWellFormed(inputText.getBytes())) return inputText.getBytes();
            String xmlString = XStreamHolder.XStream.toXML(inputText);
            return xmlString.getBytes(destinationType.getCharset());
         }
         if (contentType.match(APPLICATION_OCTET_STREAM)) {
            String inputText = StandardConversions.convertTextToObject(content, contentType);
            if (isWellFormed(inputText.getBytes())) return inputText.getBytes();
            String xmlString = XStreamHolder.XStream.toXML(inputText);
            return xmlString.getBytes(destinationType.getCharset());
         }
      }
      if (destinationType.match(APPLICATION_OCTET_STREAM)) {
         return StandardConversions.convertTextToOctetStream(content, contentType);
      }
      if (destinationType.match(TEXT_PLAIN)) {
         return StandardConversions.convertCharset(content, contentType.getCharset(), destinationType.getCharset());
      }
      if (destinationType.match(APPLICATION_OBJECT)) {
         try {
            Reader xmlReader = content instanceof byte[] ?
                  new InputStreamReader(new ByteArrayInputStream((byte[]) content)) :
                  new StringReader(content.toString());
            return XStreamHolder.XStream.fromXML(xmlReader);
         } catch (XStreamException e) {
            throw logger.errorDuringTranscoding(e);
         }
      }
      throw logger.unsupportedDataFormat(contentType);
   }

   private boolean isWellFormed(byte[] content) {
      XMLReader xmlReader;
      try {
         xmlReader = SAXFACTORY.newSAXParser().getXMLReader();
         xmlReader.parse(new InputSource(new ByteArrayInputStream(content)));
      } catch (SAXException | IOException | ParserConfigurationException e) {
         return false;
      }
      return true;
   }
}
