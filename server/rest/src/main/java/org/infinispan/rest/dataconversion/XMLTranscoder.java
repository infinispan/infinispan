package org.infinispan.rest.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;
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

   protected final static Log logger = LogFactory.getLog(XMLTranscoder.class, Log.class);
   private static final SAXParserFactory SAXFACTORY = SAXParserFactory.newInstance();

   private static class XStreamHolder {
      static final XStream XStream = new XStream();
   }

   public XMLTranscoder() {
      super(APPLICATION_XML, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM, TEXT_PLAIN);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_XML)) {
         if (contentType.match(APPLICATION_OBJECT)) {
            Object decoded = StandardConversions.decodeObjectContent(content, contentType);
            return XStreamHolder.XStream.toXML(decoded);
         }
         if (contentType.match(TEXT_PLAIN)) {
            validate(content, contentType.getCharset());
            return StandardConversions.convertCharset(content, contentType.getCharset(), destinationType.getCharset());
         }
         if (contentType.match(APPLICATION_OCTET_STREAM)) {
            byte[] bytes = StandardConversions.decodeOctetStream(content, contentType);
            validate(content, contentType.getCharset());
            return StandardConversions.convertOctetStreamToText(bytes, destinationType);
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
            throw logger.errorTranscoding(e);
         }
      }
      throw logger.unsupportedDataFormat(contentType.toString());
   }

   private void validate(Object content, Charset contentType) {
      XMLReader xmlReader;
      try {
         xmlReader = SAXFACTORY.newSAXParser().getXMLReader();
         byte[] source = content instanceof byte[] ? (byte[]) content : content.toString().getBytes(contentType);
         xmlReader.parse(new InputSource(new ByteArrayInputStream(source)));
      } catch (SAXException | IOException | ParserConfigurationException e) {
         throw logger.cannotConvertToXML(e);
      }
   }

}
