package org.infinispan.server.core.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.logging.Log;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.XStreamException;
import com.thoughtworks.xstream.security.ForbiddenClassException;
import com.thoughtworks.xstream.security.NoTypePermission;

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
      this(new ClassWhiteList(Collections.emptyList()));
   }

   public XMLTranscoder(ClassWhiteList whiteList) {
      super(APPLICATION_XML, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM, TEXT_PLAIN, APPLICATION_UNKNOWN);
      XStreamHolder.XStream.addPermission(NoTypePermission.NONE);
      XStreamHolder.XStream.addPermission(type -> whiteList.isSafeClass(type.getName()));
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_XML)) {
         if (contentType.match(APPLICATION_OBJECT)) {
            Object decoded = StandardConversions.decodeObjectContent(content, contentType);
            String xmlString = XStreamHolder.XStream.toXML(decoded);
            return xmlString.getBytes(destinationType.getCharset());
         }
         if (contentType.match(TEXT_PLAIN)) {
            String inputText = StandardConversions.convertTextToObject(content, contentType);
            if (isWellFormed(inputText.getBytes())) return inputText.getBytes();
            String xmlString = XStreamHolder.XStream.toXML(inputText);
            return xmlString.getBytes(destinationType.getCharset());
         }
         if (contentType.match(APPLICATION_OCTET_STREAM) || contentType.match(APPLICATION_UNKNOWN)) {
            String inputText = StandardConversions.convertTextToObject(content, contentType);
            if (isWellFormed(inputText.getBytes())) return inputText.getBytes();
            String xmlString = XStreamHolder.XStream.toXML(inputText);
            return xmlString.getBytes(destinationType.getCharset());
         }
      }
      if (destinationType.match(APPLICATION_OCTET_STREAM) || destinationType.match(APPLICATION_UNKNOWN)) {
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
         } catch (ForbiddenClassException e) {
            throw logger.errorDeserializing(e.getMessage());
         } catch (XStreamException e) {
            throw new CacheException(e);
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
