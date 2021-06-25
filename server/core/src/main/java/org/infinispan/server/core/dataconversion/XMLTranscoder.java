package org.infinispan.server.core.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.dataconversion.xml.XStreamEngine;
import org.infinispan.server.core.logging.Log;

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

   private final XStreamEngine xstream = new XStreamEngine();

   public XMLTranscoder() {
      this(XMLTranscoder.class.getClassLoader(), new ClassAllowList(Collections.emptyList()));
   }

   public XMLTranscoder(ClassAllowList classAllowList) {
      this(XMLTranscoder.class.getClassLoader(), classAllowList);
   }

   public XMLTranscoder(ClassLoader classLoader, ClassAllowList allowList) {
      super(APPLICATION_XML, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM, TEXT_PLAIN, APPLICATION_UNKNOWN);
      xstream.addPermission(NoTypePermission.NONE);
      xstream.addPermission(type -> allowList.isSafeClass(type.getName()));
      xstream.setClassLoader(classLoader);
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_XML)) {
         if (contentType.match(APPLICATION_OBJECT)) {
            String xmlString = xstream.toXML(content);
            return xmlString.getBytes(destinationType.getCharset());
         }
         if (contentType.match(TEXT_PLAIN)) {
            String inputText = StandardConversions.convertTextToObject(content, contentType);
            if (isWellFormed(inputText.getBytes())) return inputText.getBytes();
            String xmlString = xstream.toXML(inputText);
            return xmlString.getBytes(destinationType.getCharset());
         }
         if (contentType.match(APPLICATION_OCTET_STREAM) || contentType.match(APPLICATION_UNKNOWN)) {
            String inputText = StandardConversions.convertTextToObject(content, contentType);
            if (isWellFormed(inputText.getBytes())) return inputText.getBytes();
            String xmlString = xstream.toXML(inputText);
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
            return xstream.fromXML(xmlReader);
         } catch (ForbiddenClassException e) {
            throw logger.errorDeserializing(e.getMessage());
         } catch (XStreamException e) {
            throw new CacheException(e);
         }
      }
      throw logger.unsupportedDataFormat(contentType);
   }

   private boolean isWellFormed(byte[] content) {
      ByteArrayInputStream is = new ByteArrayInputStream(content);
      try (ConfigurationReader reader = ConfigurationReader.from(is).build()) {
         // Consume all the stream
         while (reader.hasNext()) {
            reader.nextElement();
         }
         return true;
      } catch (Exception e) {
         return false;
      }
   }
}
