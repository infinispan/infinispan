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
import org.infinispan.server.core.dataconversion.xml.MXParserDriver;
import org.infinispan.server.core.logging.Log;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.XStreamException;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.basic.BigDecimalConverter;
import com.thoughtworks.xstream.converters.basic.BigIntegerConverter;
import com.thoughtworks.xstream.converters.basic.BooleanConverter;
import com.thoughtworks.xstream.converters.basic.ByteConverter;
import com.thoughtworks.xstream.converters.basic.CharConverter;
import com.thoughtworks.xstream.converters.basic.DateConverter;
import com.thoughtworks.xstream.converters.basic.DoubleConverter;
import com.thoughtworks.xstream.converters.basic.FloatConverter;
import com.thoughtworks.xstream.converters.basic.IntConverter;
import com.thoughtworks.xstream.converters.basic.LongConverter;
import com.thoughtworks.xstream.converters.basic.NullConverter;
import com.thoughtworks.xstream.converters.basic.ShortConverter;
import com.thoughtworks.xstream.converters.basic.StringConverter;
import com.thoughtworks.xstream.converters.basic.URIConverter;
import com.thoughtworks.xstream.converters.basic.URLConverter;
import com.thoughtworks.xstream.converters.collections.ArrayConverter;
import com.thoughtworks.xstream.converters.collections.BitSetConverter;
import com.thoughtworks.xstream.converters.collections.CharArrayConverter;
import com.thoughtworks.xstream.converters.collections.CollectionConverter;
import com.thoughtworks.xstream.converters.collections.MapConverter;
import com.thoughtworks.xstream.converters.collections.SingletonCollectionConverter;
import com.thoughtworks.xstream.converters.collections.SingletonMapConverter;
import com.thoughtworks.xstream.converters.extended.EncodedByteArrayConverter;
import com.thoughtworks.xstream.converters.extended.FileConverter;
import com.thoughtworks.xstream.converters.extended.GregorianCalendarConverter;
import com.thoughtworks.xstream.converters.extended.JavaClassConverter;
import com.thoughtworks.xstream.converters.extended.JavaFieldConverter;
import com.thoughtworks.xstream.converters.extended.JavaMethodConverter;
import com.thoughtworks.xstream.converters.extended.LocaleConverter;
import com.thoughtworks.xstream.converters.reflection.ExternalizableConverter;
import com.thoughtworks.xstream.converters.reflection.ReflectionConverter;
import com.thoughtworks.xstream.converters.reflection.SerializableConverter;
import com.thoughtworks.xstream.security.ForbiddenClassException;
import com.thoughtworks.xstream.security.NoTypePermission;

/**
 * Basic XML transcoder supporting conversions from XML to commons formats.
 *
 * @since 9.2
 */
public class XMLTranscoder extends OneToManyTranscoder {

   private static final Log logger = LogFactory.getLog(XMLTranscoder.class, Log.class);

   private static class XStreamHolder {
      static final XStream XStream;

      static {
         XStream = new XStream(new MXParserDriver()) {
            @Override
            protected void setupConverters() {
               registerConverter(new ReflectionConverter(getMapper(), getReflectionProvider()), PRIORITY_VERY_LOW);
               registerConverter(new SerializableConverter(getMapper(), getReflectionProvider(), getClassLoaderReference()), PRIORITY_LOW);
               registerConverter(new ExternalizableConverter(getMapper(), getClassLoaderReference()), PRIORITY_LOW);
               registerConverter(new NullConverter(), PRIORITY_VERY_HIGH);
               registerConverter(new IntConverter(), PRIORITY_NORMAL);
               registerConverter(new FloatConverter(), PRIORITY_NORMAL);
               registerConverter(new DoubleConverter(), PRIORITY_NORMAL);
               registerConverter(new LongConverter(), PRIORITY_NORMAL);
               registerConverter(new ShortConverter(), PRIORITY_NORMAL);
               registerConverter((Converter) new CharConverter(), PRIORITY_NORMAL);
               registerConverter(new BooleanConverter(), PRIORITY_NORMAL);
               registerConverter(new ByteConverter(), PRIORITY_NORMAL);
               registerConverter(new StringConverter(), PRIORITY_NORMAL);
               registerConverter(new DateConverter(), PRIORITY_NORMAL);
               registerConverter(new BitSetConverter(), PRIORITY_NORMAL);
               registerConverter(new URIConverter(), PRIORITY_NORMAL);
               registerConverter(new URLConverter(), PRIORITY_NORMAL);
               registerConverter(new BigIntegerConverter(), PRIORITY_NORMAL);
               registerConverter(new BigDecimalConverter(), PRIORITY_NORMAL);
               registerConverter(new ArrayConverter(getMapper()), PRIORITY_NORMAL);
               registerConverter(new CharArrayConverter(), PRIORITY_NORMAL);
               registerConverter(new CollectionConverter(getMapper()), PRIORITY_NORMAL);
               registerConverter(new MapConverter(getMapper()), PRIORITY_NORMAL);
               registerConverter(new SingletonCollectionConverter(getMapper()), PRIORITY_NORMAL);
               registerConverter(new SingletonMapConverter(getMapper()), PRIORITY_NORMAL);
               registerConverter((Converter) new EncodedByteArrayConverter(), PRIORITY_NORMAL);
               registerConverter(new FileConverter(), PRIORITY_NORMAL);
               registerConverter(new JavaClassConverter(getClassLoaderReference()), PRIORITY_NORMAL);
               registerConverter(new JavaMethodConverter(getClassLoaderReference()), PRIORITY_NORMAL);
               registerConverter(new JavaFieldConverter(getClassLoaderReference()), PRIORITY_NORMAL);
               registerConverter(new LocaleConverter(), PRIORITY_NORMAL);
               registerConverter(new GregorianCalendarConverter(), PRIORITY_NORMAL);
            }
         };
         XStream.setMode(com.thoughtworks.xstream.XStream.NO_REFERENCES);
      }
   }

   public XMLTranscoder() {
      this(XMLTranscoder.class.getClassLoader(), new ClassAllowList(Collections.emptyList()));
   }

   public XMLTranscoder(ClassAllowList classAllowList) {
      this(XMLTranscoder.class.getClassLoader(), classAllowList);
   }

   public XMLTranscoder(ClassLoader classLoader, ClassAllowList allowList) {
      super(APPLICATION_XML, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM, TEXT_PLAIN, APPLICATION_UNKNOWN);
      XStreamHolder.XStream.addPermission(NoTypePermission.NONE);
      XStreamHolder.XStream.addPermission(type -> allowList.isSafeClass(type.getName()));
      XStreamHolder.XStream.setClassLoader(classLoader);
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
