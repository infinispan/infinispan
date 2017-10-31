package org.infinispan.configuration.parsing;

import static javax.xml.stream.XMLStreamConstants.END_DOCUMENT;
import static javax.xml.stream.XMLStreamConstants.START_DOCUMENT;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentMap;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.serializing.ConfigurationHolder;
import org.infinispan.configuration.serializing.Serializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriterImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * ParserRegistry is a namespace-mapping-aware meta-parser which provides a way to delegate the
 * parsing of multi-namespace XML files to appropriate parsers, defined by the
 * {@link ConfigurationParser} interface. A registry of available parsers is built using the
 * {@link ServiceLoader} system. Implementations of {@link ConfigurationParser} should include a
 * META-INF/services/org.infinispan.configuration.parsing.ConfigurationParser file containing a list
 * of available parsers.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ParserRegistry implements NamespaceMappingParser {
   private static final Log log = LogFactory.getLog(ParserRegistry.class);
   private final WeakReference<ClassLoader> cl;
   private final ConcurrentMap<QName, ConfigurationParser> parserMappings;

   public ParserRegistry() {
      this(Thread.currentThread().getContextClassLoader());
   }

   public ParserRegistry(ClassLoader classLoader) {
      this(classLoader, false);
   }

   public ParserRegistry(ClassLoader classLoader, boolean defaultOnly) {
      this.parserMappings = CollectionFactory.makeConcurrentMap();
      this.cl = new WeakReference<>(classLoader);
      Collection<ConfigurationParser> parsers = ServiceFinder.load(ConfigurationParser.class, cl.get(), ParserRegistry.class.getClassLoader());
      for (ConfigurationParser parser : parsers) {

         Namespace[] namespaces = parser.getNamespaces();
         if (namespaces == null) {
            throw log.parserDoesNotDeclareNamespaces(parser.getClass().getName());
         }

         boolean skipParser = defaultOnly;

         if (skipParser) {
            for (Namespace ns : namespaces) {
               if ("".equals(ns.uri())) {
                  skipParser = false;
               }
            }
         }

         if (!skipParser) {
            for (Namespace ns : namespaces) {
               QName qName = new QName(ns.uri(), ns.root());
               ConfigurationParser existingParser = parserMappings.putIfAbsent(qName, parser);
               if (existingParser != null && !parser.getClass().equals(existingParser.getClass())) {
                  log.parserRootElementAlreadyRegistered(qName, parser.getClass().getName(), existingParser.getClass().getName());
               }
            }
         }
      }
   }

   public ConfigurationBuilderHolder parseFile(String filename) throws IOException {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      InputStream is = fileLookup.lookupFile(filename, cl.get());
      if (is == null) {
         throw new FileNotFoundException(filename);
      }
      try {
         return parse(is);
      } finally {
         Util.close(is);
      }
   }

   public ConfigurationBuilderHolder parse(String s) {
      return parse(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
   }

   public ConfigurationBuilderHolder parse(InputStream is) {
      try {
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder(cl.get());
         parse(is, holder);
         holder.validate();
         return holder;
      } catch (CacheConfigurationException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
   }

   public void parse(InputStream is, ConfigurationBuilderHolder holder) throws XMLStreamException {
      BufferedInputStream input = new BufferedInputStream(is);
      XMLStreamReader subReader = XMLInputFactory.newInstance().createXMLStreamReader(input);
      XMLExtendedStreamReader reader = new XMLExtendedStreamReaderImpl(this, subReader);
      parse(reader, holder);
      subReader.close();
      // Fire all parsingComplete events if any
      for (ParserContext parserContext : holder.getParserContexts().values()) {
         parserContext.fireParsingComplete();
      }
   }

   public void parse(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      try {
         reader.require(START_DOCUMENT, null, null);
         reader.nextTag();
         reader.require(START_ELEMENT, null, null);
         parseElement(reader, holder);
         while (reader.next() != END_DOCUMENT) {
            // consume remaining parsing events
         }
      } finally {
         try {
            reader.close();
         } catch (Exception e) {
         }
      }
   }

   @Override
   public void parseElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      QName name = reader.getName();
      ConfigurationParser parser = parserMappings.get(name);
      if (parser == null) {
         throw log.unsupportedConfiguration(name.getLocalPart(), name.getNamespaceURI());
      }
      Schema oldSchema = reader.getSchema();
      reader.setSchema(Schema.fromNamespaceURI(name.getNamespaceURI()));
      parser.readElement(reader, holder);
      reader.setSchema(oldSchema);
   }

   /**
    * Serializes a full configuration to an {@link OutputStream}
    *
    * @param os the output stream where the configuration should be serialized to
    * @param globalConfiguration the global configuration. Can be null
    * @param configurations a map of named configurations
    * @throws XMLStreamException
    */
   public void serialize(OutputStream os, GlobalConfiguration globalConfiguration, Map<String, Configuration> configurations) throws XMLStreamException {
      BufferedOutputStream output = new BufferedOutputStream(os);
      XMLStreamWriter subWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(output);
      XMLExtendedStreamWriter writer = new XMLExtendedStreamWriterImpl(subWriter);
      serialize(writer, globalConfiguration, configurations);
      subWriter.close();
   }

   /**
    * Serializes a full configuration to an {@link XMLExtendedStreamWriter}
    *
    * @param writer the writer where the configuration should be serialized to
    * @param globalConfiguration the global configuration. Can be null
    * @param configurations a map of named configurations
    * @throws XMLStreamException
    */
   public void serialize(XMLExtendedStreamWriter writer, GlobalConfiguration globalConfiguration, Map<String, Configuration> configurations) throws XMLStreamException {
      writer.writeStartDocument();
      writer.writeStartElement("infinispan");
      Serializer serializer = new Serializer();
      serializer.serialize(writer, new ConfigurationHolder(globalConfiguration, configurations));
      writer.writeEndElement();
      writer.writeEndDocument();
   }

   /**
    * Serializes a single configuration to an OutputStream
    * @param os
    * @param name
    * @param configuration
    */
   public void serialize(OutputStream os, String name, Configuration configuration) throws XMLStreamException {
      serialize(os, null, Collections.singletonMap(name, configuration));
   }

   /**
    * Serializes a single configuration to a String
    * @param name the name of the configuration
    * @param configuration the {@link Configuration}
    * @return the XML representation of the specified configuration
    * @throws XMLStreamException
    */
   public String serialize(String name, Configuration configuration) throws XMLStreamException {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      serialize(os, name, configuration);
      try {
         return os.toString("UTF-8");
      } catch (UnsupportedEncodingException e) {
         // Will never happen
         return null;
      }
   }
}
