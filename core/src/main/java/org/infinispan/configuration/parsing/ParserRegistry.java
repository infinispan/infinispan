package org.infinispan.configuration.parsing;

import static javax.xml.stream.XMLStreamConstants.END_DOCUMENT;
import static javax.xml.stream.XMLStreamConstants.START_DOCUMENT;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collection;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentMap;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
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
      this.parserMappings = CollectionFactory.makeConcurrentMap();
      this.cl = new WeakReference<ClassLoader>(classLoader);
      Collection<ConfigurationParser> parsers = ServiceFinder.load(ConfigurationParser.class, cl.get(), ParserRegistry.class.getClassLoader());
      for (ConfigurationParser parser : parsers) {
         try {
            Namespace[] namespaces = parser.getNamespaces();
            if (namespaces == null) {
               throw log.parserDoesNotDeclareNamespaces(parser.getClass().getName());
            }

            for (Namespace ns : namespaces) {
               QName qName = new QName(ns.uri(), ns.root());
               if (parserMappings.putIfAbsent(qName, parser) != null) {
                  throw log.parserRootElementAlreadyRegistered(qName);
               }
            }
         } catch (Exception e) {
            // 
         }
      }
   }

   public ConfigurationBuilderHolder parseFile(String filename) throws IOException {
      FileLookup fileLookup = new FileLookup();
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

   public ConfigurationBuilderHolder parse(InputStream is) {
      try {
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder(cl.get());
         parse(is, holder);
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
         for (; reader.next() != END_DOCUMENT;)
            ;
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
         throw new XMLStreamException("Unexpected element '" + name + "'", reader.getLocation());
      }
      parser.readElement(reader, holder);
   }
}
