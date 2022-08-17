package org.infinispan.configuration.parsing;

import static org.infinispan.util.logging.Log.CONFIG;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationReaderException;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolver;
import org.infinispan.commons.configuration.io.ConfigurationSchemaVersion;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.configuration.io.ConfigurationWriterException;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.configuration.io.URLConfigurationResourceResolver;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.serializing.ConfigurationHolder;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.CoreConfigurationSerializer;

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
   private final ClassLoader cl;
   private final ConcurrentMap<QName, NamespaceParserPair> parserMappings;
   private final Properties properties;

   public ParserRegistry() {
      this(Thread.currentThread().getContextClassLoader());
   }

   public ParserRegistry(ClassLoader classLoader) {
      this(classLoader, false, SecurityActions.getSystemProperties());
   }

   public ParserRegistry(ClassLoader classLoader, boolean defaultOnly, Properties properties) {
      this.parserMappings = new ConcurrentHashMap<>();
      this.cl = classLoader;
      this.properties = properties;
      Collection<ConfigurationParser> parsers = ServiceFinder.load(ConfigurationParser.class, cl, ParserRegistry.class.getClassLoader());
      for (ConfigurationParser parser : parsers) {

         Namespace[] namespaces = parser.getNamespaces();
         if (namespaces == null) {
            throw CONFIG.parserDoesNotDeclareNamespaces(parser.getClass().getName());
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
               NamespaceParserPair existing = parserMappings.putIfAbsent(qName, new NamespaceParserPair(ns, parser));
               if (existing != null && !parser.getClass().equals(existing.parser.getClass())) {
                  CONFIG.parserRootElementAlreadyRegistered(qName.toString(), parser.getClass().getName(), existing.parser.getClass().getName());
               }
            }
         }
      }
   }

   public ConfigurationBuilderHolder parse(Path path) throws IOException {
      return parse(path.toUri().toURL());
   }

   public ConfigurationBuilderHolder parse(URL url) throws IOException {
      try (InputStream is = url.openStream()) {
         return parse(is, new URLConfigurationResourceResolver(url), MediaType.fromExtension(url.getFile()));
      }
   }

   public ConfigurationBuilderHolder parseFile(String filename) throws IOException {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      URL url = fileLookup.lookupFileLocation(filename, cl);
      if (url == null) {
         throw new FileNotFoundException(filename);
      }
      try (InputStream is = url.openStream()) {
         return parse(is, new URLConfigurationResourceResolver(url), MediaType.fromExtension(url.getFile()));
      }
   }



   public ConfigurationBuilderHolder parseFile(File file) throws IOException {
      InputStream is = new FileInputStream(file);
      if (is == null) {
         throw new FileNotFoundException(file.getAbsolutePath());
      }
      try {
         URL url = file.toURI().toURL();
         return parse(is, new URLConfigurationResourceResolver(url), MediaType.fromExtension(url.getFile()));
      } finally {
         Util.close(is);
      }
   }

   public ConfigurationBuilderHolder parse(String s) {
      return parse(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)), null, MediaType.APPLICATION_XML);
   }

   public ConfigurationBuilderHolder parse(String s, MediaType mediaType) {
      return parse(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)), null, mediaType);
   }

   /**
    * Parses the supplied {@link InputStream} returning a new {@link ConfigurationBuilderHolder}
    * @param is an {@link InputStream} pointing to a configuration file
    * @param resourceResolver an optional resolver for Xinclude
    * @return a new {@link ConfigurationBuilderHolder} which contains the parsed configuration
    */
   public ConfigurationBuilderHolder parse(InputStream is, ConfigurationResourceResolver resourceResolver, MediaType mediaType) {
      try {
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder(cl);
         parse(is, holder, resourceResolver, mediaType);
         holder.validate();
         return holder;
      } catch (CacheConfigurationException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
   }

   public ConfigurationBuilderHolder parse(URL url, ConfigurationBuilderHolder holder) throws IOException {
      try(InputStream is = url.openStream()) {
         return parse(is, holder, new URLConfigurationResourceResolver(url), MediaType.fromExtension(url.getFile()));
      }
   }

   public ConfigurationBuilderHolder parse(InputStream is, ConfigurationBuilderHolder holder, ConfigurationResourceResolver resourceResolver, MediaType mediaType) {
      ConfigurationReader reader = ConfigurationReader.from(is).withResolver(resourceResolver).withType(mediaType).withProperties(properties).withNamingStrategy(NamingStrategy.KEBAB_CASE).build();
      parse(reader, holder);
      // Fire all parsingComplete events if any
      holder.fireParserListeners();
      return holder;
   }

   public ConfigurationBuilderHolder parse(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      try {
         holder.setNamespaceMappingParser(this);
         reader.require(ConfigurationReader.ElementType.START_DOCUMENT);
         ConfigurationReader.ElementType elementType = reader.nextElement();
         if (elementType == ConfigurationReader.ElementType.START_ELEMENT) {
            parseElement(reader, holder);
         }
         while (elementType != ConfigurationReader.ElementType.END_DOCUMENT) {
            // consume remaining parsing events
            elementType = reader.nextElement();
         }
         return holder;
      } finally {
         try {
            reader.close();
         } catch (Exception e) {
         }
      }
   }

   @Override
   public void parseElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      String namespace = reader.getNamespace();
      String name = reader.getLocalName();
      NamespaceParserPair parser = findNamespaceParser(reader, namespace, name);
      ConfigurationSchemaVersion oldSchema = reader.getSchema();
      reader.setSchema(Schema.fromNamespaceURI(namespace));
      parser.parser.readElement(reader, holder);
      reader.setSchema(oldSchema);
   }

   private NamespaceParserPair parseCacheName(ConfigurationReader reader, String name, String namespace) {
      if (reader.hasNext()) {
         reader.nextElement();
      }
      // If there's an invalid cache type element, then it's not a cache name and an invalid cache definition
      if (!Element.isCacheElement(reader.getLocalName()))
         throw CONFIG.unsupportedConfiguration(name, namespace, Version.getVersion());
      reader.setAttributeValue("", "name", name);
      return findNamespaceParser(reader, reader.getNamespace(), reader.getLocalName());
   }

   @Override
   public void parseAttribute(ConfigurationReader reader, int i, ConfigurationBuilderHolder holder) throws ConfigurationReaderException {
      String namespace = reader.getAttributeNamespace(i);
      String name = reader.getLocalName();
      NamespaceParserPair parser = findNamespaceParser(reader, namespace, name);
      ConfigurationSchemaVersion oldSchema = reader.getSchema();
      reader.setSchema(Schema.fromNamespaceURI(namespace));
      parser.parser.readAttribute(reader, name, i, holder);
      reader.setSchema(oldSchema);
   }

   private NamespaceParserPair findNamespaceParser(ConfigurationReader reader, String namespace, String name) {
      NamespaceParserPair parser = parserMappings.get(new QName(namespace, name));
      if (parser == null) {
         // Next we strip off the version from the URI and look for a wildcard match
         int lastColon = namespace.lastIndexOf(':');
         String baseUri = namespace.substring(0,  lastColon + 1) + "*";
         parser = parserMappings.get(new QName(baseUri, name));
         // See if we can get a default parser instead
         if (parser == null || !isSupportedNamespaceVersion(parser.namespace, namespace.substring(lastColon + 1)))
            // Parse a possible cache name because the cache name has not a namespace definition in YAML/JSON
            return parseCacheName(reader, name, namespace);
      }
      return parser;
   }


   private boolean isSupportedNamespaceVersion(Namespace namespace, String version) {
      short reqVersion = Version.getVersionShort(version);
      if (reqVersion < Version.getVersionShort(namespace.since())) {
         return false;
      }
      short untilVersion = namespace.until().length() > 0 ? Version.getVersionShort(namespace.until()) : Version.getVersionShort();
      return reqVersion <= untilVersion;
   }

   /**
    * Serializes a full configuration to an {@link OutputStream}
    *
    * @param os the output stream where the configuration should be serialized to
    * @param globalConfiguration the global configuration. Can be null
    * @param configurations a map of named configurations
    * @deprecated use {@link #serialize(ConfigurationWriter, GlobalConfiguration, Map)} instead
    */
   @Deprecated
   public void serialize(OutputStream os, GlobalConfiguration globalConfiguration, Map<String, Configuration> configurations) {
      ConfigurationWriter writer = ConfigurationWriter.to(os).build();
      serialize(writer, globalConfiguration, configurations);
      try {
         writer.close();
      } catch (Exception e) {
         throw new ConfigurationWriterException(e);
      }
   }

   /**
    * Serializes a full configuration to an {@link ConfigurationWriter}
    *
    * @param writer the writer where the configuration should be serialized to
    * @param globalConfiguration the global configuration. Can be null
    * @param configurations a map of named configurations
    */
   public void serialize(ConfigurationWriter writer, GlobalConfiguration globalConfiguration, Map<String, Configuration> configurations) {
      serializeWith(writer, new CoreConfigurationSerializer(), new ConfigurationHolder(globalConfiguration, configurations));
   }

   public <T> void serializeWith(ConfigurationWriter writer, ConfigurationSerializer<T> serializer, T t) {
      writer.writeStartDocument();
      serializer.serialize(writer, t);
      writer.writeEndDocument();
   }

   /**
    * Serializes a single configuration to an OutputStream
    * @param os
    * @param name
    * @param configuration
    * @deprecated use {@link #serialize(ConfigurationWriter, GlobalConfiguration, Map)} instead
    */
   @Deprecated
   public void serialize(OutputStream os, String name, Configuration configuration) {
      serialize(os, null, Collections.singletonMap(name, configuration));
   }

   /**
    * Serializes a single configuration to a String
    * @param name the name of the configuration
    * @param configuration the {@link Configuration}
    * @return the XML representation of the specified configuration
    * @deprecated use {@link #serialize(ConfigurationWriter, GlobalConfiguration, Map)} instead
    */
   @Deprecated
   public String serialize(String name, Configuration configuration) {
      try {
         ByteArrayOutputStream os = new ByteArrayOutputStream();
         serialize(os, name, configuration);
         return os.toString("UTF-8");
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
   }

   @Override
   public String toString() {
      return "ParserRegistry{}";
   }

   /**
    * Serializes a single cache configuration
    * @param writer
    * @param name
    * @param configuration
    */
   public void serialize(ConfigurationWriter writer, String name, Configuration configuration) {
      writer.writeStartDocument();
      CoreConfigurationSerializer serializer = new CoreConfigurationSerializer();
      serializer.writeCache(writer, name, configuration);
      writer.writeEndDocument();
   }

   public static class NamespaceParserPair {
      Namespace namespace;
      ConfigurationParser parser;

      NamespaceParserPair(Namespace namespace, ConfigurationParser parser) {
         this.namespace = namespace;
         this.parser = parser;
      }

      @Override
      public String toString() {
         return "NamespaceParserPair{" +
               "namespace=" + namespace +
               ", parser=" + parser +
               '}';
      }
   }

   public static class QName {
      final String namespace;
      final String name;

      public QName(String namespace, String name) {
         this.namespace = namespace;
         this.name = name;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         QName qName = (QName) o;
         return Objects.equals(namespace, qName.namespace) && Objects.equals(name, qName.name);
      }

      @Override
      public int hashCode() {
         return Objects.hash(namespace, name);
      }

      @Override
      public String toString() {
         return this.namespace.equals("") ? this.name : "{" + this.namespace + "}" + this.name;
      }
   }
}
