package org.infinispan.commons.configuration.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.configuration.io.json.JsonConfigurationReader;
import org.infinispan.commons.configuration.io.xml.XmlConfigurationReader;
import org.infinispan.commons.configuration.io.yaml.YamlConfigurationReader;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface ConfigurationReader extends AutoCloseable {

   class Builder {
      final private BufferedReader reader;
      private MediaType type;
      private PropertyReplacer replacer = PropertyReplacer.DEFAULT;
      private Properties properties = new Properties();
      private ConfigurationResourceResolver resolver = ConfigurationResourceResolver.DEFAULT;
      private NamingStrategy namingStrategy = NamingStrategy.IDENTITY;

      private Builder(InputStream is) {
         this(new InputStreamReader(is));
      }

      private Builder(Reader reader) {
         this.reader = reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
      }

      public Builder withProperties(Properties properties) {
         this.properties = properties;
         return this;
      }

      public Builder withType(MediaType type) {
         this.type = type;
         return this;
      }

      public Builder withReplacer(PropertyReplacer replacer) {
         this.replacer = replacer;
         return this;
      }

      public Builder withResolver(ConfigurationResourceResolver resolver) {
         this.resolver = resolver;
         return this;
      }

      public Builder withNamingStrategy(NamingStrategy namingStrategy) {
         this.namingStrategy = namingStrategy;
         return this;
      }

      private int firstChar() {
         try {
            reader.mark(16);
            int first = reader.read();
            reader.reset();
            return first;
         } catch (IOException e) {
            throw new ConfigurationReaderException(e, Location.of(1,1));
         }
      }

      public ConfigurationReader build() {
         if (type == null) {
            // Auto-detect
            int first = firstChar();
            switch (first) {
               case '<':
                  type = MediaType.APPLICATION_XML;
                  break;
               case '{':
                  type = MediaType.APPLICATION_JSON;
                  break;
               default:
                  // no way to detect it easily
                  type = MediaType.APPLICATION_YAML;
                  break;
            }
         }
         switch (type.getTypeSubtype()) {
            case MediaType.APPLICATION_XML_TYPE:
               return new XmlConfigurationReader(reader, resolver, properties, replacer, namingStrategy);
            case MediaType.APPLICATION_YAML_TYPE:
               return new YamlConfigurationReader(reader, resolver, properties, replacer, namingStrategy);
            case MediaType.APPLICATION_JSON_TYPE:
               return new JsonConfigurationReader(reader, resolver, properties, replacer, namingStrategy);
            default:
               throw new IllegalArgumentException(type.toString());
         }
      }
   }

   static Builder from(InputStream is) {
      return new Builder(is);
   }

   static Builder from(Reader reader) {
      return new Builder(reader);
   }

   static Builder from(String s) {
      return new Builder(new StringReader(s));
   }

   /**
    * @return the resource resolver used by this ConfigurationReader to find external references (e.g. includes)
    */
   ConfigurationResourceResolver getResourceResolver();

   /**
    * @return the naming strategy used by this ConfigurationReader
    */
   NamingStrategy getNamingStrategy();

   /**
    * @param schema the ConfigurationSchema in use
    */
   void setSchema(ConfigurationSchemaVersion schema);

   /**
    * @return the schema
    */
   ConfigurationSchemaVersion getSchema();

   /**
    * @return the next element
    */
   ElementType nextElement();

   default boolean inTag() {
      return hasNext() && (nextElement() != ConfigurationReader.ElementType.END_ELEMENT);
   }

   default boolean inTag(String name) {
      return hasNext() && (nextElement() != ConfigurationReader.ElementType.END_ELEMENT || !name.equals(getLocalName()));
   }

   default boolean inTag(Enum<?> name) {
      return inTag(name.toString());
   }

   Location getLocation();

   Object getProperty(String name);

   Properties getProperties();

   String getAttributeName(int index);

   String getAttributeName(int index, NamingStrategy strategy);

   String getAttributeNamespace(int index);

   String getAttributeValue(String localName);

   default String getAttributeValue(Enum<?> localName) {
      return getAttributeValue(localName.toString());
   }

   String getAttributeValue(String localName, NamingStrategy strategy);

   default String getAttributeValue(Enum<?> localName, NamingStrategy strategy) {
      return getAttributeValue(localName.toString(), strategy);
   }

   String getAttributeValue(int index);

   /**
    * Get the value of an attribute as a space-delimited string list.
    *
    * @param index the index of the attribute
    */
   default String[] getListAttributeValue(int index) {
      return getAttributeValue(index).split("\\s+");
   }

   String getElementText();

   String getLocalName();

   String getLocalName(NamingStrategy strategy);

   String getNamespace();

   boolean hasNext();

   int getAttributeCount();

   void handleAny(ConfigurationReaderContext context);

   void handleAttribute(ConfigurationReaderContext context, int i);

   default void require(ElementType type) {
      require(type, null, (String) null);
   }

   void require(ElementType type, String namespace, String name);

   default void require(ElementType type, String namespace, Enum<?> name) {
      require(type, namespace, name.toString());
   }

   Map.Entry<String, String> getMapItem(String nameAttribute);

   Map.Entry<String, String> getMapItem(Enum<?> nameAttribute);

   void endMapItem();

   String[] readArray(Enum<?> outer, Enum<?> inner);

   String[] readArray(String outer, String inner);

   boolean hasFeature(ConfigurationFormatFeature feature);

   @Override
   void close();

   void setAttributeValue(String namespace, String name, String value);

   default void setAttributeValue(String namespace, Enum<?> localName, String value) {
      setAttributeValue(namespace, localName.toString(), value);
   }

   enum ElementType {
      START_DOCUMENT,
      END_DOCUMENT,
      START_ELEMENT,
      END_ELEMENT,
      TEXT
   }
}
