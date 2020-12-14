package org.infinispan.commons.configuration.io;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.infinispan.commons.configuration.io.json.JsonConfigurationWriter;
import org.infinispan.commons.configuration.io.xml.XmlConfigurationWriter;
import org.infinispan.commons.configuration.io.yaml.YamlConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface ConfigurationWriter extends AutoCloseable {

   class Builder {
      private final BufferedWriter writer;
      private MediaType type = MediaType.APPLICATION_XML;
      private boolean prettyPrint = true;

      private Builder(OutputStream os) {
         this(new OutputStreamWriter(os));
      }

      private Builder(Writer writer) {
         this.writer = writer instanceof BufferedWriter ? (BufferedWriter) writer : new BufferedWriter(writer);
      }

      public ConfigurationWriter.Builder withType(MediaType type) {
         this.type = type;
         return this;
      }

      public ConfigurationWriter.Builder prettyPrint(boolean prettyPrint) {
         this.prettyPrint = prettyPrint;
         return this;
      }

      public ConfigurationWriter build() {
         switch (type.toString()) {
            case MediaType.APPLICATION_XML_TYPE:
               return new XmlConfigurationWriter(writer, prettyPrint);
            case MediaType.APPLICATION_YAML_TYPE:
               return new YamlConfigurationWriter(writer);
            case MediaType.APPLICATION_JSON_TYPE:
               return new JsonConfigurationWriter(writer, prettyPrint);
            default:
               throw new IllegalArgumentException(type.toString());
         }
      }
   }

   static ConfigurationWriter.Builder to(OutputStream os) {
      return new ConfigurationWriter.Builder(os);
   }

   static ConfigurationWriter.Builder to(Writer writer) {
      return new ConfigurationWriter.Builder(writer);
   }

   void writeStartDocument();

   void writeStartElement(String name);

   void writeStartElement(Enum<?> name);

   void writeStartElement(String prefix, String namespace, String name);

   void writeStartElement(String prefix, String namespace, Enum<?> name);

   void writeStartListElement(String name, boolean explicit);

   void writeStartListElement(Enum<?> name, boolean explicit);

   void writeStartListElement(String prefix, String namespace, String name, boolean explicit);

   void writeStartListElement(String prefix, String namespace, Enum<?> name, boolean explicit);

   void writeEndListElement();

   void writeStartMapElement(String name);

   void writeStartMapElement(Enum<?> name);

   void writeEndMapElement();

   void writeStartMapEntry(String name, String key, String value);

   void writeStartMapEntry(Enum<?> name, Enum<?> key, String value);

   void writeDefaultNamespace(String namespace);

   void writeEndElement();

   void writeEndDocument();

   void writeAttribute(Enum<?> name, String value);

   void writeAttribute(String name, String value);

   void writeCharacters(String chars);

   void writeEmptyElement(String name);

   void writeEmptyElement(Enum<?> name);

   void writeComment(String comment);

   void writeNamespace(String prefix, String namespace);
}
