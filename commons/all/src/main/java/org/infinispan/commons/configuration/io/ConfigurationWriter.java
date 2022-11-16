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
      private boolean prettyPrint = false;
      private boolean clearTextSecrets = false;

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

      public ConfigurationWriter.Builder clearTextSecrets(boolean clearTextSecrets) {
         this.clearTextSecrets = clearTextSecrets;
         return this;
      }

      public ConfigurationWriter build() {
         switch (type.getTypeSubtype()) {
            case MediaType.APPLICATION_XML_TYPE:
               return new XmlConfigurationWriter(writer, prettyPrint, clearTextSecrets);
            case MediaType.APPLICATION_YAML_TYPE:
               return new YamlConfigurationWriter(writer, clearTextSecrets);
            case MediaType.APPLICATION_JSON_TYPE:
               return new JsonConfigurationWriter(writer, prettyPrint, clearTextSecrets);
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

   boolean clearTextSecrets();

   void writeStartDocument();

   void writeStartElement(String name);

   void writeStartElement(Enum<?> name);

   void writeStartElement(String prefix, String namespace, String name);

   void writeStartElement(String prefix, String namespace, Enum<?> name);

   /**
    * Writes an array element. This will be treated as follows by the various implementations:
    * <ul>
    *    <li><strong>XML</strong> &lt;outer&gt;&lt;/outer&gt;</li>
    *    <li><strong>YAML</strong> <pre>
    *       name:<br>
    *       - item1
    *       - item2
    *    </pre>
    *    </li>
    *    <li><strong>JSON</strong> name: [ item1, item2 ]</li>
    * </ul>
    *
    * @param name
    */
   void writeStartArrayElement(String name);

   void writeStartArrayElement(Enum<?> name);

   void writeEndArrayElement();

   void writeArrayElement(String outer, String inner, String attribute, Iterable<String> values);

   void writeArrayElement(Enum<?> outer, Enum<?> inner, Enum<?> attribute, Iterable<String> values);

   /**
    * Starts a list element.
    * @param name
    * @param explicit
    */
   void writeStartListElement(String name, boolean explicit);

   void writeStartListElement(Enum<?> name, boolean explicit);

   void writeStartListElement(String prefix, String namespace, String name, boolean explicit);

   void writeStartListElement(String prefix, String namespace, Enum<?> name, boolean explicit);

   void writeEndListElement();

   void writeStartMap(String name);

   void writeStartMap(Enum<?> name);

   /**
    * Writes a simple map entry.
    * <ul>
    *    <li><strong>XML</strong>: <tt>&lt;element name="key"&gt;value&lt;element&gt;</tt></li>
    *    <li><strong>JSON</strong>: <tt>{ key: value }</tt></li>
    *    <li><strong>YAML</strong>: <tt>key: value</tt></li>
    * </ul>
    * <p>
    * The key name is not translated by the underlying serialization implementation and is used as is
    *
    * @param element Used only by XML
    * @param name Used only by XML
    * @param key
    * @param value
    */
   void writeMapItem(String element, String name, String key, String value);

   /**
    * @see #writeMapItem(String, String, String, String)
    */
   void writeMapItem(Enum<?> element, Enum<?> name, String key, String value);

   /**
    * Writes a complex map entry.
    * <ul>
    *    <li><strong>XML</strong>: <tt>&lt;element name="key"&gt;...&lt;element&gt;</tt></li>
    *    <li><strong>JSON</strong>: <tt>{ key: { ... } }</tt></li>
    *    <li><strong>YAML</strong>: <tt>key:</tt></li>
    * </ul>
    * <p>
    * The key name is not translated by the underlying serialization implementation and is used as is
    *
    * @param element Used only by XML
    * @param name Used only by XML
    * @param key
    */
   void writeMapItem(String element, String name, String key);

   void writeMapItem(Enum<?> element, Enum<?> name, String key);

   void writeEndMapItem();

   void writeEndMap();

   void writeDefaultNamespace(String namespace);

   void writeEndElement();

   void writeEndDocument();

   void writeAttribute(Enum<?> name, String value);

   void writeAttribute(String name, String value);

   void writeAttribute(Enum<?> name, boolean value);

   void writeAttribute(String name, boolean value);

   void writeAttribute(Enum<?> name, Iterable<String> values);

   void writeAttribute(String name, Iterable<String> values);

   void writeCharacters(String chars);

   void writeEmptyElement(String name);

   void writeEmptyElement(Enum<?> name);

   void writeComment(String comment);

   void writeNamespace(String prefix, String namespace);

   boolean hasFeature(ConfigurationFormatFeature feature);

   @Override
   void close();
}
