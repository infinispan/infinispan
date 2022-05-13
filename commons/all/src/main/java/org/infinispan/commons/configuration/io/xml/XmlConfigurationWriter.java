package org.infinispan.commons.configuration.io.xml;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Optional;

import org.infinispan.commons.configuration.io.AbstractConfigurationWriter;
import org.infinispan.commons.configuration.io.ConfigurationFormatFeature;
import org.infinispan.commons.configuration.io.ConfigurationWriterException;
import org.infinispan.commons.configuration.io.NamingStrategy;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class XmlConfigurationWriter extends AbstractConfigurationWriter {
   private String version = "1.0";
   private String encoding;
   private Optional<Boolean> standalone = Optional.empty();
   private boolean openTag;
   private boolean skipIndentClose;

   public XmlConfigurationWriter(Writer writer, boolean prettyPrint, boolean clearTextSecrets) {
      super(writer, 4, prettyPrint, clearTextSecrets, NamingStrategy.KEBAB_CASE);
   }

   public String getVersion() {
      return version;
   }

   public void setVersion(String version) {
      this.version = version;
   }

   public String getEncoding() {
      return encoding;
   }

   public void setEncoding(String encoding) {
      this.encoding = encoding;
   }


   public Optional<Boolean> getStandalone() {
      return standalone;
   }

   public void setStandalone(Optional<Boolean> standalone) {
      this.standalone = standalone;
   }

   @Override
   public void writeStartDocument() {
      try {
         writer.write("<?xml version=\"");
         writer.write(version);

         if (encoding != null && !encoding.isEmpty()) {
            writer.write("\" encoding=\"");
            writer.write(encoding);
         }

         if (standalone.isPresent()) {
            writer.write("\" standalone=\"");
            if (standalone.get().booleanValue()) {
               writer.write("yes");
            } else {
               writer.write("no");
            }
         }
         writer.write("\"?>");
         nl();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   private void closeCurrentTag(boolean newline) throws IOException {
      if (openTag) {
         writer.write(">");
         if (newline) {
            nl();
         }
         openTag = false;
      }
   }

   @Override
   public void writeStartElement(String name) {
      writeStartElement0(new Tag(name, false, true, true));
   }

   private void writeStartElement0(Tag tag) {
      try {
         closeCurrentTag(true);
         tagStack.push(tag);
         tab();
         writer.write("<");
         writer.write(naming.convert(tag.getName()));
         openTag = true;
         indent();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeStartElement(String prefix, String namespace, String name) {
      writeStartElement((prefix == null ? "" : (prefix + ":")) + name);
   }

   @Override
   public void writeStartArrayElement(String name) {
      writeStartElement0(new Tag(name, true, true, true));
   }

   @Override
   public void writeEndArrayElement() {
      writeEndElement();
   }

   @Override
   public void writeStartListElement(String name, boolean explicit) {
      // XML allows repeated elements without a wrapper element
      if (explicit) {
         writeStartElement(name);
      } else {
         tagStack.push(new Tag(name));
      }
   }

   @Override
   public void writeStartListElement(String prefix, String namespace, String name, boolean explicit) {
      // XML allows repeated elements without a wrapper element
      writeStartListElement((prefix == null ? "" : (prefix + ":")) + name, explicit);
   }

   @Override
   public void writeEndListElement() {
      // XML allows repeated elements without a wrapper element
      if (tagStack.peek().isExplicitOuter()) {
         writeEndElement();
      } else {
         tagStack.pop();
      }
   }

   @Override
   public void writeNamespace(String prefix, String namespace) {
      if (!openTag) {
         throw new ConfigurationWriterException("Cannot set namespace without a started element");
      }
      if (prefix == null || prefix.isEmpty()) {
         writeDefaultNamespace(namespace);
         return;
      }
      if (namespaces.containsKey(prefix)) {
         if (!namespaces.get(prefix).equals(namespace)) {
            throw new ConfigurationWriterException("Duplicate declaration of prefix '" + prefix + "' with different namespace");
         }
      } else {
         namespaces.put(prefix, namespace);
      }
      try {
         writer.write(" xmlns:");
         writer.write(prefix);
         writer.write("=\"");
         writer.write(namespace);
         writer.write("\"");
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeDefaultNamespace(String namespace) {
      if (!openTag) {
         throw new ConfigurationWriterException("Cannot set namespace without a started element");
      }
      try {
         writer.write(" xmlns=\"");
         writer.write(namespace);
         writer.write("\"");
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeEndElement() {
      try {
         outdent();
         if (openTag) {
            writer.write("/>");
            nl();
            openTag = false;
            tagStack.pop();
         } else {
            if (skipIndentClose) {
               skipIndentClose = false;
            } else {
               tab();
            }
            writer.write("</");
            writer.write(tagStack.pop().getName());
            writer.write(">");
            nl();
         }
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeEndDocument() {
      try {
         closeCurrentTag(true);
         writer.flush();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }


   @Override
   public void writeAttribute(String name, String value) {
      writeAttribute(name, value, true);
   }

   private void writeAttribute(String name, String value, boolean rename) {
      try {
         writer.write(' ');
         writer.write(rename ? naming.convert(name) : name);
         writer.write("=\"");
         if (value != null) {
            writer.write(value.replaceAll("&", "&amp;"));
         }
         writer.write('"');
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeAttribute(String name, Iterable<String> values) {
      if (values.iterator().hasNext()) {
         writeAttribute(name, String.join(" ", values));
      }
   }

   @Override
   public void writeArrayElement(String outer, String inner, String attribute, Iterable<String> values) {
      Iterator<String> it = values.iterator();
      boolean wrapped = !inner.equals(outer);
      if (it.hasNext()) {
         if (wrapped) {
            writeStartElement(outer);
         }
         while (it.hasNext()) {
            writeStartElement(inner);
            if (attribute == null) {
               writeCharacters(it.next());
            } else {
               writeAttribute(attribute, it.next());
            }
            writeEndElement();
         }
         if (wrapped) {
            writeEndElement();
         }
      }
   }

   @Override
   public void writeCharacters(String chars) {
      try {
         closeCurrentTag(false);
         writer.write(chars);
         skipIndentClose = true;
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeEmptyElement(String name) {
      try {
         closeCurrentTag(true);
         tab();
         writer.write("<");
         writer.write(naming.convert(name));
         writer.write("/>");
         nl();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeStartMap(String name) {
      writeStartElement(name);
   }

   @Override
   public void writeMapItem(String element, String name, String key, String value) {
      writeStartElement(element);
      writeAttribute(name, key, false);
      writeCharacters(value);
      writeEndElement();
   }

   @Override
   public void writeMapItem(String element, String name, String key) {
      writeStartElement(element);
      writeAttribute(name, key, false);
   }

   @Override
   public void writeEndMapItem() {
      writeEndElement();
   }

   @Override
   public void writeEndMap() {
      writeEndElement();
   }

   @Override
   public void writeComment(String comment) {
      try {
         closeCurrentTag(true);
         tab();
         writer.write("<!--");
         writer.write(comment);
         writer.write("-->");
         nl();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public boolean hasFeature(ConfigurationFormatFeature feature) {
      switch (feature) {
         case MIXED_ELEMENTS:
         case BARE_COLLECTIONS:
            return true;
         default:
            return false;
      }
   }
}
