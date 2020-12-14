package org.infinispan.commons.configuration.io.xml;

import java.io.IOException;
import java.io.Writer;
import java.util.Optional;

import org.infinispan.commons.configuration.io.AbstractConfigurationWriter;
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

   public XmlConfigurationWriter(Writer writer, boolean prettyPrint) {
      super(writer, 4, prettyPrint, NamingStrategy.KEBAB_CASE);
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
      try {
         closeCurrentTag(true);
         tagStack.push(new Tag(name, false, true));
         writeIndent();
         writer.write("<");
         writer.write(naming.convert(name));
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
      if (tagStack.peek().isExplicit()) {
         writeEndElement();
      } else {
         tagStack.pop();
      }
   }

   @Override
   public void writeStartMapElement(String name) {
      // XML allows repeated elements without a wrapper element
      tagStack.push(new Tag(name));
   }

   @Override
   public void writeEndMapElement() {
      // XML allows repeated elements without a wrapper element
      tagStack.pop();
   }

   @Override
   public void writeStartMapEntry(String name, String key, String value) {
      writeStartElement(name);
      writeAttribute(key, value);
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
               writeIndent();
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
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }


   @Override
   public void writeAttribute(String name, String value) {
      try {
         writer.write(' ');
         writer.write(naming.convert(name));
         writer.write("=\"");
         writer.write(value);
         writer.write('"');
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
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
         writeIndent();
         writer.write("<");
         writer.write(naming.convert(name));
         writer.write("/>");
         nl();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeComment(String comment) {
      try {
         closeCurrentTag(true);
         writeIndent();
         writer.write("<!--");
         writer.write(comment);
         writer.write("-->");
         nl();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }
}
