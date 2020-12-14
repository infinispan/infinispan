package org.infinispan.commons.configuration.io.yaml;

import java.io.IOException;
import java.io.Writer;

import org.infinispan.commons.configuration.io.AbstractConfigurationWriter;
import org.infinispan.commons.configuration.io.ConfigurationWriterException;
import org.infinispan.commons.configuration.io.NamingStrategy;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class YamlConfigurationWriter extends AbstractConfigurationWriter {
   public static final int INDENT = 2;
   private boolean openTag;
   private boolean attributes;

   public YamlConfigurationWriter(Writer writer) {
      super(writer, INDENT, true, NamingStrategy.CAMEL_CASE);
   }

   @Override
   public void writeStartDocument() {
   }

   @Override
   public void writeStartElement(String name) {
      writeStartElement0(name, false, true);
   }

   private void writeStartElement0(String name, boolean repeated, boolean explicit) {
      try {
         if (openTag) {
            nl();
         }
         Tag parent = tagStack.peek();
         tagStack.push(new Tag(name, repeated, explicit));
         writeIndent();
         if (parent != null && parent.isRepeating()) {
            writer.write("- ");
            if (!parent.getName().equals(name)) {
               writeName(name);
            }
         } else {
            writeName(name);
         }
         openTag = true;
         attributes = false;
         indent();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   private void writeName(String name) throws IOException {
      writer.write(naming.convert(name));
      writer.write(": ");
   }

   @Override
   public void writeStartElement(String prefix, String namespace, String name) {
      writeStartElement(prefixName(prefix, namespace, name));
   }

   private String prefixName(String prefix, String namespace, String name) {
      if (prefix == null) {
         return name;
      } else if (namespaces.containsKey(prefix)) {
         return prefix + ":" + name;
      } else {
         return namespace + ":" + name;
      }
   }

   @Override
   public void writeStartListElement(String name, boolean explicit) {
      writeStartRepeatedElement(name, explicit);
   }

   @Override
   public void writeStartListElement(String prefix, String namespace, String name, boolean explicit) {
      writeStartListElement(prefixName(prefix, namespace, name), explicit);
   }

   @Override
   public void writeStartMapElement(String name) {
      writeStartRepeatedElement(name, false);
   }

   @Override
   public void writeEndMapElement() {
      writeEndElement();
   }

   @Override
   public void writeStartMapEntry(String name, String key, String value) {
      writeStartElement(value);
   }

   private void writeStartRepeatedElement(String name, boolean explicit) {
      writeStartElement0(name, true, explicit);
   }

   public void writeNamespace(String prefix, String namespace) {
      if (!openTag) {
         throw new ConfigurationWriterException("Cannot set namespace without a started element");
      }
   }

   @Override
   public void writeDefaultNamespace(String namespace) {
      if (!openTag) {
         throw new ConfigurationWriterException("Cannot set namespace without a started element");
      }
   }

   @Override
   public void writeEndElement() {
      openTag = false;
      attributes = false;
      tagStack.pop();
      outdent();
   }

   @Override
   public void writeEndDocument() {
      if (!tagStack.isEmpty()) {
         throw new ConfigurationWriterException("Tag stack not empty: " + tagStack);
      }
   }

   @Override
   public void writeAttribute(String name, String value) {
      try {
         openTag = false;
         if (!attributes) {
            nl();
            attributes = true;
         }
         writeIndent();
         writer.write(naming.convert(name));
         writer.write(": \"");
         writer.write(value);
         writer.write('"');
         nl();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeCharacters(String chars) {
      try {
         if (attributes) {
            writeAttribute("value", chars);
         } else {
            writer.write("\"");
            writer.write(chars);
            writer.write('"');
            nl();
         }
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeEmptyElement(String name) {
      try {
         writeStartElement(name);
         writer.write('~');
         nl();
         writeEndElement();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeComment(String comment) {
      try {
         writer.write("# ");
         writer.write(comment);
         nl();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }
}
