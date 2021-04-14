package org.infinispan.commons.configuration.io.yaml;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import org.infinispan.commons.configuration.io.AbstractConfigurationWriter;
import org.infinispan.commons.configuration.io.ConfigurationFormatFeature;
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
      writeStartElement0(new Tag(name, false, true, true), naming);
   }

   private void writeStartElement0(Tag tag, NamingStrategy naming) {
      try {
         if (openTag) {
            nl();
         }
         Tag parent = tagStack.peek();
         tagStack.push(tag);
         tab();
         if (parent != null && parent.isRepeating()) {
            writer.write("- ");
         } else {
            writeName(tag.getName(), naming);
         }
         openTag = true;
         attributes = false;
         indent();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   private void writeName(String name, NamingStrategy naming) throws IOException {
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
   public void writeStartArrayElement(String name) {
      writeStartElement0(new Tag(name, true, true, false), naming);
   }

   @Override
   public void writeEndArrayElement() {
      writeEndElement();   }

   @Override
   public void writeStartListElement(String name, boolean explicit) {
      writeStartElement0(new Tag(name, true, explicit, true), naming);
   }

   @Override
   public void writeStartListElement(String prefix, String namespace, String name, boolean explicit) {
      writeStartListElement(prefixName(prefix, namespace, name), explicit);
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
      try {
         writer.flush();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
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
         tab();
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
   public void writeAttribute(String name, String[] values) {
      try {
         openTag = false;
         if (!attributes) {
            nl();
            attributes = true;
         }
         tab();
         writer.write(naming.convert(name));
         writer.write(":");
         nl();
         indent();
         for(String value : values) {
            tab();
            writer.write("- \"");
            writer.write(value);
            writer.write('"');
            nl();
         }
         outdent();
         nl();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeArrayElement(String outer, String inner, String attribute, Iterable<String> values) {
      try {
         Iterator<String> it = values.iterator();
         if (it.hasNext()) {
            writeStartElement(outer);
            nl();
            while (it.hasNext()) {
               tab();
               writer.write("- \"");
               writer.write(it.next());
               writer.write('"');
               nl();
            }
            writeEndElement();
         }
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

   @Override
   public void writeStartMap(String name) {
      writeStartElement(name);
   }

   @Override
   public void writeMapItem(String element, String name, String key, String value) {
      writeAttribute(key, value);
   }

   @Override
   public void writeMapItem(String element, String name, String key) {
      writeStartElement0(new Tag(key, false, true, true), NamingStrategy.IDENTITY);
      writeStartElement(element);
   }

   @Override
   public void writeEndMapItem() {
      writeEndElement();
      writeEndElement();
   }

   @Override
   public void writeEndMap() {
      writeEndElement();
   }

   @Override
   public boolean hasFeature(ConfigurationFormatFeature feature) {
      return false;
   }
}
