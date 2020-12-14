package org.infinispan.commons.configuration.io.json;

import java.io.IOException;
import java.io.Writer;

import org.infinispan.commons.configuration.io.AbstractConfigurationWriter;
import org.infinispan.commons.configuration.io.ConfigurationWriterException;
import org.infinispan.commons.configuration.io.NamingStrategy;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class JsonConfigurationWriter extends AbstractConfigurationWriter {
   private boolean openTag;
   private boolean attributes;

   public JsonConfigurationWriter(Writer writer, boolean prettyPrint) {
      super(writer,2, prettyPrint, NamingStrategy.KEBAB_CASE);
   }

   @Override
   public void writeStartDocument() {
      try {
         writer.write('{');
         nl();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeStartElement(String name) {
      try {
         if (attributes) {
            writer.write(',');
            nl();
         }
         tagStack.push(new Tag(name));
         writeIndent();
         writer.write('"');
         writer.write(naming.convert(name));
         writer.write("\": {");
         openTag = true;
         attributes = false;
         nl();
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
      try {
         writer.write('[');
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeStartListElement(String prefix, String namespace, String name, boolean explicit) {
      writeStartListElement(prefixName(prefix, name), explicit);
   }

   private String prefixName(String prefix, String name) {
      return (prefix == null ? "" : (prefix + ":")) + name;
   }

   @Override
   public void writeEndListElement() {
      try {
         writer.write(']');
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeStartMapElement(String name) {
   }

   @Override
   public void writeEndMapElement() {
   }

   @Override
   public void writeStartMapEntry(String name, String key, String value) {

   }

   @Override
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
      try {
         nl();
         outdent();
         writeIndent();
         writer.write('}');
         nl();
         openTag = false;
         attributes = false;
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeEndDocument() {
      try {
         writer.write('}');
         nl();
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeAttribute(String name, String value) {
      try {
         openTag = false;
         if (!attributes) {
            attributes = true;
         } else {
            writer.write(',');
            nl();
         }
         writeIndent();
         writer.write('"');
         writer.write(name);
         writer.write("\": \"");
         writer.write(value);
         writer.write('"');
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
            writer.write(": \"");
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
      writeStartElement(name);
      writeCharacters("");
      writeEndElement();
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
