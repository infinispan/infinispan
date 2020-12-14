package org.infinispan.commons.configuration.io;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public abstract class AbstractConfigurationWriter implements ConfigurationWriter {
   final protected Writer writer;
   final protected Deque<Tag> tagStack = new ArrayDeque<>();
   final protected Map<String, String> namespaces = new HashMap<>();
   protected int currentIndent = 0;
   private final int indent;
   private final boolean prettyPrint;
   protected final NamingStrategy naming;

   protected AbstractConfigurationWriter(Writer writer, int indent, boolean prettyPrint, NamingStrategy naming) {
      this.writer = writer;
      this.indent = indent;
      this.prettyPrint = prettyPrint;
      this.naming = naming;
   }

   @Override
   public void writeStartElement(Enum<?> name) {
      writeStartElement(name.toString());
   }

   @Override
   public void writeStartElement(String prefix, String namespace, Enum<?> name) {
      writeStartElement(prefix, namespace, name.toString());
   }

   @Override
   public void writeStartListElement(Enum<?> name, boolean explicit) {
      writeStartListElement(name.toString(), explicit);
   }

   @Override
   public void writeStartListElement(String prefix, String namespace, Enum<?> name, boolean explicit) {
      writeStartListElement(prefix, namespace, name.toString(), explicit);
   }

   @Override
   public void writeEndListElement() {
      writeEndElement();
   }

   @Override
   public void writeStartMapElement(Enum<?> name) {
      writeStartMapElement(name.toString());
   }

   @Override
   public void writeStartMapEntry(Enum<?> name, Enum<?> key, String value) {
      writeStartMapEntry(name.toString(), key.toString(), value);
   }

   @Override
   public void writeAttribute(Enum<?> name, String value) {
      writeAttribute(name.toString(), value);
   }

   @Override
   public void writeEmptyElement(Enum<?> name) {
      writeEmptyElement(name.toString());
   }

   protected void nl() throws IOException {
      if (prettyPrint) {
         writer.write(System.lineSeparator());
      }
   }

   protected void writeIndent() throws IOException {
      if (prettyPrint) {
         for (int i = 0; i < currentIndent; i++) {
            writer.write(' ');
         }
      }
   }

   protected void indent() {
      currentIndent += indent;
   }

   protected void outdent() {
      currentIndent -= indent;
   }

   @Override
   public void close() throws Exception {
      writer.close();
   }

   public static class Tag {
      final String name;
      final boolean repeating;
      final boolean explicit;

      public Tag(String name, boolean repeating, boolean explicit) {
         this.name = name;
         this.repeating = repeating;
         this.explicit = explicit;
      }

      public Tag(String name) {
         this(name, false, false);
      }

      public String getName() {
         return name;
      }

      public boolean isRepeating() {
         return repeating;
      }

      public boolean isExplicit() {
         return explicit;
      }

      @Override
      public String toString() {
         return name + (repeating ? "+" : "");
      }
   }
}
