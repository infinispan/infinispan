package org.infinispan.commons.configuration.io;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public abstract class AbstractConfigurationWriter implements ConfigurationWriter {
   protected final Writer writer;
   protected final Deque<Tag> tagStack = new ArrayDeque<>();
   protected final Map<String, String> namespaces = new HashMap<>();
   protected int currentIndent = 0;
   private final int indent;
   protected final boolean prettyPrint;
   protected final boolean clearTextSecrets;
   protected final NamingStrategy naming;

   protected AbstractConfigurationWriter(Writer writer, int indent, boolean prettyPrint, boolean clearTextSecrets, NamingStrategy naming) {
      this.writer = writer;
      this.indent = indent;
      this.prettyPrint = prettyPrint;
      this.clearTextSecrets = clearTextSecrets;
      this.naming = naming;
   }

   @Override
   public boolean clearTextSecrets() {
      return clearTextSecrets;
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
   public void writeStartArrayElement(Enum<?> name) {
      writeStartArrayElement(name.toString());
   }

   @Override
   public void writeArrayElement(Enum<?> outer, Enum<?> inner, Enum<?> attribute, Iterable<String> values) {
      writeArrayElement(outer.toString(), inner.toString(), attribute != null ? attribute.toString(): null, values);
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
   public void writeAttribute(Enum<?> name, String value) {
      writeAttribute(name.toString(), value);
   }

   @Override
   public void writeAttribute(Enum<?> name, Iterable<String> value) {
      writeAttribute(name.toString(), value);
   }

   @Override
   public void writeAttribute(Enum<?> name, boolean value) {
      writeAttribute(name.toString(), value);
   }

   @Override
   public void writeAttribute(String name, boolean value) {
      writeAttribute(name, String.valueOf(value));
   }

   @Override
   public void writeEmptyElement(Enum<?> name) {
      writeEmptyElement(name.toString());
   }

   @Override
   public void writeStartMap(Enum<?> name) {
      writeStartMap(name.toString());
   }

   @Override
   public void writeMapItem(Enum<?> element, Enum<?> name, String key, String value) {
      writeMapItem(element.toString(), name.toString(), key, value);
   }

   @Override
   public void writeMapItem(Enum<?> element, Enum<?> name, String key) {
      writeMapItem(element.toString(), name.toString(), key);
   }

   protected void nl() throws IOException {
      if (prettyPrint) {
         writer.write(System.lineSeparator());
      }
   }

   protected void tab() throws IOException {
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
   public void close() {
      Util.close(writer);
   }

   public static class Tag {
      final String name;
      final boolean repeating;
      final boolean explicitOuter;
      final boolean explicitInner;
      int children;

      public Tag(String name, boolean repeating, boolean explicitOuter, boolean explicitInner) {
         this.name = name;
         this.repeating = repeating;
         this.explicitOuter = explicitOuter;
         this.explicitInner = explicitInner;
      }

      public Tag(String name) {
         this(name, false, false, true);
      }

      public String getName() {
         return name;
      }

      public boolean isRepeating() {
         return repeating;
      }

      public boolean isExplicitOuter() {
         return explicitOuter;
      }

      public boolean isExplicitInner() {
         return explicitInner;
      }

      @Override
      public String toString() {
         return name + (repeating ? "+" : "");
      }
   }
}
