package org.infinispan.commons.configuration.io.json;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.Deque;

import org.infinispan.commons.configuration.io.AbstractConfigurationWriter;
import org.infinispan.commons.configuration.io.ConfigurationFormatFeature;
import org.infinispan.commons.configuration.io.ConfigurationWriterException;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.dataconversion.internal.Json;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class JsonConfigurationWriter extends AbstractConfigurationWriter {
   private boolean openTag;
   private final Deque<Json> json;

   public JsonConfigurationWriter(Writer writer, boolean prettyPrint, boolean clearTextSecrets) {
      super(writer, 2, prettyPrint, clearTextSecrets, NamingStrategy.KEBAB_CASE);
      json = new ArrayDeque<>();
   }

   @Override
   public void writeStartDocument() {
      json.push(Json.object());
   }

   @Override
   public void writeStartElement(String name) {
      writeStartElement(name, true);
   }

   private void writeStartElement(String name, boolean convert) {
      if (convert) {
         name = naming.convert(name);
      }
      Tag parentTag = tagStack.peek();
      tagStack.push(new Tag(name, false, true, true));
      Json object = Json.object();
      Json parent = json.peek();
      if (parent.isArray()) {
         parent.add(object);
      } else if (parentTag != null && parentTag.isRepeating()) {
         if (parent.has(name)) {
            parent.at(name).add(object);
         } else {
            parent.set(name, Json.array(object));
         }
      } else {
         parent.set(name, object);
      }
      json.push(object);
      openTag = true;
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
      tagStack.push(new Tag(name, true, true, false));
      Json array = Json.array();
      json.peek().set(name, array);
      json.push(array);
   }

   @Override
   public void writeEndArrayElement() {
      tagStack.pop();
      json.pop();
   }

   @Override
   public void writeStartListElement(String name, boolean explicit) {
      tagStack.push(new Tag(name, true, explicit, true));
      Json array = Json.array();
      json.peek().set(name, array);
      json.push(array);
      openTag = true;
   }

   @Override
   public void writeStartListElement(String prefix, String namespace, String name, boolean explicit) {
      writeStartListElement(prefixName(prefix, namespace, name), explicit);
   }

   @Override
   public void writeEndListElement() {
      tagStack.pop();
      json.pop();
      openTag = false;
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
      tagStack.pop();
      json.pop();
      openTag = false;
   }

   @Override
   public void writeEndDocument() {
      try {
         Json json = this.json.pop();
         writer.write(prettyPrint ? json.toPrettyString() : json.toString());
      } catch (IOException e) {
         throw new ConfigurationWriterException(e);
      }
   }

   @Override
   public void writeAttribute(String name, Iterable<String> values) {
      Json parent = attributeParent();
      Json array = Json.array();
      for (String value : values) {
         array.add(value);
      }
      parent.set(name, array);
   }

   private Json attributeParent() {
      Json parent = json.peek();
      if (parent.isArray()) { // Replace the array with an object
         json.pop();
         parent = json.peek().replace(parent, Json.object());
         json.push(parent);
      }
      return parent;
   }

   @Override
   public void writeAttribute(String name, String value) {
      Json parent = attributeParent();
      parent.set(name, value);
   }

   @Override
   public void writeAttribute(String name, boolean value) {
      Json parent = attributeParent();
      parent.set(name, value);
   }

   @Override
   public void writeArrayElement(String outer, String inner, String attribute, Iterable<String> values) {
      Json array = Json.array();
      values.forEach(array::add);
      json.peek().set(outer, array);
   }

   @Override
   public void writeCharacters(String chars) {
      Tag tag = tagStack.peek();
      Json parent = json.peek();
      if (openTag && parent.isObject()) {
         json.pop();
         parent = json.peek();
         parent.set(tag.getName(), chars);
         json.push(parent);
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void writeEmptyElement(String name) {
      json.peek().set(name, null);
   }

   @Override
   public void writeComment(String comment) {
      // Comments are unsupported in JSON
   }

   @Override
   public void writeStartMap(String name) {
      writeStartElement(name);
   }

   @Override
   public void writeMapItem(String element, String name, String key, String value) {
      json.peek().set(key, value);
   }

   @Override
   public void writeMapItem(String element, String name, String key) {
      writeStartElement(key, false);
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
