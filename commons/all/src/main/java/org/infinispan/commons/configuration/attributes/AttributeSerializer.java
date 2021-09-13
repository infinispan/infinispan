package org.infinispan.commons.configuration.attributes;

import java.util.Collection;
import java.util.function.Supplier;

import org.infinispan.commons.configuration.io.ConfigurationWriter;

/**
 * AttributeSerializer.
 *
 * @since 10.0
 */
public interface AttributeSerializer<T> {

   AttributeSerializer<Object> DEFAULT = (writer, name, value) -> {
      if (Boolean.class == value.getClass()) {
         writer.writeAttribute(name, (Boolean) value);
      } else {
         writer.writeAttribute(name, value.toString());
      }
   };
   AttributeSerializer<Supplier<char[]>> SECRET = (writer, name, value) -> {
      if (Boolean.getBoolean("org.infinispan.configuration.clear-text-secrets")) {
         writer.writeAttribute(name, new String(value.get()));
      } else {
         writer.writeAttribute(name, "***");
      }
   };
   AttributeSerializer<String[]> STRING_ARRAY = ConfigurationWriter::writeAttribute;
   AttributeSerializer<Collection<String>> STRING_COLLECTION = (writer, name, value) -> writer.writeAttribute(name, value.toArray(new String[0]));
   AttributeSerializer<Collection<? extends Enum<?>>> ENUM_COLLECTION = (writer, name, value) -> writer.writeAttribute(name, value.stream().map(Enum::toString).toArray(String[]::new));
   AttributeSerializer<Object> INSTANCE_CLASS_NAME = ((writer, name, value) -> writer.writeAttribute(name, value.getClass().getName()));
   AttributeSerializer<Class> CLASS_NAME = ((writer, name, value) -> writer.writeAttribute(name, value.getName()));

   void serialize(ConfigurationWriter writer, String name, T value);
}
