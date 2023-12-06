package org.infinispan.commons.configuration.attributes;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

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
   AttributeSerializer<String> SECRET = (writer, name, value) -> {
      if (writer.clearTextSecrets()) {
         writer.writeAttribute(name, value);
      } else {
         writer.writeAttribute(name, "***");
      }
   };
   AttributeSerializer<String[]> STRING_ARRAY = (writer, name, value) -> writer.writeAttribute(name, Arrays.asList(value));
   AttributeSerializer<Collection<String>> STRING_COLLECTION = ConfigurationWriter::writeAttribute;
   AttributeSerializer<Collection<? extends Enum<?>>> ENUM_COLLECTION = (writer, name, value) -> writer.writeAttribute(name, value.stream().map(Enum::toString).collect(Collectors.toList()));
   AttributeSerializer<Object> INSTANCE_CLASS_NAME = ((writer, name, value) -> writer.writeAttribute(name, value.getClass().getName()));
   AttributeSerializer<Class> CLASS_NAME = ((writer, name, value) -> writer.writeAttribute(name, value.getName()));

   void serialize(ConfigurationWriter writer, String name, T value);

   default boolean defer() {
      return false;
   }
}
