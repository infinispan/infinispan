package org.infinispan.factories.components;

/**
 * Metadata for JMX attributes.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public final class JmxAttributeMetadata {

   private final String name;
   private final String description;
   private final boolean writable;
   private final boolean useSetter;
   private final String type;
   private final boolean is;

   public JmxAttributeMetadata(String name, String description, boolean writable, boolean useSetter, String type, boolean is) {
      this.name = name;
      this.description = description;
      this.writable = writable;
      this.useSetter = useSetter;
      this.type = type;
      this.is = is;
   }

   public String getName() {
      return name;
   }

   public String getDescription() {
      return description;
   }

   public boolean isWritable() {
      return writable;
   }

   public boolean isUseSetter() {
      return useSetter;
   }

   public String getType() {
      return type;
   }

   public boolean isIs() {
      return is;
   }

   @Override
   public String toString() {
      return "JmxAttributeMetadata{" +
            "name='" + name + '\'' +
            ", description='" + description + '\'' +
            ", writable=" + writable +
            ", type='" + type + '\'' +
            ", is=" + is +
            '}';
   }
}
