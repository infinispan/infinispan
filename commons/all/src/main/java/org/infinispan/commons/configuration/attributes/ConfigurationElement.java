package org.infinispan.commons.configuration.attributes;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.Util;

/**
 * An abstract class which represents a configuration element, with attributes and child elements.
 *
 * @author Gustavo Fernandes
 * @author Tristan Tarrant
 * @since 13.0
 **/
public abstract class ConfigurationElement<T extends ConfigurationElement> implements Matchable<T>, Updatable<T> {
   public static final ConfigurationElement<?>[] CHILDLESS = new ConfigurationElement[0];
   protected final String element;
   protected final AttributeSet attributes;
   protected final ConfigurationElement<?>[] children;
   protected final boolean repeated;

   protected ConfigurationElement(Enum<?> element, AttributeSet attributes, ConfigurationElement<?>... children) {
      this(element.toString(), false, attributes, children);
   }

   protected ConfigurationElement(String element, AttributeSet attributes, ConfigurationElement<?>... children) {
      this(element, false, attributes, children);
   }

   protected ConfigurationElement(String element, boolean repeated, AttributeSet attributes, ConfigurationElement<?>... children) {
      this.element = element;
      this.repeated = repeated;
      this.attributes = attributes.checkProtection();
      this.children = children != null ? children : CHILDLESS;
   }

   public final String elementName() {
      return element;
   }

   public final AttributeSet attributes() {
      return attributes;
   }

   public ConfigurationElement<?>[] children() {
      return children;
   }

   public Attribute<?> findAttribute(String name) {
      int sep = name.indexOf('.');
      if (sep < 0) {
         if (!attributes.contains(name)) {
            throw Log.CONFIG.noAttribute(name, element);
         } else {
            return attributes.attribute(name);
         }
      } else {
         String part = name.substring(0, sep);
         for (ConfigurationElement<?> child : children) {
            if (part.equals(child.elementName())) {
               return child.findAttribute(name.substring(sep + 1));
            }
         }
         throw Log.CONFIG.noAttribute(name, element);
      }
   }

   protected static <T extends ConfigurationElement> ConfigurationElement<T> list(Enum<?> element, List<T> list) {
      ConfigurationElement[] configurationElements = list.toArray(new ConfigurationElement[0]);
      return new ConfigurationElement<T>(element, AttributeSet.EMPTY, configurationElements) {
      };
   }

   @Override
   public boolean matches(T other) {
      if (!attributes.matches(other.attributes)) return false;
      if (children.length != other.children.length) return false;
      for (int i = 0; i < children.length; i++) {
         ConfigurationElement ours = children[i];
         ConfigurationElement theirs = other.children[i];
         if (!ours.matches(theirs)) return false;
      }
      return true;
   }

   @Override
   public void update(T other) {
      this.attributes.update(other.attributes);
      for (int i = 0; i < children.length; i++) {
         ConfigurationElement ours = children[i];
         ConfigurationElement theirs = other.children[i];
         ours.update(theirs);
      }
   }

   @Override
   public void validateUpdate(T other) {
      IllegalArgumentException iae = new IllegalArgumentException();
      try {
         this.attributes.validateUpdate(other.attributes);
      } catch (Throwable t) {
         Util.unwrapSuppressed(iae, t);
      }
      for (int i = 0; i < children.length; i++) {
         ConfigurationElement ours = children[i];
         ConfigurationElement theirs = other.children[i];
         try {
            ours.validateUpdate(theirs);
         } catch (Throwable t) {
            Util.unwrapSuppressed(iae, t);
         }
      }
      if (iae.getSuppressed().length > 0) {
         throw iae;
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ConfigurationElement<?> that = (ConfigurationElement<?>) o;
      return attributes.equals(that.attributes) && Arrays.equals(children, that.children);
   }

   @Override
   public int hashCode() {
      int result = Objects.hash(attributes);
      result = 31 * result + Arrays.hashCode(children);
      return result;
   }

   @Override
   public String toString() {
      return attributes.toString();
   }

   public boolean isModified() {
      if (attributes.isModified()) return true;
      for (ConfigurationElement<?> child : children) {
         if (child.isModified()) return true;
      }
      return false;
   }

   /**
    * Writes this {@link ConfigurationElement} to the writer
    *
    * @param writer
    */
   public void write(ConfigurationWriter writer) {
      if (isModified()) {
         if (attributes.attributes().isEmpty() && children.length > 0 && Arrays.stream(children).allMatch(c -> children[0].element.equals(c.element))) {
            // Simple array: all children are homogeneous
            writer.writeStartListElement(element, true);
            for (ConfigurationElement<?> child : children) {
               child.write(writer);
            }
            writer.writeEndListElement();
         } else {
            writer.writeStartElement(element);
            attributes.write(writer);
            String repeatElement = null;
            for (ConfigurationElement<?> child : children) {
               if (child.repeated) {
                  if (!child.element.equals(repeatElement)) {
                     if (repeatElement != null) {
                        writer.writeEndListElement();
                     }
                     repeatElement = child.element;
                     writer.writeStartListElement(repeatElement, false);
                  }
               } else {
                  repeatElement = null;
               }
               child.write(writer);
            }
            if (repeatElement != null) {
               writer.writeEndListElement();
            }
            writer.writeEndElement();
         }
      }
   }

   protected static <T> ConfigurationElement<?>[] children(Collection<T> children) {
      return children.toArray(CHILDLESS);
   }

   protected static ConfigurationElement<?> child(Attribute<?> attribute) {
      return new AttributeAsElement(attribute);
   }

   private static class AttributeAsElement extends ConfigurationElement<AttributeAsElement> {
      private final Attribute<?> attribute;

      protected AttributeAsElement(Attribute<?> attribute) {
         super(attribute.name(), false, AttributeSet.EMPTY, CHILDLESS);
         this.attribute = attribute;
      }

      @Override
      public void write(ConfigurationWriter writer) {
         attribute.write(writer, attribute.name());
      }
   }
}
