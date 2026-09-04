package org.infinispan.commons.configuration.attributes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.infinispan.commons.configuration.io.ConfigurationSchemaVersion;
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
   protected final int sinceMajor;
   protected final int sinceMinor;

   protected ConfigurationElement(Enum<?> element, AttributeSet attributes, ConfigurationElement<?>... children) {
      this(element.toString(), false, attributes, children);
   }

   protected ConfigurationElement(String element, AttributeSet attributes, ConfigurationElement<?>... children) {
      this(element, false, attributes, children);
   }

   protected ConfigurationElement(String element, boolean repeated, AttributeSet attributes, ConfigurationElement<?>... children) {
      this(element, repeated, 4, 0, attributes, children);
   }

   protected ConfigurationElement(String element, boolean repeated, int sinceMajor, int sinceMinor, AttributeSet attributes, ConfigurationElement<?>... children) {
      this.element = element;
      this.repeated = repeated;
      this.sinceMajor = sinceMajor;
      this.sinceMinor = sinceMinor;
      this.attributes = attributes.checkProtection();
      this.children = (children != null && children.length > 0) ? children : CHILDLESS;
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

   public void collectMutableAttributes(String prefix, Map<String, Attribute<?>> collector) {
      String qualifiedPrefix = prefix == null ? element : prefix + "." + element;
      for (Attribute<?> attribute : attributes.attributes()) {
         if (!attribute.isImmutable()) {
            collector.put(qualifiedPrefix + "." + attribute.getAttributeDefinition().name(), attribute);
         }
      }
      for (ConfigurationElement<?> child : children) {
         child.collectMutableAttributes(qualifiedPrefix, collector);
      }
   }

   protected static <T extends ConfigurationElement> ConfigurationElement<T> list(Enum<?> element, List<T> list) {
      ConfigurationElement[] configurationElements = list.toArray(new ConfigurationElement[0]);
      return new ConfigurationElement<T>(element, AttributeSet.EMPTY, configurationElements) {
      };
   }

   @Override
   public boolean matches(T other) {
      return internalMatches(other);
   }

   public boolean matches(T other, ConfigurationElement<?> parent) {
      return matches(other);
   }

   public boolean matches(T other, Attribute<?> ... ignored) {
      return internalMatches(other, ignored);
   }

   public final boolean isSince(int major, int minor) {
      return major > sinceMajor || (major == sinceMajor && minor >= sinceMinor);
   }

   private boolean internalMatches(T other, Attribute<?> ... ignored) {
      if (!attributes.matches(other.attributes, ignored)) return false;
      if (children.length != other.children.length) return false;
      for (int i = 0; i < children.length; i++) {
         ConfigurationElement ours = children[i];
         ConfigurationElement theirs = other.children[i];
         if (!ours.matches(theirs, this)) return false;
      }
      return true;
   }

   public void update(String parentName, T other, Attribute<?> ... ignored) {
      updateInternal(parentName, other, ignored);
   }

   public void update(String parentName, T other, ConfigurationElement<?> parent) {
      update(parentName, other);
   }

   @Override
   public void update(String parentName, T other) {
      updateInternal(parentName, other);
   }

   private void updateInternal(String parentName, T other, Attribute<?> ... ignored) {
      String qualifiedName = qualifiedName(parentName);
      this.attributes.update(qualifiedName, other.attributes, ignored);
      for (int i = 0; i < children.length; i++) {
         ConfigurationElement ours = children[i];
         ConfigurationElement theirs = other.children[i];
         ours.update(qualifiedName, theirs, this);
      }
   }

   public void validateUpdate(String parentName, T other, ConfigurationElement<?> parent) {
      validateUpdate(parentName, other);
   }

   public void validateUpdate(String parentName, T other, Attribute<?> ... ignored) {
      validateUpdateInternal(parentName, other, ignored);
   }

   @Override
   public void validateUpdate(String parentName, T other) {
      validateUpdateInternal(parentName, other);
   }

   private void validateUpdateInternal(String parentName, T other, Attribute<?> ... ignored) {
      String qualifiedName = qualifiedName(parentName);
      IllegalArgumentException iae = Log.CONFIG.invalidConfiguration(qualifiedName);
      try {
         this.attributes.validateUpdate(qualifiedName, other.attributes, ignored);
      } catch (Throwable t) {
         Util.unwrapSuppressed(iae, t);
      }
      for (int i = 0; i < children.length; i++) {
         ConfigurationElement ours = children[i];
         ConfigurationElement theirs = other.children[i];
         try {
            ours.validateUpdate(qualifiedName, theirs, this);
         } catch (Throwable t) {
            Util.unwrapSuppressed(iae, t);
         }
      }
      if (iae.getSuppressed().length > 0) {
         throw iae;
      }
   }

   private String qualifiedName(String parentName) {
      return parentName == null ? element : parentName + "." + element;
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
      if (children == CHILDLESS) {
         return attributes.toString(null);
      } else {
         StringBuilder sb = new StringBuilder();
         sb.append('[');
         sb.append(attributes.toString(null));
         for(ConfigurationElement<?> child : children){
            sb.append(", ");
            sb.append(child.elementName());
            sb.append('=');
            sb.append(child);
         }
         sb.append(']');
         return sb.toString();
      }
   }

   public boolean isModified() {
      if (attributes.isModified()) return true;
      for (ConfigurationElement<?> child : children) {
         if (child.isModified()) return true;
      }
      return false;
   }

   /**
    * Writes this {@link ConfigurationElement} to the writer.
    *
    * <p>
    * If the writer carries a target {@link ConfigurationSchemaVersion} (see
    * {@link ConfigurationWriter.Builder#targetVersion(ConfigurationSchemaVersion)}), the whole element (and its subtree)
    * is omitted when its own {@code since} is newer than the target, and any child elements newer than the target are
    * excluded before children are wrapped in a list or repeated-group element, so no empty wrapper is ever emitted for
    * a fully-gated group.
    * </p>
    *
    * @param writer the writer to serialise this element into
    */
   public void write(ConfigurationWriter writer) {
      ConfigurationSchemaVersion target = writer.targetVersion();
      if (target != null && !isSince(target.getMajor(), target.getMinor()))
         return;

      if (isModified()) {
         // Filter to the target-eligible subset *before* deciding how to wrap children.
         // This ensures we only iterate over children that accepts the requested version for serialisation.
         ConfigurationElement<?>[] eligibleChildren = target == null
               ? children
               : Arrays.stream(children).filter(c -> c.isSince(target.getMajor(), target.getMinor())).toArray(ConfigurationElement<?>[]::new);

         // A list container, without attributes, which exists only to hold children elements.
         // If all the children containers are filtered out, the outer container should be skipped, too.
         if (attributes.attributes().isEmpty() && children.length > 0 && eligibleChildren.length == 0)
            return;

         if (attributes.attributes().isEmpty()
               && eligibleChildren.length > 0
               && Stream.of(eligibleChildren).allMatch(c -> eligibleChildren[0].element.equals(c.element))) {
            // Simple array: all children are homogeneous
            writer.writeStartListElement(element, true);
            for (ConfigurationElement<?> child : eligibleChildren) {
               child.write(writer);
            }
            writer.writeEndListElement();
         } else {
            writer.writeStartElement(element);
            attributes.write(writer);
            String repeatElement = null;
            for (ConfigurationElement<?> child : eligibleChildren) {
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
         ConfigurationSchemaVersion target = writer.targetVersion();
         AttributeDefinition<?> definition = attribute.getAttributeDefinition();
         if (target != null && !definition.isSince(target.getMajor(), target.getMinor()))
            return;

         attribute.write(writer, attribute.name());
      }
   }

   public static Attribute<?>[] extractAttributes(AttributeSet attributes, AttributeDefinition<?> ... definitions) {
      List<Attribute<?>> collected = new ArrayList<>();
      for (AttributeDefinition<?> definition : definitions) {
         collected.add(attributes.attribute(definition));
      }
      return collected.toArray(new Attribute<?>[0]);
   }
}
