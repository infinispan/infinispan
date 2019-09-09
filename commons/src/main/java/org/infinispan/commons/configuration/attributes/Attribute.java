package org.infinispan.commons.configuration.attributes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;

/**
 * Attribute. This class implements a configuration attribute value holder. A configuration attribute is defined by an {@link AttributeDefinition}.
 * An attribute contains an optional value (or defers to its AttributeDefinition for the default value). It is possible to determine whether
 * a value has been changed with respect to its initial value. An Attribute remains modifiable until it is protected. At which point it can only
 * be modified if its AttributeDefinition allows it to be mutable. Additionally it is possible to register listeners when values change so that
 * code can react to these changes.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public final class Attribute<T> implements Cloneable, Matchable<Attribute<?>> {
   private final AttributeDefinition<T> definition;
   protected T value;
   private boolean protect;
   private boolean modified;
   private List<AttributeListener<T>> listeners;

   Attribute(AttributeDefinition<T> definition) {
      this.definition = definition;
      this.value = definition.getDefaultValue();
   }

   public String name() {
      return definition.name();
   }

   public T get() {
      return value;
   }

   public T getInitialValue() {
      return definition.getDefaultValue();
   }

   public void validate() {
      definition.validate(value);
   }

   public Attribute<T> protect() {
      if (!protect && definition.isImmutable()) {
         this.protect = true;
      }
      return this;
   }

   public void set(T value) {
      if (protect) {
         throw new IllegalStateException();
      }
      T oldValue = this.value;
      this.value = value;
      this.modified = true;
      this.fireValueChanged(oldValue);
   }

   public T computeIfAbsent(Supplier<T> supplier) {
      if (value == null)
         set(supplier.get());
      return value;
   }

   public boolean isImmutable() {
      return definition.isImmutable();
   }

   public boolean isPersistent() {
      return definition.isAutoPersist();
   }

   public boolean isModified() {
      return modified;
   }

   public <K> K asObject() {
      return (K) value;
   }

   public void addListener(AttributeListener<T> listener) {
      if (isImmutable()) {
         throw new UnsupportedOperationException();
      }
      if (listeners == null) {
         listeners = new ArrayList<>();
      }
      listeners.add(listener);
   }

   public void removeListener(AttributeListener<T> listener) {
      if (listeners != null) {
         listeners.remove(listener);
      }
   }

   T getValue() {
      return value;
   }

   void setValue(T value) {
      this.value = value;
   }

   public boolean isNull() {
      return value == null;
   }

   boolean isProtect() {
      return protect;
   }

   void setProtect(boolean protect) {
      this.protect = protect;
   }

   public AttributeDefinition<T> getAttributeDefinition() {
      return definition;
   }

   void setModified(boolean modified) {
      this.modified = modified;
   }

   private void fireValueChanged(T oldValue) {
      if (listeners != null) {
         for (AttributeListener<T> listener : listeners) {
            listener.attributeChanged(this, oldValue);
         }
      }
   }

   @SuppressWarnings("unchecked")
   @Override
   protected Attribute<T> clone() {
      Attribute<T> clone;
      try {
         clone = (Attribute<T>) super.clone();
         clone.modified = this.modified;
         clone.listeners = null;
         clone.protect = false;
         return clone;
      } catch (CloneNotSupportedException e) {
         throw new CacheException(e);
      }

   }

   public void read(Attribute<T> other) {
      AttributeCopier<T> copier = definition.copier();
      if (copier == null) {
         Attribute<T> clone = other.clone();
         this.value = clone.value;
      } else {
         this.value = copier.copyAttribute(other.value);
      }
      this.modified = other.modified;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((definition == null) ? 0 : definition.hashCode());
      result = prime * result + (modified ? 1231 : 1237);
      result = prime * result + ((value == null) ? 0 : value.getClass().isArray() ? Arrays.deepHashCode((Object[]) value) : value.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      Attribute other = (Attribute) obj;
      if (definition == null) {
         if (other.definition != null)
            return false;
      } else if (!definition.equals(other.definition))
         return false;
      if (value == null) {
         return other.value == null;
      } else return Objects.deepEquals(value, other.value);
   }

   /**
    * Compares this attribute to another attribute taking into account the {@link AttributeDefinition#isGlobal()} flag.
    * If the attribute is global, then this method will return true only if the values are identical.
    * If the attribute is local, then this method will return true even if the values don't match.
    * Essentially, this method only ensures that the attribute definitions are equals.
    * @param other
    * @return
    */
   public boolean matches(Attribute<?> other) {
      if (other == null)
         return false;
      if (!this.definition.equals(other.definition))
         return false;
      if (this.definition.isGlobal()) {
         if (Matchable.class.isAssignableFrom(this.definition.getType())) {
            return ((Matchable) value).matches(other.value);
         } else {
            return Objects.equals(value, other.value);
         }
      }
      return true;
   }

   @Override
   public String toString() {
      return definition.name() + "=" + value;
   }

   public void reset() {
      if (protect) {
         throw new IllegalStateException("Cannot reset a protected Attribute");
      }
      value = definition.getDefaultValue();
      modified = false;
   }

   void write(XMLStreamWriter writer, String name) throws XMLStreamException {
      if (modified && value != null) {
         Class<?> klass = value.getClass();
         if (klass == Class.class) {
            writer.writeAttribute(name, ((Class) value).getName());
         } else if (klass.isEnum()) {
            writer.writeAttribute(name, value.toString());
         } else if (Util.isBasicType(klass)) {
            writer.writeAttribute(name, value.toString());
         } else {
            writer.writeAttribute(name, klass.getName());
         }
      }
   }

}
