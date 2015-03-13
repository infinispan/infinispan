package org.infinispan.commons.configuration.attributes;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;

public final class Attribute<T> implements Cloneable {
   private static final GenericJBossMarshaller ATTRIBUTE_MARSHALLER = new GenericJBossMarshaller();
   private final AttributeDefinition<T> definition;
   private T value;
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
      Attribute<T> oldValue = this.clone();
      this.value = value;
      this.modified = true;
      this.fireValueChanged(oldValue);
   }

   public boolean isImmutable() {
      return definition.isImmutable();
   }

   public boolean isModified() {
      return modified;
   }

   public boolean asBoolean() {
      return (Boolean) value;
   }

   public double asDouble() {
      return (Double) value;
   }

   public float asFloat() {
      return (Float) value;
   }

   public int asInteger() {
      return (Integer) value;
   }

   public long asLong() {
      return (Long) value;
   }

   public <K> K asObject(Class<K> klass) {
      return (K) value;
   }

   public String asString() {
      return (String) value;
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

   private void fireValueChanged(Attribute<T> oldValue) {
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
      Attribute<T> clone = other.clone();
      this.value = clone.value;
      this.modified = other.modified;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((definition == null) ? 0 : definition.hashCode());
      result = prime * result + (modified ? 1231 : 1237);
      result = prime * result + ((value == null) ? 0 : value.hashCode());
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
         if (other.value != null)
            return false;
      } else if (!value.equals(other.value))
         return false;
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
}
