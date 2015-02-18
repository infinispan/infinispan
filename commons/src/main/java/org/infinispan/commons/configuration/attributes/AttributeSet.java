package org.infinispan.commons.configuration.attributes;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * AttributeSet is a container for {@link Attribute}s. It is constructed by passing in a list of {@link AttributeDefinition}s.
 * AttributeSets are initially unprotected, which means that the contained attributes can be modified. If the {@link #protect()} method is invoked
 * then only attributes which are not {@link AttributeDefinition#isImmutable()} can be modified from then on.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class AttributeSet implements AttributeListener<Object> {
   private static final Log log = LogFactory.getLog(AttributeSet.class);
   private final String name;
   private final Map<String, Attribute<? extends Object>> attributes;
   private boolean protect;

   @SafeVarargs
   public AttributeSet(Class<?> klass, AttributeDefinition<?>... attributeDefinitions) {
      this(klass.getSimpleName(), attributeDefinitions);
   }

   @SafeVarargs
   public AttributeSet(String name, AttributeDefinition<?>... attributeDefinitions) {
      this.name = name;
      this.attributes = new HashMap<>(attributeDefinitions.length);
      for (AttributeDefinition<?> def : attributeDefinitions) {
         if (attributes.containsKey(def.name())) {
            throw log.attributeSetDuplicateAttribute(def.name(), name);
         }
         Attribute<Object> attribute = (Attribute<Object>) def.toAttribute();
         if (!attribute.isImmutable())
            attribute.addListener(this);
         this.attributes.put(def.name(), attribute);
      }
   }

   @SafeVarargs
   public AttributeSet(Class<?> klass, AttributeSet attributeSet, AttributeDefinition<?>... attributeDefinitions) {
      this(klass.getSimpleName(), attributeSet, attributeDefinitions);
   }

   public AttributeSet(String name, AttributeSet attributeSet, AttributeDefinition<?>[] attributeDefinitions) {
      this.name = name;
      this.attributes = new HashMap<>(attributeDefinitions.length + attributeSet.attributes.size());
      for (Attribute<? extends Object> attribute : attributeSet.attributes.values()) {
         this.attributes.put(attribute.name(), attribute.getAttributeDefinition().toAttribute());
      }
      for (AttributeDefinition<?> def : attributeDefinitions) {
         Attribute<Object> attribute = (Attribute<Object>) def.toAttribute();
         if (!attribute.isImmutable())
            attribute.addListener(this);
         this.attributes.put(def.name(), attribute);
      }
   }

   public String getName() {
      return name;
   }

   public boolean contains(String name) {
      return attributes.containsKey(name);
   }

   public <T> boolean contains(AttributeDefinition<T> def) {
      return contains(def.name());
   }

   @SuppressWarnings("unchecked")
   public <T> Attribute<T> attribute(String name) {
      return (Attribute<T>) this.attributes.get(name);
   }

   public <T> Attribute<T> attribute(AttributeDefinition<T> def) {
      Attribute<T> attribute = attribute(def.name());
      if (attribute != null)
         return attribute;
      else
         throw log.noSuchAttribute(def.name(), name);
   }

   public void read(AttributeSet other) {

      for (Iterator<Attribute<? extends Object>> iterator = attributes.values().iterator(); iterator.hasNext();) {
         Attribute<Object> attribute = (Attribute<Object>) iterator.next();

         Attribute<Object> a = other.attribute(attribute.name());
         if (a.isModified()) {
            attribute.read(a);
         }
      }
   }

   /**
    * Returns a new ValueSet where immutable {@link Attribute}s are write-protected
    *
    * @return
    */
   public AttributeSet protect() {
      AttributeDefinition<?> attrDefs[] = new AttributeDefinition[attributes.size()];
      int i = 0;
      for (Attribute<?> attribute : attributes.values()) {
         attrDefs[i++] = attribute.getAttributeDefinition();
      }
      AttributeSet protectedSet = new AttributeSet(name, attrDefs);
      for (Attribute<?> attribute : protectedSet.attributes.values()) {
         Attribute<?> localAttr = this.attributes.get(attribute.name());
         attribute.read((Attribute)localAttr);
         attribute.protect();
      }
      protectedSet.protect = true;
      return protectedSet;
   }

   public boolean isProtected() {
      return protect;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
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
      AttributeSet other = (AttributeSet) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public String toString() {
      return toString(null);
   }

   public String toString(String name) {
      StringBuilder sb = new StringBuilder();
      if (name != null) {
         sb.append(name);
         sb.append(" = ");
      }
      sb.append("[");
      boolean comma = false;
      for (Attribute<?> value : attributes.values()) {
         if (comma) {
            sb.append(", ");
         } else {
            comma = true;
         }
         sb.append(value.toString());
      }
      sb.append("]");
      return sb.toString();
   }

   public AttributeSet checkProtection() {
      if (!protect) {
         throw log.unprotectedAttributeSet(name);
      }
      return this;
   }

   public void reset() {
      if (protect) {
         throw log.protectedAttributeSet(name);
      }
      for (Iterator<Attribute<? extends Object>> iterator = attributes.values().iterator(); iterator.hasNext();) {
         iterator.next().reset();
      }
   }

   @Override
   public void attributeChanged(Attribute<Object> attribute, Object oldValue) {
      // TODO
   }

}
