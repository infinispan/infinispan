package org.infinispan.commons.configuration.attributes;

import static org.infinispan.commons.logging.Log.CONFIG;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * AttributeSet is a container for {@link Attribute}s. It is constructed by passing in a list of {@link AttributeDefinition}s.
 * AttributeSets are initially unprotected, which means that the contained attributes can be modified. If the {@link #protect()} method is invoked
 * then only attributes which are not {@link AttributeDefinition#isImmutable()} can be modified from then on.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class AttributeSet implements AttributeListener<Object>, Matchable<AttributeSet> {
   private final Class<?> klass;
   private final String name;
   private final Map<String, Attribute<?>> attributes;
   private boolean protect;

   public AttributeSet(Class<?> klass, AttributeDefinition<?>... attributeDefinitions) {
      this(klass, klass.getSimpleName(), null, attributeDefinitions);
   }

   public AttributeSet(String name, AttributeDefinition<?>... attributeDefinitions) {
      this(name, null, attributeDefinitions);
   }

   public AttributeSet(Class<?> klass, AttributeSet attributeSet, AttributeDefinition<?>... attributeDefinitions) {
      this(klass, klass.getSimpleName(), attributeSet, attributeDefinitions);
   }

   public AttributeSet(String name, AttributeSet attributeSet, AttributeDefinition<?>[] attributeDefinitions) {
      this(null, name, attributeSet, attributeDefinitions);
   }

   private AttributeSet(Class<?> klass, String name, AttributeSet attributeSet, AttributeDefinition<?>[] attributeDefinitions) {
      this.klass = klass;
      this.name = name;
      if (attributeSet != null) {
         this.attributes = new LinkedHashMap<>(attributeDefinitions.length + attributeSet.attributes.size());
         for (Attribute<?> attribute : attributeSet.attributes.values()) {
            this.attributes.put(attribute.name(), attribute.getAttributeDefinition().toAttribute());
         }
      } else {
         this.attributes = new LinkedHashMap<>(attributeDefinitions.length);
      }
      for (AttributeDefinition<?> def : attributeDefinitions) {
         if (attributes.containsKey(def.name())) {
            throw CONFIG.attributeSetDuplicateAttribute(def.name(), name);
         }
         Attribute<Object> attribute = (Attribute<Object>) def.toAttribute();
         if (!attribute.isImmutable())
            attribute.addListener(this);
         this.attributes.put(def.name(), attribute);
      }
   }

   public Class<?> getKlass() {
      return klass;
   }

   public String getName() {
      return name;
   }

   /**
    * Returns whether this attribute set contains the specified named attribute
    * @param name the name of the attribute
    */
   public boolean contains(String name) {
      return attributes.containsKey(name);
   }

   /**
    * Returns whether this set contains the specified attribute definition
    *
    * @param def the {@link AttributeDefinition}
    */
   public <T> boolean contains(AttributeDefinition<T> def) {
      return contains(def.name());
   }

   /**
    * Returns the named attribute
    * @param name the name of the attribute to return
    * @return the attribute
    */
   @SuppressWarnings("unchecked")
   public <T> Attribute<T> attribute(String name) {
      return (Attribute<T>) this.attributes.get(name);
   }

   /**
    * Returns the attribute identified by the supplied {@link AttributeDefinition}
    * @param def the attribute definition
    * @return the attribute
    */
   public <T> Attribute<T> attribute(AttributeDefinition<T> def) {
      Attribute<T> attribute = attribute(def.name());
      if (attribute != null)
         return attribute;
      else
         throw CONFIG.noSuchAttribute(def.name(), name);
   }

   /**
    * Copies all attribute from another AttributeSet
    *
    * @param other the source AttributeSet
    */
   public void read(AttributeSet other) {

      for (Iterator<Attribute<?>> iterator = attributes.values().iterator(); iterator.hasNext();) {
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
      AttributeSet protectedSet = new AttributeSet(klass, name, null, attrDefs);
      for (Attribute<?> attribute : protectedSet.attributes.values()) {
         Attribute<?> localAttr = this.attributes.get(attribute.name());
         attribute.read((Attribute)localAttr);
         attribute.protect();
      }
      protectedSet.protect = true;
      return protectedSet;
   }

   /**
    * Returns whether any attributes in this set have been modified
    */
   public boolean isModified() {
      for(Attribute<?> attribute : attributes.values()) {
         if (attribute.isModified())
            return true;
      }
      return false;
   }

   /**
    * Returns whether this attribute set is protected
    */
   public boolean isProtected() {
      return protect;
   }

   /**
    * Writer a single attribute to the specified {@link XMLStreamWriter} using the attribute's xmlName
    * @param writer the writer
    * @param def the Attribute definition
    * @throws XMLStreamException
    */
   public void write(XMLStreamWriter writer, AttributeDefinition<?> def) throws XMLStreamException {
      write(writer, def, def.xmlName());
   }

   /**
    * Writer a single attribute to the specified {@link XMLStreamWriter} using the supplied name
    * @param writer the writer
    * @param def the Attribute definition
    * @param name the XML tag name for the attribute
    * @throws XMLStreamException
    */
   public void write(XMLStreamWriter writer, AttributeDefinition<?> def, Enum<?> name) throws XMLStreamException {
      write(writer, def, name.toString());
   }

   /**
    * Writer a single attribute to the specified {@link XMLStreamWriter} using the supplied name
    * @param writer the writer
    * @param def the Attribute definition
    * @param name the XML tag name for the attribute
    * @throws XMLStreamException
    */
   public void write(XMLStreamWriter writer, AttributeDefinition<?> def, String name) throws XMLStreamException {
      Attribute<?> attribute = attribute(def);
      attribute.write(writer, name);
   }


   /**
    * Writes this attributeset to the specified XMLStreamWriter as an element
    * @param writer
    */
   public void write(XMLStreamWriter writer, String xmlElementName) throws XMLStreamException {
      if (isModified()) {
         writer.writeStartElement(xmlElementName);
         write(writer);
         writer.writeEndElement();
      }
   }

   /**
    * Writes the specified attributes in this attributeset to the specified XMLStreamWriter as an element
    * @param writer
    */
   public void write(XMLStreamWriter writer, String xmlElementName, AttributeDefinition<?>... defs) throws XMLStreamException {
      boolean skip = true;
      for (AttributeDefinition def : defs) {
         skip = skip && !attribute(def).isModified();
      }
      if (!skip) {
         writer.writeStartElement(xmlElementName);
         for (AttributeDefinition def : defs) {
            Attribute attr = attribute(def);
            attr.write(writer, attr.getAttributeDefinition().xmlName());
         }
         writer.writeEndElement();
      }
   }

   /**
    * Writes the attributes of this attributeset as part of the current element
    * @param writer
    */
   public void write(XMLStreamWriter writer) throws XMLStreamException {
      for (Attribute<?> attr : attributes.values()) {
         if (attr.isPersistent())
            attr.write(writer, attr.getAttributeDefinition().xmlName());
      }
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

   public boolean matches(AttributeSet other) {
      if (other.attributes.size() != attributes.size())
         return false;
      Iterator<Map.Entry<String, Attribute<?>>> i = attributes.entrySet().iterator();
      while (i.hasNext()) {
         Map.Entry<String, Attribute<?>> e = i.next();
         String key = e.getKey();
         Attribute<?> value = e.getValue();
         if (value == null) {
            if (!(other.attributes.containsKey(key) && other.attributes.get(key)==null))
               return false;
         } else {
            if (!value.matches(other.attributes.get(key)))
               return false;
         }
      }
      return true;
   }

   @Override
   public String toString() {
      return toString(name);
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
         throw CONFIG.unprotectedAttributeSet(name);
      }
      return this;
   }

   public void reset() {
      if (protect) {
         throw CONFIG.protectedAttributeSet(name);
      }
      for (Attribute<?> attribute : attributes.values()) {
         attribute.reset();
      }
   }

   @Override
   public void attributeChanged(Attribute<Object> attribute, Object oldValue) {
      // TODO
   }

   public Collection<Attribute<?>> attributes() {
      return attributes.values();
   }

   public boolean isEmpty() {
      return attributes.entrySet().stream().allMatch(attrs -> {
         Attribute<?> attr = attrs.getValue();
         return attr.isNull() || !attr.isModified();
      });
   }
}
