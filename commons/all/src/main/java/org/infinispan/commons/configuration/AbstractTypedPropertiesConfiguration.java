package org.infinispan.commons.configuration;

import static org.infinispan.commons.util.Immutables.immutableTypedProperties;

import java.util.Properties;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeInitializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.TypedPropertiesAttributeCopier;
import org.infinispan.commons.util.TypedProperties;

public abstract class AbstractTypedPropertiesConfiguration {
   public static final AttributeDefinition<TypedProperties> PROPERTIES = AttributeDefinition.builder("properties", null, TypedProperties.class).copier(TypedPropertiesAttributeCopier.INSTANCE).initializer(new AttributeInitializer<TypedProperties>() {
      @Override
      public TypedProperties initialize() {
         return new TypedProperties();
      }
   }).build();
   public static AttributeSet attributeSet() {
      return new AttributeSet(AbstractTypedPropertiesConfiguration.class, PROPERTIES);
   };

   protected AttributeSet attributes;
   private final Attribute<TypedProperties> properties;

   /**
    * @deprecated use {@link AbstractTypedPropertiesConfiguration#AbstractTypedPropertiesConfiguration(AttributeSet)} instead
    */
   @Deprecated
   protected AbstractTypedPropertiesConfiguration(Properties properties) {
      this.attributes = attributeSet();
      this.attributes = attributes.protect();
      this.properties = this.attributes.attribute(PROPERTIES);
      this.attributes.attribute(PROPERTIES).set(immutableTypedProperties(TypedProperties.toTypedProperties(properties)));
   }

   protected AbstractTypedPropertiesConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.properties = this.attributes.attribute(PROPERTIES);
      if (properties.isModified()) {
         properties.set(immutableTypedProperties(properties.get()));
      }
   }

   @Override
   public String toString() {
      return "AbstractTypedPropertiesConfiguration [attributes=" + attributes + "]";
   }

   public TypedProperties properties() {
      return properties.get();
   }

   @Override
   public final boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      AbstractTypedPropertiesConfiguration other = (AbstractTypedPropertiesConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public final int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

}
