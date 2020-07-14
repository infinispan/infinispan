package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ClassAttributeSerializer;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.parsing.Element;

/**
 * @since 10.1.3
 */
public class GlobalJmxConfiguration extends GlobalJmxStatisticsConfiguration {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<String> DOMAIN = AttributeDefinition.builder("domain", "org.infinispan").immutable().build();
   public static final AttributeDefinition<MBeanServerLookup> MBEAN_SERVER_LOOKUP = AttributeDefinition.builder("mbeanServerLookup", null, MBeanServerLookup.class).copier(IdentityAttributeCopier.INSTANCE).serializer(ClassAttributeSerializer.INSTANCE).immutable().build();
   public static final AttributeDefinition<TypedProperties> PROPERTIES = AttributeDefinition.builder("properties", null, TypedProperties.class).immutable().initializer(TypedProperties::new).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalJmxConfiguration.class, ENABLED, DOMAIN, MBEAN_SERVER_LOOKUP, PROPERTIES);
   }

   private static final ElementDefinition<GlobalJmxConfiguration> ELEMENT_DEFINITION = new DefaultElementDefinition<>(Element.JMX.getLocalName());

   private final Attribute<Boolean> enabled;
   private final Attribute<String> domain;
   private final Attribute<MBeanServerLookup> mBeanServerLookup;
   private final Attribute<TypedProperties> properties;
   private final String cacheManagerName;

   private final AttributeSet attributes;

   GlobalJmxConfiguration(AttributeSet attributes, String cacheManagerName) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.domain = attributes.attribute(DOMAIN);
      this.mBeanServerLookup = attributes.attribute(MBEAN_SERVER_LOOKUP);
      this.properties = attributes.attribute(PROPERTIES);
      this.cacheManagerName = cacheManagerName;
   }

   @Override
   public ElementDefinition<GlobalJmxConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   /**
    * @return true if JMX is enabled.
    */
   public boolean enabled() {
      return enabled.get();
   }

   public String domain() {
      return domain.get();
   }

   public TypedProperties properties() {
      return properties.get();
   }

   /**
    * @return the cache manager name
    * @deprecated Since 10.1. please use {@link GlobalConfiguration#cacheManagerName()} instead.
    */
   @Deprecated
   public String cacheManagerName() {
      return cacheManagerName;
   }

   public MBeanServerLookup mbeanServerLookup() {
      return mBeanServerLookup.get();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalJmxConfiguration that = (GlobalJmxConfiguration) o;
      if (cacheManagerName != null ? !cacheManagerName.equals(that.cacheManagerName) : that.cacheManagerName != null)
         return false;
      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      int result = cacheManagerName != null ? cacheManagerName.hashCode() : 0;
      result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "GlobalJmxConfiguration [" +
            "cacheManagerName='" + cacheManagerName + '\'' +
            ", attributes=" + attributes +
            ']';
   }
}
