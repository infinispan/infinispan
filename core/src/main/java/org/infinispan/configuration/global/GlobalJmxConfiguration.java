package org.infinispan.configuration.global;

import static org.infinispan.commons.configuration.attributes.IdentityAttributeCopier.identityCopier;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.attributes.PropertiesAttributeSerializer;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.parsing.Element;

/**
 * @since 10.1.3
 */
@BuiltBy(GlobalJmxConfigurationBuilder.class)
public class GlobalJmxConfiguration extends ConfigurationElement<GlobalJmxConfiguration> {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).immutable().build();
   public static final AttributeDefinition<String> DOMAIN = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.DOMAIN, "org.infinispan").immutable().build();
   public static final AttributeDefinition<MBeanServerLookup> MBEAN_SERVER_LOOKUP = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MBEAN_SERVER_LOOKUP, null, MBeanServerLookup.class)
         .copier(identityCopier()).serializer(AttributeSerializer.INSTANCE_CLASS_NAME).immutable().build();
   public static final AttributeDefinition<TypedProperties> PROPERTIES = AttributeDefinition.builder(Element.PROPERTIES, null, TypedProperties.class).immutable()
         .initializer(TypedProperties::new).serializer(PropertiesAttributeSerializer.PROPERTIES).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalJmxConfiguration.class, ENABLED, DOMAIN, MBEAN_SERVER_LOOKUP, PROPERTIES);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<String> domain;
   private final Attribute<TypedProperties> properties;
   private final String cacheManagerName;

   GlobalJmxConfiguration(AttributeSet attributes, String cacheManagerName) {
      super(Element.JMX, attributes);
      this.enabled = attributes.attribute(ENABLED);
      this.domain = attributes.attribute(DOMAIN);
      this.properties = attributes.attribute(PROPERTIES);
      this.cacheManagerName = cacheManagerName;
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

   public MBeanServerLookup mbeanServerLookup() {
      return attributes.attribute(MBEAN_SERVER_LOOKUP).get();
   }

   @Override
   public String toString() {
      return "GlobalJmxConfiguration [" +
            "cacheManagerName='" + cacheManagerName + '\'' +
            ", attributes=" + attributes +
            ']';
   }
}
