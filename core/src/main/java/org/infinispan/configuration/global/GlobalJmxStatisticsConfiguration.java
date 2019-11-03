package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.parsing.Element;

public class GlobalJmxStatisticsConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<String> JMX_DOMAIN = AttributeDefinition.builder("domain", "org.infinispan").immutable().build();
   public static final AttributeDefinition<MBeanServerLookup> MBEAN_SERVER_LOOKUP = AttributeDefinition.builder("mbeanServerLookup", null, MBeanServerLookup.class).copier(IdentityAttributeCopier.INSTANCE).immutable().build();
   public static final AttributeDefinition<Boolean> ALLOW_DUPLICATE_DOMAINS = AttributeDefinition.builder("duplicateDomains", true).immutable().build();
   public static final AttributeDefinition<TypedProperties> PROPERTIES = AttributeDefinition.builder("properties", null, TypedProperties.class).immutable().initializer(TypedProperties::new).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalJmxStatisticsConfiguration.class, JMX_DOMAIN, MBEAN_SERVER_LOOKUP, ALLOW_DUPLICATE_DOMAINS, PROPERTIES);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.JMX.getLocalName());

   private final Attribute<String> jmxDomain;
   private final Attribute<MBeanServerLookup> mBeanServerLookup;
   private final Attribute<Boolean> allowDuplicateDomains;
   private final Attribute<TypedProperties> properties;
   private final String cacheManagerName;
   private final boolean enabled;

   private final AttributeSet attributes;

   GlobalJmxStatisticsConfiguration(AttributeSet attributes, String cacheManagerName, boolean enabled) {
      this.attributes = attributes.checkProtection();
      this.jmxDomain = attributes.attribute(JMX_DOMAIN);
      this.mBeanServerLookup = attributes.attribute(MBEAN_SERVER_LOOKUP);
      this.allowDuplicateDomains = attributes.attribute(ALLOW_DUPLICATE_DOMAINS);
      this.properties = attributes.attribute(PROPERTIES);
      this.enabled = enabled;
      this.cacheManagerName = cacheManagerName;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   /**
    * @return true if JMX statistics are enabled.
    * @deprecated use {@link CacheContainerConfigurationBuilder#statistics(Boolean)}
    */
   @Deprecated
   public boolean enabled() {
      return enabled;
   }

   public String domain() {
      return jmxDomain.get();
   }

   public TypedProperties properties() {
      return properties.get();
   }

   /**
    * @deprecated Since 10.1, please set a unique {@link #jmxDomain} or {@link GlobalConfiguration#cacheManagerName()} instead.
    */
   @Deprecated
   public boolean allowDuplicateDomains() {
      return allowDuplicateDomains.get();
   }

   /**
    * @return the cache manager name
    * @deprecated use {@link GlobalConfiguration#cacheManagerName()} instead
    */
   @Deprecated
   public String cacheManagerName() {
      return cacheManagerName;
   }

   public MBeanServerLookup mbeanServerLookup() {
      return mBeanServerLookup.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalJmxStatisticsConfiguration that = (GlobalJmxStatisticsConfiguration) o;

      if (enabled != that.enabled) return false;
      if (cacheManagerName != null ? !cacheManagerName.equals(that.cacheManagerName) : that.cacheManagerName != null)
         return false;
      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + (cacheManagerName != null ? cacheManagerName.hashCode() : 0);
      result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "GlobalJmxStatisticsConfiguration{" +
            "enabled=" + enabled +
            ", cacheManagerName='" + cacheManagerName + '\'' +
            ", attributes=" + attributes +
            '}';
   }
}
