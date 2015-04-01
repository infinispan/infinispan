package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeInitializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.jmx.PlatformMBeanServerLookup;

public class GlobalJmxStatisticsConfiguration {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<String> JMX_DOMAIN = AttributeDefinition.builder("jmxDomain", "org.infinispan").immutable().build();
   public static final AttributeDefinition<MBeanServerLookup> MBEAN_SERVER_LOOKUP = AttributeDefinition.builder("mBeanServerLookup", (MBeanServerLookup) Util.getInstance(PlatformMBeanServerLookup.class)).copier(IdentityAttributeCopier.INSTANCE).immutable().build();
   public static final AttributeDefinition<Boolean> ALLOW_DUPLICATE_DOMAINS = AttributeDefinition.builder("allowDuplicateDomains", false).immutable().build();
   public static final AttributeDefinition<String> CACHE_MANAGER_NAME = AttributeDefinition.builder("cacheManagerName", "DefaultCacheManager").immutable().build();
   public static final AttributeDefinition<TypedProperties> PROPERTIES = AttributeDefinition.builder("properties", null, TypedProperties.class).immutable().initializer(new AttributeInitializer<TypedProperties>() {
      @Override
      public TypedProperties initialize() {
         return new TypedProperties();
      }
   }).build();

   public static final AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalJmxStatisticsConfiguration.class, ENABLED, JMX_DOMAIN, MBEAN_SERVER_LOOKUP, ALLOW_DUPLICATE_DOMAINS, CACHE_MANAGER_NAME, PROPERTIES);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<String> jmxDomain;
   private final Attribute<MBeanServerLookup> mBeanServerLookup;
   private final Attribute<Boolean> allowDuplicateDomains;
   private final Attribute<String> cacheManagerName;
   private final Attribute<TypedProperties> properties;

   private final AttributeSet attributes;

   GlobalJmxStatisticsConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.jmxDomain = attributes.attribute(JMX_DOMAIN);
      this.mBeanServerLookup = attributes.attribute(MBEAN_SERVER_LOOKUP);
      this.allowDuplicateDomains = attributes.attribute(ALLOW_DUPLICATE_DOMAINS);
      this.cacheManagerName = attributes.attribute(CACHE_MANAGER_NAME);
      this.properties = attributes.attribute(PROPERTIES);
   }

   public boolean enabled() {
      return enabled.get();
   }

   public String domain() {
      return jmxDomain.get();
   }

   public TypedProperties properties() {
      return properties.get();
   }

   public boolean allowDuplicateDomains() {
      return allowDuplicateDomains.get();
   }

   public String cacheManagerName() {
      return cacheManagerName.get();
   }

   public MBeanServerLookup mbeanServerLookup() {
      return mBeanServerLookup.get();
   }

   public AttributeSet attributes() {
      return attributes;
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
      GlobalJmxStatisticsConfiguration other = (GlobalJmxStatisticsConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public String toString() {
      return "GlobalJmxStatisticsConfiguration [attributes=" + attributes + "]";
   }


}