package org.infinispan.hotrod.configuration;

import static org.infinispan.commons.configuration.attributes.IdentityAttributeCopier.identityCopier;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.PlatformMBeanServerLookup;
import org.infinispan.commons.util.Util;

/**
 * @since 14.0
 */
public class StatisticsConfiguration extends ConfigurationElement<StatisticsConfiguration> {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<Boolean> JMX_ENABLED = AttributeDefinition.builder("jmx_enabled", false).immutable().build();
   public static final AttributeDefinition<String> JMX_DOMAIN = AttributeDefinition.builder("jmx_domain", "org.infinispan").immutable().build();
   public static final AttributeDefinition<MBeanServerLookup> MBEAN_SERVER_LOOKUP = AttributeDefinition.builder("mbeanserverlookup", (MBeanServerLookup) Util.getInstance(PlatformMBeanServerLookup.class))
         .copier(identityCopier()).immutable().build();
   public static final AttributeDefinition<String> JMX_NAME = AttributeDefinition.builder("jmx_name", "Default").immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StatisticsConfiguration.class, ENABLED, JMX_ENABLED, JMX_DOMAIN, MBEAN_SERVER_LOOKUP, JMX_NAME);
   }

   StatisticsConfiguration(AttributeSet attributes) {
      super("statistics", attributes);
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public boolean jmxEnabled() {
      return attributes.attribute(JMX_ENABLED).get();
   }

   public String jmxDomain() {
      return attributes.attribute(JMX_DOMAIN).get();
   }

   public MBeanServerLookup mbeanServerLookup() {
      return attributes.attribute(MBEAN_SERVER_LOOKUP).get();
   }

   public String jmxName() {
      return attributes.attribute(JMX_NAME).get();
   }
}
