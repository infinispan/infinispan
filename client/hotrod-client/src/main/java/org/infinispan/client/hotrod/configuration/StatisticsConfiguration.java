package org.infinispan.client.hotrod.configuration;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.PlatformMBeanServerLookup;
import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant
 * @since 9.4
 */
public class StatisticsConfiguration {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<Boolean> JMX_ENABLED = AttributeDefinition.builder("jmx_enabled", false).immutable().build();
   public static final AttributeDefinition<String> JMX_DOMAIN = AttributeDefinition.builder("jmx_domain", "org.infinispan").immutable().build();
   public static final AttributeDefinition<MBeanServerLookup> MBEAN_SERVER_LOOKUP = AttributeDefinition.builder("mbeanserverlookup", (MBeanServerLookup) Util.getInstance(PlatformMBeanServerLookup.class)).copier(IdentityAttributeCopier.INSTANCE).immutable().build();
   public static final AttributeDefinition<String> JMX_NAME = AttributeDefinition.builder("jmx_name", "Default").immutable().build();
   public static final AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StatisticsConfiguration.class, ENABLED, JMX_ENABLED, JMX_DOMAIN, MBEAN_SERVER_LOOKUP, JMX_NAME);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Boolean> jmxEnabled;
   private final Attribute<String> jmxDomain;
   private final Attribute<String> jmxName;
   private final Attribute<MBeanServerLookup> mBeanServerLookup;

   private final AttributeSet attributes;

   StatisticsConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.jmxEnabled = attributes.attribute(JMX_ENABLED);
      this.jmxDomain = attributes.attribute(JMX_DOMAIN);
      this.jmxName = attributes.attribute(JMX_NAME);
      this.mBeanServerLookup = attributes.attribute(MBEAN_SERVER_LOOKUP);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public boolean enabled() {
      return enabled.get();
   }

   public boolean jmxEnabled() {
      return jmxEnabled.get();
   }

   public String jmxDomain() {
      return jmxDomain.get();
   }

   public MBeanServerLookup mbeanServerLookup() {
      return mBeanServerLookup.get();
   }

   public String jmxName() {
      return jmxName.get();
   }

   @Override
   public String toString() {
      return attributes.toString(StatisticsConfiguration.class.getSimpleName());
   }
}
