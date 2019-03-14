package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.configuration.StatisticsConfiguration.ENABLED;
import static org.infinispan.client.hotrod.configuration.StatisticsConfiguration.JMX_DOMAIN;
import static org.infinispan.client.hotrod.configuration.StatisticsConfiguration.JMX_ENABLED;
import static org.infinispan.client.hotrod.configuration.StatisticsConfiguration.JMX_NAME;
import static org.infinispan.client.hotrod.configuration.StatisticsConfiguration.MBEAN_SERVER_LOOKUP;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.JMX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.STATISTICS;

import java.util.Properties;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configures client-side statistics
 *
 * @author Tristan Tarrant
 * @since 9.4
 */
public class StatisticsConfigurationBuilder extends AbstractConfigurationChildBuilder implements
      Builder<StatisticsConfiguration> {

   AttributeSet attributes = StatisticsConfiguration.attributeDefinitionSet();

   StatisticsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enables or disables client-side statistics collection
    *
    * @param enabled whether to enable client-side statistics
    */
   public StatisticsConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Enables client-side statistics collection
    */
   public StatisticsConfigurationBuilder enable() {
      return enabled(true);
   }

   /**
    * Disables client-side statistics collection
    */
   public StatisticsConfigurationBuilder disable() {
      return enabled(false);
   }

   /**
    * Enables or disables exposure of client-side statistics over JMX
    */
   public StatisticsConfigurationBuilder jmxEnabled(boolean enabled) {
      attributes.attribute(JMX_ENABLED).set(enabled);
      return this;
   }

   /**
    * Enables exposure of client-side statistics over JMX
    */
   public StatisticsConfigurationBuilder jmxEnable() {
      return jmxEnabled(true);
   }

   /**
    * Disables exposure of client-side statistics over JMX
    */
   public StatisticsConfigurationBuilder jmxDisable() {
      return jmxEnabled(false);
   }

   /**
    * Sets the JMX domain name with which MBeans are exposed. Defaults to "org.infinispan" ({@link StatisticsConfiguration#JMX_DOMAIN})
    * @param jmxDomain the JMX domain name
    */
   public StatisticsConfigurationBuilder jmxDomain(String jmxDomain) {
      attributes.attribute(JMX_DOMAIN).set(jmxDomain);
      return this;
   }

   /**
    * Sets the name of the MBean. Defaults to "Default" ({@link StatisticsConfiguration#JMX_NAME})
    * @param jmxName
    */
   public StatisticsConfigurationBuilder jmxName(String jmxName) {
      attributes.attribute(JMX_NAME).set(jmxName);
      return this;
   }

   /**
    * Sets the instance of the {@link org.infinispan.commons.jmx.MBeanServerLookup} class to be used to bound JMX MBeans
    * to.
    *
    * @param mBeanServerLookupInstance An instance of {@link org.infinispan.commons.jmx.MBeanServerLookup}
    */
   public StatisticsConfigurationBuilder mBeanServerLookup(MBeanServerLookup mBeanServerLookupInstance) {
      attributes.attribute(MBEAN_SERVER_LOOKUP).set(mBeanServerLookupInstance);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public StatisticsConfiguration create() {
      return new StatisticsConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(StatisticsConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public ConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);
      enabled(typed.getBooleanProperty(STATISTICS, ENABLED.getDefaultValue()));
      jmxEnabled(typed.getBooleanProperty(JMX, JMX_ENABLED.getDefaultValue()));
      jmxDomain(typed.getProperty(ConfigurationProperties.JMX_DOMAIN, JMX_DOMAIN.getDefaultValue()));
      jmxName(typed.getProperty(ConfigurationProperties.JMX_NAME, JMX_NAME.getDefaultValue()));
      return builder;
   }
}
