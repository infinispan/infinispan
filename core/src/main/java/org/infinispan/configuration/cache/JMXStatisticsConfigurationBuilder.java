package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.JMXStatisticsConfiguration.ENABLED;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
/**
 * Determines whether statistics are gather and reported.
 *
 * @author pmuir
 *
 */
public class JMXStatisticsConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<JMXStatisticsConfiguration> {

   private final AttributeSet attributes;

   JMXStatisticsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.attributes = JMXStatisticsConfiguration.attributeDefinitionSet();
   }

   /**
    * Enable statistics gathering and reporting
    */
   public JMXStatisticsConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Disable statistics gathering and reporting
    */
   public JMXStatisticsConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   /**
    * Enable or disable statistics gathering and reporting
    */
   public JMXStatisticsConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public JMXStatisticsConfiguration create() {
      return new JMXStatisticsConfiguration(attributes.protect());
   }

   @Override
   public JMXStatisticsConfigurationBuilder read(JMXStatisticsConfiguration template) {
      this.attributes.read(template.attributes());

      return this;
   }

   @Override
   public String toString() {
      return "JMXStatisticsConfigurationBuilder [attributes=" + attributes + "]";
   }
}
