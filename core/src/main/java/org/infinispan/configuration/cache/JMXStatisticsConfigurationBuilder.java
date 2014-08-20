package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Determines whether statistics are gather and reported.
 *
 * @author pmuir
 *
 */
public class JMXStatisticsConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<JMXStatisticsConfiguration> {

   private boolean enabled = false;

   JMXStatisticsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enable statistics gathering and reporting
    */
   public JMXStatisticsConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   /**
    * Disable statistics gathering and reporting
    */
   public JMXStatisticsConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   /**
    * Enable or disable statistics gathering and reporting
    */
   public JMXStatisticsConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
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
      return new JMXStatisticsConfiguration(enabled);
   }

   @Override
   public JMXStatisticsConfigurationBuilder read(JMXStatisticsConfiguration template) {
      this.enabled = template.enabled();

      return this;
   }

   @Override
   public String toString() {
      return "JMXStatisticsConfigurationBuilder{" +
            "enabled=" + enabled +
            '}';
   }
}
