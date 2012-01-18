package org.infinispan.configuration.cache;

/**
 * Determines whether statistics are gather and reported.
 * 
 * @author pmuir
 * 
 */
public class JMXStatisticsConfigurationBuilder extends AbstractConfigurationChildBuilder<JMXStatisticsConfiguration> {

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
   void validate() {
      // TODO Auto-generated method stub

   }

   @Override
   JMXStatisticsConfiguration create() {
      return new JMXStatisticsConfiguration(enabled);
   }

   @Override
   public JMXStatisticsConfigurationBuilder read(JMXStatisticsConfiguration template) {
      this.enabled = template.enabled();

      return this;
   }

}
