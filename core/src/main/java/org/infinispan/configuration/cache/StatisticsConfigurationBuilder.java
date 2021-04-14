package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.StatisticsConfiguration.AVAILABLE;
import static org.infinispan.configuration.cache.StatisticsConfiguration.ENABLED;
import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Determines whether cache statistics are gathered.
 *
 * @since 10.1.3
 */
public class StatisticsConfigurationBuilder extends JMXStatisticsConfigurationBuilder implements Builder<StatisticsConfiguration> {

   private final AttributeSet attributes;

   StatisticsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.attributes = StatisticsConfiguration.attributeDefinitionSet();
   }

   /**
    * Enable statistics gathering.
    */
   public StatisticsConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Disable statistics gathering.
    */
   public StatisticsConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   /**
    * Enable or disable statistics gathering.
    */
   public StatisticsConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * If set to false, statistics gathering cannot be enabled during runtime. Performance optimization.
    * @deprecated since 10.1.3. This method will be removed in a future version.
    */
   @Deprecated
   public StatisticsConfigurationBuilder available(boolean available) {
      attributes.attribute(AVAILABLE).set(available);
      return this;
   }

   @Override
   public void validate() {
      Attribute<Boolean> enabled = attributes.attribute(ENABLED);
      Attribute<Boolean> available = attributes.attribute(AVAILABLE);
      if (enabled.isModified() && available.isModified()) {
         if (enabled.get() && !available.get()) {
            throw CONFIG.statisticsEnabledNotAvailable();
         }
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public StatisticsConfiguration create() {
      return new StatisticsConfiguration(attributes.protect());
   }

   @Override
   public StatisticsConfigurationBuilder read(StatisticsConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "StatisticsConfigurationBuilder [attributes=" + attributes + "]";
   }
}
