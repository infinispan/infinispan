package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.StatisticsConfiguration.ENABLED;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Determines whether cache statistics are gathered.
 *
 * @since 10.1.3
 */
public class StatisticsConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<StatisticsConfiguration> {

   private final AttributeSet attributes;

   StatisticsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.attributes = StatisticsConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
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

   @Override
   public StatisticsConfiguration create() {
      return new StatisticsConfiguration(attributes.protect());
   }

   @Override
   public StatisticsConfigurationBuilder read(StatisticsConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "StatisticsConfigurationBuilder [attributes=" + attributes + "]";
   }
}
