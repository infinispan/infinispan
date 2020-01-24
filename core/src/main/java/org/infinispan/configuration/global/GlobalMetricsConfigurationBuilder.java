package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalMetricsConfiguration.GAUGES;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.HISTOGRAMS;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.PREFIX;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures the types of metrics gathered and exported via microprofile metrics for all caches under this cache
 * manager. Gauges do not have any performance penalty so are enabled by default. Histograms are harder to compute so
 * should be enabled manually.
 */
public class GlobalMetricsConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<GlobalMetricsConfiguration> {

   private final AttributeSet attributes;

   GlobalMetricsConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = GlobalMetricsConfiguration.attributeDefinitionSet();
   }

   public GlobalMetricsConfigurationBuilder gauges(boolean gauges) {
      attributes.attribute(GAUGES).set(gauges);
      return this;
   }

   public GlobalMetricsConfigurationBuilder histograms(boolean histograms) {
      attributes.attribute(HISTOGRAMS).set(histograms);
      return this;
   }

   public GlobalMetricsConfigurationBuilder prefix(String prefix) {
      attributes.attribute(PREFIX).set(prefix);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public GlobalMetricsConfiguration create() {
      return new GlobalMetricsConfiguration(attributes.protect());
   }

   @Override
   public GlobalMetricsConfigurationBuilder read(GlobalMetricsConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "GlobalMetricsConfigurationBuilder [attributes=" + attributes + "]";
   }
}
