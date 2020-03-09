package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalMetricsConfiguration.GAUGES;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.HISTOGRAMS;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.NAMES_AS_TAGS;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.PREFIX;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures the types of metrics gathered and exported via microprofile metrics for all caches under this cache
 * manager. Gauges do not have any performance penalty so are enabled by default. Histograms are more expensive to
 * compute so should be enabled manually when needed.
 */
public class GlobalMetricsConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<GlobalMetricsConfiguration> {

   private final AttributeSet attributes;

   GlobalMetricsConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = GlobalMetricsConfiguration.attributeDefinitionSet();
   }

   /**
    * Metrics are enabled if at least one of the metric types is enabled. See {@link #gauges()}, {@link #histograms()}.
    */
   public boolean enabled() {
      return gauges() || histograms();
   }

   public boolean gauges() {
      return attributes.attribute(GAUGES).get();
   }

   /**
    * Enables or disables gauges.
    */
   public GlobalMetricsConfigurationBuilder gauges(boolean gauges) {
      attributes.attribute(GAUGES).set(gauges);
      return this;
   }

   public boolean histograms() {
      return attributes.attribute(HISTOGRAMS).get();
   }

   /**
    * Enables or disables histograms.
    */
   public GlobalMetricsConfigurationBuilder histograms(boolean histograms) {
      attributes.attribute(HISTOGRAMS).set(histograms);
      return this;
   }

   public String prefix() {
      return attributes.attribute(PREFIX).get();
   }

   /**
    * The global prefix to add to all metric names.
    */
   public GlobalMetricsConfigurationBuilder prefix(String prefix) {
      attributes.attribute(PREFIX).set(prefix);
      return this;
   }

   public boolean namesAsTags() {
      return attributes.attribute(NAMES_AS_TAGS).get();
   }

   /**
    * Put the cache manager and cache name in tags rather then include them in the metric name.
    */
   public GlobalMetricsConfigurationBuilder namesAsTags(boolean namesAsTags) {
      attributes.attribute(NAMES_AS_TAGS).set(namesAsTags);
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
