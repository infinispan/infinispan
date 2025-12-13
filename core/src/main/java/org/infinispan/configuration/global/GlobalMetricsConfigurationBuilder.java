package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalMetricsConfiguration.ACCURATE_SIZE;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.GAUGES;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.HISTOGRAMS;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.JVM;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.LEGACY;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.NAMES_AS_TAGS;
import static org.infinispan.configuration.global.GlobalMetricsConfiguration.PREFIX;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures the types of metrics gathered and exported via Micrometer metrics for all caches owned by this Cache Manager.
 * Gauges do not have any performance penalty so are enabled by default.
 * Histograms are more expensive to compute so must be enabled manually.
 * Enabling metrics in configuration has no effect unless the necessary
 * Micrometer JAR is available on the classpath.
 */
public class GlobalMetricsConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<GlobalMetricsConfiguration> {

   private final AttributeSet attributes;

   GlobalMetricsConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = GlobalMetricsConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Metrics are enabled if at least one of the metric types is enabled. See {@link #gauges()}, {@link #histograms()}.
    */
   public boolean enabled() {
      return gauges() || histograms();
   }

   /**
    * Are gauges enabled?
    */
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

   /**
    * Are histograms enabled?
    */
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

   /**
    * The global prefix to add to all metric names.
    */
   @Deprecated(forRemoval = true, since = "16.0")
   public String prefix() {
      return attributes.attribute(PREFIX).get();
   }

   /**
    * The global prefix to add to all metric names.
    */
   @Deprecated(forRemoval = true, since = "16.0")
   public GlobalMetricsConfigurationBuilder prefix(String prefix) {
      attributes.attribute(PREFIX).set(prefix);
      return this;
   }

   /**
    * Put the cache manager and cache name in tags rather than including them in the metric name.
    */
   @Deprecated(forRemoval = true, since = "16.0")
   public boolean namesAsTags() {
      return attributes.attribute(NAMES_AS_TAGS).get();
   }

   /**
    * Put the cache manager and cache name in tags rather than including them in the metric name.
    */
   @Deprecated(forRemoval = true, since = "16.0")
   public GlobalMetricsConfigurationBuilder namesAsTags(boolean namesAsTags) {
      attributes.attribute(NAMES_AS_TAGS).set(namesAsTags);
      return this;
   }

   /**
    * Enables accurate size computation for numberOfEntries statistics. Note that this doesn't affect invocations of
    * the {@link Cache#size()} method.
    */
   public GlobalMetricsConfigurationBuilder accurateSize(boolean accurateSize) {
      attributes.attribute(ACCURATE_SIZE).set(accurateSize);
      return this;
   }

   /**
    * Whether JVM metrics should be reported.
    */
   @Deprecated(forRemoval = true, since = "16.0")
   public boolean jvm() {
      return attributes.attribute(JVM).get();
   }

   /**
    * Whether JVM metrics should be reported.
    */
   @Deprecated(forRemoval = true, since = "16.0")
   public GlobalMetricsConfigurationBuilder jvm(boolean jvm) {
      attributes.attribute(JVM).set(jvm);
      return this;
   }

   /**
    * Whether legacy metrics should be reported.
    */
   public boolean legacy() {
      return attributes.attribute(LEGACY).get();
   }

   /**
    * Whether legacy metrics should be reported.
    */
   public GlobalMetricsConfigurationBuilder legacy(boolean legacy) {
      attributes.attribute(LEGACY).set(legacy);
      return this;
   }

   @Override
   public GlobalMetricsConfiguration create() {
      return new GlobalMetricsConfiguration(attributes.protect());
   }

   @Override
   public GlobalMetricsConfigurationBuilder read(GlobalMetricsConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "GlobalMetricsConfigurationBuilder [attributes=" + attributes + "]";
   }
}
