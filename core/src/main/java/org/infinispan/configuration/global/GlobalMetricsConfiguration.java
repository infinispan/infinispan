package org.infinispan.configuration.global;

import java.util.Objects;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configuration for metrics. See {@link GlobalMetricsConfigurationBuilder}.
 */
public class GlobalMetricsConfiguration {

   public static final AttributeDefinition<Boolean> GAUGES = AttributeDefinition.builder("gauges", true).immutable().build();
   public static final AttributeDefinition<Boolean> HISTOGRAMS = AttributeDefinition.builder("histograms", false).immutable().build();
   public static final AttributeDefinition<String> PREFIX = AttributeDefinition.builder("prefix", "").immutable().build();
   public static final AttributeDefinition<Boolean> NAMES_AS_TAGS = AttributeDefinition.builder("namesAsTags", false).immutable().build();
   public static final AttributeDefinition<Boolean> ACCURATE_SIZE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ACCURATE_SIZE, false).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalMetricsConfiguration.class, GAUGES, HISTOGRAMS, PREFIX, NAMES_AS_TAGS, ACCURATE_SIZE);
   }

   private final AttributeSet attributes;
   private final Attribute<Boolean> gauges;
   private final Attribute<Boolean> histograms;
   private final Attribute<String> prefix;
   private final Attribute<Boolean> namesAsTags;
   private final Attribute<Boolean> accurateSize;

   GlobalMetricsConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.gauges = attributes.attribute(GAUGES);
      this.histograms = attributes.attribute(HISTOGRAMS);
      this.prefix = attributes.attribute(PREFIX);
      this.namesAsTags = attributes.attribute(NAMES_AS_TAGS);
      this.accurateSize = attributes.attribute(ACCURATE_SIZE);
   }

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
      return gauges.get();
   }

   /**
    * Are histograms enabled?
    */
   public boolean histograms() {
      return histograms.get();
   }

   /**
    * The global prefix to add to all metric names.
    */
   public String prefix() {
      return prefix.get();
   }

   /**
    * Put the cache manager and cache name in tags rather then include them in the metric name.
    */
   public boolean namesAsTags() {
      return namesAsTags.get();
   }

   /**
    * Whether cache sizes should be computed
    * @return
    */
   public boolean accurateSize() {
      return accurateSize.get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GlobalMetricsConfiguration that = (GlobalMetricsConfiguration) o;
      return Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "GlobalMetricsConfiguration{attributes=" + attributes + '}';
   }
}
