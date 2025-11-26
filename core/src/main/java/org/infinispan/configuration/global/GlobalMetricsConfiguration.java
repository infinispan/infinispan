package org.infinispan.configuration.global;

import java.util.Objects;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configuration for metrics. See {@link GlobalMetricsConfigurationBuilder}.
 */
public class GlobalMetricsConfiguration {

   public static final AttributeDefinition<Boolean> GAUGES = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.GAUGES, true).immutable().build();
   public static final AttributeDefinition<Boolean> HISTOGRAMS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.HISTOGRAMS, false).immutable().build();
   @Deprecated(forRemoval = true, since = "16.0")
   public static final AttributeDefinition<String> PREFIX = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.PREFIX, "").immutable().build();
   @Deprecated(forRemoval = true, since = "16.0")
   public static final AttributeDefinition<Boolean> NAMES_AS_TAGS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.NAMES_AS_TAGS, true).immutable().build();
   public static final AttributeDefinition<Boolean> ACCURATE_SIZE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ACCURATE_SIZE, false).build();
   @Deprecated(forRemoval = true, since = "16.0")
   public static final AttributeDefinition<Boolean> JVM = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.JVM, false).build();
   @Deprecated(forRemoval = true, since = "16.0")
   public static final AttributeDefinition<Boolean> LEGACY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.LEGACY, false).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalMetricsConfiguration.class, GAUGES, HISTOGRAMS, PREFIX, NAMES_AS_TAGS, ACCURATE_SIZE, JVM, LEGACY);
   }

   private final AttributeSet attributes;
   private final Attribute<Boolean> gauges;
   private final Attribute<Boolean> histograms;
   @Deprecated(forRemoval = true, since = "16.0")
   private final Attribute<String> prefix;
   @Deprecated(forRemoval = true, since = "16.0")
   private final Attribute<Boolean> namesAsTags;
   private final Attribute<Boolean> accurateSize;
   private final Attribute<Boolean> jvm;
   private final Attribute<Boolean> legacy;

   GlobalMetricsConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.gauges = attributes.attribute(GAUGES);
      this.histograms = attributes.attribute(HISTOGRAMS);
      this.prefix = attributes.attribute(PREFIX);
      this.namesAsTags = attributes.attribute(NAMES_AS_TAGS);
      this.accurateSize = attributes.attribute(ACCURATE_SIZE);
      this.jvm = attributes.attribute(JVM);
      this.legacy = attributes.attribute(LEGACY);
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
   @Deprecated(forRemoval = true, since = "16.0")
   public String prefix() {
      return prefix.get();
   }

   /**
    * Put the cache manager and cache name in tags rather then include them in the metric name.
    */
   @Deprecated(forRemoval = true, since = "16.0")
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

   /**
    * Whether JVM metrics should be reported.
    */
   @Deprecated(forRemoval = true, since = "16.0")
   public boolean jvm() {
      return jvm.get();
   }

   /**
    * Whether legacy metrics should be reported.
    * @return
    */
   public boolean legacy() {
      return legacy.get();
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
