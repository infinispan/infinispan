package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;

public class GlobalMetricsConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<Boolean> GAUGES = AttributeDefinition.builder("gauges", true).immutable().build();
   public static final AttributeDefinition<Boolean> HISTOGRAMS = AttributeDefinition.builder("histograms", false).immutable().build();
   public static final AttributeDefinition<String> PREFIX = AttributeDefinition.builder("prefix", "").immutable().build();
   public static final AttributeDefinition<Boolean> NAMES_AS_TAGS = AttributeDefinition.builder("namesAsTags", false).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalMetricsConfiguration.class, GAUGES, HISTOGRAMS, PREFIX, NAMES_AS_TAGS);
   }

   private static final ElementDefinition<GlobalMetricsConfiguration> ELEMENT_DEFINITION = new DefaultElementDefinition<>(Element.METRICS.getLocalName());

   private final AttributeSet attributes;

   private final Attribute<Boolean> gauges;

   private final Attribute<Boolean> histograms;

   private final Attribute<String> prefix;

   private final Attribute<Boolean> namesAsTags;

   GlobalMetricsConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.gauges = attributes.attribute(GAUGES);
      this.histograms = attributes.attribute(HISTOGRAMS);
      this.prefix = attributes.attribute(PREFIX);
      this.namesAsTags = attributes.attribute(NAMES_AS_TAGS);
   }

   @Override
   public ElementDefinition<GlobalMetricsConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
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

   public boolean gauges() {
      return gauges.get();
   }

   public boolean histograms() {
      return histograms.get();
   }

   public String prefix() {
      return prefix.get();
   }

   public boolean namesAsTags() {
      return namesAsTags.get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GlobalMetricsConfiguration that = (GlobalMetricsConfiguration) o;
      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
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
