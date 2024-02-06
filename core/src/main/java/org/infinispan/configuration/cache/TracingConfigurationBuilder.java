package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.TracingConfiguration.ENABLED;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeParser;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.telemetry.SpanCategory;

public class TracingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<TracingConfiguration> {

   private final AttributeSet attributes;

   protected TracingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = TracingConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Enable tracing on the given cache.
    * This property can be used to enable tracing at runtime.
    *
    * @return <code>this</code>, for method chaining
    */
   public TracingConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Disable tracing on the given cache.
    * This property can be used to disable tracing at runtime.
    *
    * @return <code>this</code>, for method chaining
    */
   public TracingConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   public TracingConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Enable tracing for the given category on the given cache.
    * This property can be used to enable tracing at runtime.
    *
    * @return <code>this</code>, for method chaining
    */
   public TracingConfigurationBuilder enableCategory(SpanCategory category) {
      Attribute<Set<SpanCategory>> attribute = attributes.attribute(TracingConfiguration.CATEGORIES);
      Set<SpanCategory> policies = attribute.get();
      boolean added = policies.add(category);
      if (added) {
         attribute.set(policies);
      }

      return this;
   }

   /**
    * Disable tracing for the given category on the given cache.
    * This property can be used to enable tracing at runtime.
    *
    * @return <code>this</code>, for method chaining
    */
   public TracingConfigurationBuilder disableCategory(SpanCategory category) {
      Attribute<Set<SpanCategory>> attribute = attributes.attribute(TracingConfiguration.CATEGORIES);
      Set<SpanCategory> policies = attribute.get();
      boolean removed = policies.remove(category);
      if (removed) {
         attribute.set(policies);
      }

      return this;
   }

   @Override
   public TracingConfiguration create() {
      return new TracingConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(TracingConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "TracingConfigurationBuilder{" +
            "attributes=" + attributes +
            '}';
   }

   @Override
   public void validate() {
      Set<SpanCategory> spanCategories = attributes.attribute(TracingConfiguration.CATEGORIES).get();
      if (spanCategories.contains(SpanCategory.SECURITY)) {
         throw CONFIG.securityCacheTracing();
      }
   }

   enum CategoriesAttributeParser implements AttributeParser<Set<SpanCategory>> {
      INSTANCE;

      @Override
      public Set<SpanCategory> parse(Class klass, String value) {
         LinkedHashSet<SpanCategory> spanCategories = Arrays.stream(value.split(",")).sequential().map(String::trim)
               .map(SpanCategory::fromString)
               .collect(Collectors.toCollection(LinkedHashSet::new));
         if (spanCategories.contains(SpanCategory.SECURITY)) {
            throw CONFIG.securityCacheTracing();
         }
         return spanCategories;
      }
   }
}
