package org.infinispan.configuration.cache;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.telemetry.SpanCategory;

public class TracingConfiguration extends ConfigurationElement<TracingConfiguration> {

   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, true, Boolean.class).build();

   public static final AttributeDefinition<Set<SpanCategory>> CATEGORIES = AttributeDefinition.builder(
           org.infinispan.configuration.parsing.Attribute.CATEGORIES, new LinkedHashSet<>(Collections.singleton(SpanCategory.CONTAINER)),
                   (Class<Set<SpanCategory>>) (Class<?>) Set.class)
           .initializer(LinkedHashSet::new).serializer(AttributeSerializer.ENUM_SET)
         .parser(TracingConfigurationBuilder.CategoriesAttributeParser.INSTANCE).build();

   static AttributeSet attributeDefinitionSet() {
      AttributeSet attributeSet = new AttributeSet(TracingConfiguration.class, ENABLED, CATEGORIES);
      attributeSet.attribute(CATEGORIES).set(new LinkedHashSet<>(Collections.singleton(SpanCategory.CONTAINER)));

      return attributeSet;
   }

   protected TracingConfiguration(AttributeSet attributes) {
      super(Element.TRACING, attributes);
   }

   /**
    * Whether tracing is enabled or disabled on the given cache.
    * This property can be used to enable or disable tracing at runtime.
    *
    * @return Whether the tracing is enabled on the given cache
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public Set<SpanCategory> categories() {
      return attributes.attribute(CATEGORIES).get();
   }

   public boolean enabled(SpanCategory category) {
      if (!enabled()) {
         return false;
      }

      return attributes.attribute(CATEGORIES).get().contains(category);
   }
}
