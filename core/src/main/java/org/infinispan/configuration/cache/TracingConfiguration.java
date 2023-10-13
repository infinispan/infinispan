package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.telemetry.SpanCategory;

public class TracingConfiguration extends ConfigurationElement<TracingConfiguration> {

   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, true, Boolean.class).build();
   public static final AttributeDefinition<Boolean> CONTAINER = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CONTAINER, true, Boolean.class).build();
   public static final AttributeDefinition<Boolean> CLUSTER = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CLUSTER, false, Boolean.class).build();
   public static final AttributeDefinition<Boolean> X_SITE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.X_SITE, false, Boolean.class).build();
   public static final AttributeDefinition<Boolean> PERSISTENCE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.PERSISTENCE, false, Boolean.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TracingConfiguration.class, ENABLED, CONTAINER, CLUSTER, X_SITE, PERSISTENCE);
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

   public boolean container() {
      return enabled() && attributes.attribute(CONTAINER).get();
   }

   public boolean cluster() {
      return enabled() && attributes.attribute(CLUSTER).get();
   }

   public boolean xSite() {
      return enabled() && attributes.attribute(X_SITE).get();
   }

   public boolean persistence() {
      return enabled() && attributes.attribute(PERSISTENCE).get();
   }

   public boolean enabled(SpanCategory category) {
      return enabled() && switch (category) {
         case CONTAINER -> container();
         case CLUSTER -> cluster();
         case X_SITE -> xSite();
         case PERSISTENCE -> persistence();
      };
   }
}
