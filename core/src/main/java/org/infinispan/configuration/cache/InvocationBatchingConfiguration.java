package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * @deprecated Since 16.3, use {@link TransactionConfiguration#invocationBatching()} instead.
 */
@Deprecated(since = "16.3", forRemoval = true)
public class InvocationBatchingConfiguration extends ConfigurationElement<InvocationBatchingConfiguration> {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(InvocationBatchingConfiguration.class, ENABLED);
   }
   private final Attribute<Boolean> enabled;

   InvocationBatchingConfiguration(AttributeSet attributes) {
      super("invocation-batching", attributes);
      enabled = attributes.attribute(ENABLED);
   }

   public boolean enabled() {
      return enabled.get();
   }
}
