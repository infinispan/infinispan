package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * Configuration for the async cache store. If enabled, this provides you with asynchronous writes
 * to the cache store, giving you 'write-behind' caching.
 *
 * @author pmuir
 *
 */
public class AsyncStoreConfiguration extends ConfigurationElement<AsyncStoreConfiguration> {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).immutable().build();
   public static final AttributeDefinition<Integer> MODIFICATION_QUEUE_SIZE  = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MODIFICATION_QUEUE_SIZE, 1024).immutable().build();
   public static final AttributeDefinition<Boolean> FAIL_SILENTLY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.FAIL_SILENTLY, false).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AsyncStoreConfiguration.class, ENABLED, MODIFICATION_QUEUE_SIZE, FAIL_SILENTLY);
   }

   private final Attribute<Boolean> failSilently;

   public AsyncStoreConfiguration(AttributeSet attributes) {
      super(Element.WRITE_BEHIND, attributes);
      this.failSilently = attributes.attribute(FAIL_SILENTLY);
   }

   /**
    * If true, all modifications to this cache store happen asynchronously, on a separate thread.
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   /**
    * Sets the size of the modification queue for the async store. If updates are made at a rate
    * that is faster than the underlying cache store can process this queue, then the async store
    * behaves like a synchronous store for that period, blocking until the queue can accept more
    * elements.
    */
   public int modificationQueueSize() {
      return attributes.attribute(MODIFICATION_QUEUE_SIZE).get();
   }

   public boolean failSilently() {
      return failSilently.get();
   }
}
