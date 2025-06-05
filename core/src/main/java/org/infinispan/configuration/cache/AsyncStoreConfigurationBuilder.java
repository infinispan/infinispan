package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.AsyncStoreConfiguration.ENABLED;
import static org.infinispan.configuration.cache.AsyncStoreConfiguration.FAIL_SILENTLY;
import static org.infinispan.configuration.cache.AsyncStoreConfiguration.MODIFICATION_QUEUE_SIZE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configuration for the async cache store. If enabled, this configuration provides
 * asynchronous writes to the cache store or 'write-behind' caching.
 *
 * @author pmuir
 *
 */
public class AsyncStoreConfigurationBuilder<S> extends AbstractStoreConfigurationChildBuilder<S> implements Builder<AsyncStoreConfiguration> {
   private final AttributeSet attributes;

   AsyncStoreConfigurationBuilder(AbstractStoreConfigurationBuilder<? extends AbstractStoreConfiguration, ?> builder) {
      this(builder, AsyncStoreConfiguration.attributeDefinitionSet());
   }

   AsyncStoreConfigurationBuilder(AbstractStoreConfigurationBuilder<? extends AbstractStoreConfiguration, ?> builder,
         AttributeSet attributeSet) {
      super(builder);
      this.attributes = attributeSet;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * If true, all modifications to this cache store happen asynchronously on a separate thread.
    */
   public AsyncStoreConfigurationBuilder<S> enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   public AsyncStoreConfigurationBuilder<S> disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   public AsyncStoreConfigurationBuilder<S> enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Sets the size of the modification queue for the async store. If updates are made at a rate
    * that is faster than the underlying cache store can process this queue, then the async store
    * behaves like a synchronous store for that period, blocking until the queue can accept more
    * elements.
    */
   public AsyncStoreConfigurationBuilder<S> modificationQueueSize(int i) {
      attributes.attribute(MODIFICATION_QUEUE_SIZE).set(i);
      return this;
   }

   /**
    * @param failSilently If true, the async store attempts to perform write operations only
    *           as many times as configured with `connection-attempts` in the PersistenceConfiguration.
    *           If all attempts fail, the errors are ignored and the write operations are not executed
    *           on the store.
    *           If false, write operations that fail are attempted again when the underlying store
    *           becomes available. If the modification queue becomes full before the underlying
    *           store becomes available, an error is thrown on all future write operations to the store
    *           until the modification queue is flushed. The modification queue is not persisted. If the
    *           underlying store does not become available before the Async store is stopped, queued
    *           modifications are lost.
    */
   public AsyncStoreConfigurationBuilder<S> failSilently(boolean failSilently) {
      attributes.attribute(FAIL_SILENTLY).set(failSilently);
      return this;
   }

   @Override
   public AsyncStoreConfiguration create() {
      return new AsyncStoreConfiguration(attributes.protect());
   }

   @Override
   public AsyncStoreConfigurationBuilder<S> read(AsyncStoreConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "AsyncStoreConfigurationBuilder [attributes=" + attributes + "]";
   }
}
