package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.AsyncStoreConfiguration.*;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Configuration for the async cache store. If enabled, this provides you with asynchronous writes
 * to the cache store, giving you 'write-behind' caching.
 *
 * @author pmuir
 *
 */
public class AsyncStoreConfigurationBuilder<S> extends AbstractStoreConfigurationChildBuilder<S> implements Builder<AsyncStoreConfiguration> {
   private final AttributeSet attributes;

   AsyncStoreConfigurationBuilder(AbstractStoreConfigurationBuilder<? extends AbstractStoreConfiguration, ?> builder) {
      super(builder);
      this.attributes = AsyncStoreConfiguration.attributeDefinitionSet();
   }

   /**
    * If true, all modifications to this cache store happen asynchronously, on a separate thread.
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
    * Unused.
    */
   @Deprecated
   public AsyncStoreConfigurationBuilder<S> flushLockTimeout(long l) {
      return this;
   }

   /**
    * Unused.
    */
   @Deprecated
   public AsyncStoreConfigurationBuilder<S> flushLockTimeout(long l, TimeUnit unit) {
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
    * Unused.
    */
   @Deprecated
   public AsyncStoreConfigurationBuilder<S> shutdownTimeout(long l) {
      return this;
   }

   /**
    * Unused.
    */
   @Deprecated
   public AsyncStoreConfigurationBuilder<S> shutdownTimeout(long l, TimeUnit unit) {
      return this;
   }

   /**
    * Size of the thread pool whose threads are responsible for applying the modifications.
    */
   public AsyncStoreConfigurationBuilder<S> threadPoolSize(int i) {
      attributes.attribute(THREAD_POOL_SIZE).set(i);
      return this;
   }

   @Override
   public
   void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public AsyncStoreConfiguration create() {
      return new AsyncStoreConfiguration(attributes.protect());
   }

   @Override
   public AsyncStoreConfigurationBuilder<S> read(AsyncStoreConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "AsyncStoreConfigurationBuilder [attributes=" + attributes + "]";
   }
}
