package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configuration for the async cache store. If enabled, this provides you with asynchronous writes
 * to the cache store, giving you 'write-behind' caching.
 *
 * @author pmuir
 *
 */
public class AsyncStoreConfiguration {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   static final AttributeDefinition<Long> FLUSH_LOCK_TIMEOUT = AttributeDefinition.builder("flushLockTimeout", 1l).build();
   static final AttributeDefinition<Integer> MODIFICATION_QUEUE_SIZE  = AttributeDefinition.builder("modificationQueueSize", 1024).immutable().build();
   static final AttributeDefinition<Long> SHUTDOWN_TIMEOUT = AttributeDefinition.builder("shutdownTimeout", TimeUnit.SECONDS.toMillis(25)).build();
   static final AttributeDefinition<Integer> THREAD_POOL_SIZE = AttributeDefinition.builder("threadPoolSize", 1).immutable().build();
   static AttributeSet attributeSet() {
      return new AttributeSet(AsyncStoreConfiguration.class, ENABLED, FLUSH_LOCK_TIMEOUT, MODIFICATION_QUEUE_SIZE, SHUTDOWN_TIMEOUT, THREAD_POOL_SIZE);
   }

   private final AttributeSet attributes;

   AsyncStoreConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   /**
    * If true, all modifications to this cache store happen asynchronously, on a separate thread.
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).asBoolean();
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically. The timeout can be adjusted for a running cache.
    *
    * @return
    */
   public long flushLockTimeout() {
      return attributes.attribute(FLUSH_LOCK_TIMEOUT).asLong();
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically. The timeout can be adjusted for a running cache.
    */
   public AsyncStoreConfiguration flushLockTimeout(long l) {
      attributes.attribute(FLUSH_LOCK_TIMEOUT).set(l);
      return this;
   }

   /**
    * Sets the size of the modification queue for the async store. If updates are made at a rate
    * that is faster than the underlying cache store can process this queue, then the async store
    * behaves like a synchronous store for that period, blocking until the queue can accept more
    * elements.
    */
   public int modificationQueueSize() {
      return attributes.attribute(MODIFICATION_QUEUE_SIZE).asInteger();
   }

   /**
    * Timeout to stop the cache store. When the store is stopped it's possible that some
    * modifications still need to be applied; you likely want to set a very large timeout to make
    * sure to not loose data
    */
   public long shutdownTimeout() {
      return attributes.attribute(SHUTDOWN_TIMEOUT).asLong();
   }

   public AsyncStoreConfiguration shutdownTimeout(long l) {
      attributes.attribute(SHUTDOWN_TIMEOUT).set(l);
      return this;
   }

   /**
    * Size of the thread pool whose threads are responsible for applying the modifications.
    */
   public int threadPoolSize() {
      return attributes.attribute(THREAD_POOL_SIZE).asInteger();
   }

   @Override
   public String toString() {
      return attributes.toString();
   }

   @Override
   public boolean equals(Object o) {
      AsyncStoreConfiguration other = (AsyncStoreConfiguration) o;
      return equals(other.attributes);
   }

   @Override
   public int hashCode() {
      return hashCode();
   }

   AttributeSet values() {
      return attributes;
   }

}
