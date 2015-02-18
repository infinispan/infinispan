package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
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
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<Long> FLUSH_LOCK_TIMEOUT = AttributeDefinition.builder("flushLockTimeout", 1l).build();
   public static final AttributeDefinition<Integer> MODIFICATION_QUEUE_SIZE  = AttributeDefinition.builder("modificationQueueSize", 1024).immutable().build();
   public static final AttributeDefinition<Long> SHUTDOWN_TIMEOUT = AttributeDefinition.builder("shutdownTimeout", TimeUnit.SECONDS.toMillis(25)).build();
   public static final AttributeDefinition<Integer> THREAD_POOL_SIZE = AttributeDefinition.builder("threadPoolSize", 1).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AsyncStoreConfiguration.class, ENABLED, FLUSH_LOCK_TIMEOUT, MODIFICATION_QUEUE_SIZE, SHUTDOWN_TIMEOUT, THREAD_POOL_SIZE);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Long> flushLockTimeout;
   private final Attribute<Integer> modificationQueueSize;
   private final Attribute<Long> shutdownTimeout;
   private final Attribute<Integer> threadPoolSize;

   private final AttributeSet attributes;

   AsyncStoreConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      enabled = attributes.attribute(ENABLED);
      flushLockTimeout = attributes.attribute(FLUSH_LOCK_TIMEOUT);
      modificationQueueSize = attributes.attribute(MODIFICATION_QUEUE_SIZE);
      shutdownTimeout = attributes.attribute(SHUTDOWN_TIMEOUT);
      threadPoolSize = attributes.attribute(THREAD_POOL_SIZE);
   }

   /**
    * If true, all modifications to this cache store happen asynchronously, on a separate thread.
    */
   public boolean enabled() {
      return enabled.get();
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically. The timeout can be adjusted for a running cache.
    *
    * @return
    */
   public long flushLockTimeout() {
      return flushLockTimeout.get();
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically. The timeout can be adjusted for a running cache.
    */
   public AsyncStoreConfiguration flushLockTimeout(long l) {
      flushLockTimeout.set(l);
      return this;
   }

   /**
    * Sets the size of the modification queue for the async store. If updates are made at a rate
    * that is faster than the underlying cache store can process this queue, then the async store
    * behaves like a synchronous store for that period, blocking until the queue can accept more
    * elements.
    */
   public int modificationQueueSize() {
      return modificationQueueSize.get();
   }

   /**
    * Timeout to stop the cache store. When the store is stopped it's possible that some
    * modifications still need to be applied; you likely want to set a very large timeout to make
    * sure to not loose data
    */
   public long shutdownTimeout() {
      return shutdownTimeout.get();
   }

   public AsyncStoreConfiguration shutdownTimeout(long l) {
      shutdownTimeout.set(l);
      return this;
   }

   /**
    * Size of the thread pool whose threads are responsible for applying the modifications.
    */
   public int threadPoolSize() {
      return threadPoolSize.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "AsyncStoreConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      AsyncStoreConfiguration other = (AsyncStoreConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

}
