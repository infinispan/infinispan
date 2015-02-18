package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures how state is retrieved when a new cache joins the cluster.
 * Used with invalidation and replication clustered modes.
 *
 * @since 5.1
 */
public class StateTransferConfiguration {
   public static final AttributeDefinition<Boolean> AWAIT_INITIAL_TRANSFER = AttributeDefinition.builder("awaitInitialTransfer", true).immutable().build();
   public static final AttributeDefinition<Boolean> FETCH_IN_MEMORY_STATE = AttributeDefinition.builder("fetchInMemoryState", true).immutable().build();
   public static final AttributeDefinition<Long> TIMEOUT = AttributeDefinition.builder("timeout", TimeUnit.MINUTES.toMillis(4)).immutable().build();
   public static final AttributeDefinition<Integer> CHUNK_SIZE = AttributeDefinition.builder("chunkSize", 512).immutable().build();

   static final AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StoreAsBinaryConfiguration.class, FETCH_IN_MEMORY_STATE, TIMEOUT, CHUNK_SIZE, AWAIT_INITIAL_TRANSFER);
   }

   private final Attribute<Boolean> awaitInitialTransfer;
   private final Attribute<Boolean> fetchInMemoryState;
   private final Attribute<Long> timeout;
   private final Attribute<Integer> chunkSize;
   private final AttributeSet attributes;

   StateTransferConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      awaitInitialTransfer = attributes.attribute(AWAIT_INITIAL_TRANSFER);
      fetchInMemoryState = attributes.attribute(FETCH_IN_MEMORY_STATE);
      timeout = attributes.attribute(TIMEOUT);
      chunkSize = attributes.attribute(CHUNK_SIZE);
   }

   /**
    * If {@code true}, the cache will fetch data from the neighboring caches when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
    * <p/>
    * In distributed mode, state is transferred between running caches as well, as the ownership of
    * keys changes (e.g. because a cache left the cluster). Disabling this setting means a key will
    * sometimes have less than {@code numOwner} owners.
    */
   public boolean fetchInMemoryState() {
      return fetchInMemoryState.get();
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    */
   public long timeout() {
      return timeout.get();
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    */
   public StateTransferConfiguration timeout(long l) {
      timeout.set(l);
      return this;
   }

   /**
    * The state will be transferred in batches of {@code chunkSize} cache entries.
    * If chunkSize is equal to Integer.MAX_VALUE, the state will be transferred in all at once. Not recommended.
    */
   public int chunkSize() {
      return chunkSize.get();
   }

   /**
    * If {@code true}, this will cause the first call to method {@code CacheManager.getCache()} on the joiner node to
    * block and wait until the joining is complete and the cache has finished receiving state from neighboring caches
    * (if fetchInMemoryState is enabled). This option applies to distributed and replicated caches only and is enabled
    * by default. Please note that setting this to {@code false} will make the cache object available immediately but
    * any access to keys that should be available locally but are not yet transferred will actually cause a (transparent)
    * remote access. While this will not have any impact on the logic of your application it might impact performance.
    */
   public boolean awaitInitialTransfer() {
      return awaitInitialTransfer.get();
   }

   /**
    * We want to remember if the user didn't configure awaitInitialTransfer for the default cache.
    */
   private boolean originalAwaitInitialTransfer() {
      return !awaitInitialTransfer.isModified();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + attributes;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      StateTransferConfiguration other = (StateTransferConfiguration) obj;
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
