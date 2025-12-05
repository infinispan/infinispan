package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.configuration.parsing.Element;

/**
 * Configures how state is retrieved when a new cache joins the cluster.
 * Used with distribution and replication clustered modes.
 *
 * @since 5.1
 */
public class StateTransferConfiguration extends ConfigurationElement<StateTransferConfiguration> {
   public static final AttributeDefinition<Boolean> AWAIT_INITIAL_TRANSFER = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.AWAIT_INITIAL_TRANSFER, true).global(false).build();
   public static final AttributeDefinition<Boolean> FETCH_IN_MEMORY_STATE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, true).immutable().build();
   public static final AttributeDefinition<TimeQuantity> TIMEOUT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TIMEOUT, TimeQuantity.valueOf("4m")).parser(TimeQuantity.PARSER).build();
   public static final AttributeDefinition<Integer> CHUNK_SIZE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CHUNK_SIZE, 512).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StateTransferConfiguration.class, FETCH_IN_MEMORY_STATE, TIMEOUT, CHUNK_SIZE, AWAIT_INITIAL_TRANSFER);
   }

   private final Attribute<Boolean> awaitInitialTransfer;
   private final Attribute<Boolean> fetchInMemoryState;
   private final Attribute<TimeQuantity> timeout;
   private final Attribute<Integer> chunkSize;

   StateTransferConfiguration(AttributeSet attributes) {
      super(Element.STATE_TRANSFER, attributes);
      awaitInitialTransfer = attributes.attribute(AWAIT_INITIAL_TRANSFER);
      fetchInMemoryState = attributes.attribute(FETCH_IN_MEMORY_STATE);
      timeout = attributes.attribute(TIMEOUT);
      chunkSize = attributes.attribute(CHUNK_SIZE);
   }

   /**
    * If {@code true}, the cache will fetch data from the neighboring caches when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
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
      return timeout.get().longValue();
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
}
