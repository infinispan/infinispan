package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.StateTransferConfiguration.AWAIT_INITIAL_TRANSFER;
import static org.infinispan.configuration.cache.StateTransferConfiguration.CHUNK_SIZE;
import static org.infinispan.configuration.cache.StateTransferConfiguration.FETCH_IN_MEMORY_STATE;
import static org.infinispan.configuration.cache.StateTransferConfiguration.TIMEOUT;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Configures how state is transferred when a cache joins or leaves the cluster. Used in distributed and
 * replication clustered modes.
 *
 * @since 5.1
 */
public class StateTransferConfigurationBuilder extends
      AbstractClusteringConfigurationChildBuilder implements Builder<StateTransferConfiguration> {
   private final AttributeSet attributes;

   StateTransferConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      attributes = StateTransferConfiguration.attributeDefinitionSet();
   }

   /**
    * If {@code true}, the cache will fetch data from the neighboring caches when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
        * In distributed mode, state is transferred between running caches as well, as the ownership of
    * keys changes (e.g. because a cache left the cluster). Disabling this setting means a key will
    * sometimes have less than {@code numOwner} owners.
    */
   public StateTransferConfigurationBuilder fetchInMemoryState(boolean b) {
      attributes.attribute(FETCH_IN_MEMORY_STATE).set(b);
      return this;
   }

   /**
    * If {@code true}, this will cause the first call to method {@code CacheManager.getCache()} on the joiner node to
    * block and wait until the joining is complete and the cache has finished receiving state from neighboring caches
    * (if fetchInMemoryState is enabled). This option applies to distributed and replicated caches only and is enabled
    * by default. Please note that setting this to {@code false} will make the cache object available immediately but
    * any access to keys that should be available locally but are not yet transferred will actually cause a (transparent)
    * remote access. While this will not have any impact on the logic of your application it might impact performance.
    */
   public StateTransferConfigurationBuilder awaitInitialTransfer(boolean b) {
      attributes.attribute(AWAIT_INITIAL_TRANSFER).set(b);
      return this;
   }

   /**
    * The state will be transferred in batches of {@code chunkSize} cache entries.
    * If chunkSize is equal to Integer.MAX_VALUE, the state will be transferred in all at once. Not recommended.
    */
   public StateTransferConfigurationBuilder chunkSize(int i) {
      attributes.attribute(CHUNK_SIZE).set(i);
      return this;
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    *
    * Must be greater than or equal to 'remote-timeout' in the clustering configuration.
    */
   public StateTransferConfigurationBuilder timeout(long l) {
      attributes.attribute(TIMEOUT).set(TimeQuantity.valueOf(l));
      return this;
   }

   /**
    * Same as {@link #timeout(long)} but supporting time units
    */
   public StateTransferConfigurationBuilder timeout(String s) {
      attributes.attribute(TIMEOUT).set(TimeQuantity.valueOf(s));
      return this;
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    *
    * Must be greater than or equal to 'remote-timeout' in the clustering configuration.
    */
   public StateTransferConfigurationBuilder timeout(long l, TimeUnit unit) {
      return timeout(unit.toMillis(l));
   }

   @Override
   public void validate() {
      int chunkSize = attributes.attribute(CHUNK_SIZE).get();
      if (chunkSize <= 0) {
         throw CONFIG.invalidChunkSize(chunkSize);
      }

      if (clustering().cacheMode().isInvalidation()) {
         Attribute<Boolean> fetchAttribute = attributes.attribute(FETCH_IN_MEMORY_STATE);
         if (fetchAttribute.isModified() && fetchAttribute.get()) {
            throw CONFIG.attributeNotAllowedInInvalidationMode(FETCH_IN_MEMORY_STATE.name());
         }
      }

      Attribute<Boolean> awaitInitialTransfer = attributes.attribute(AWAIT_INITIAL_TRANSFER);
      if (awaitInitialTransfer.isModified() && awaitInitialTransfer.get()
            && !getClusteringBuilder().cacheMode().needsStateTransfer())
         throw CONFIG.awaitInitialTransferOnlyForDistOrRepl();

      Attribute<TimeQuantity> timeoutAttribute = attributes.attribute(TIMEOUT);
      Attribute<TimeQuantity> remoteTimeoutAttribute = clustering().attributes.attribute(ClusteringConfiguration.REMOTE_TIMEOUT);
      if (timeoutAttribute.get().longValue() < remoteTimeoutAttribute.get().longValue()) {
         throw CONFIG.invalidStateTransferTimeout(timeoutAttribute.get().toString(), remoteTimeoutAttribute.get().toString());
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public  StateTransferConfiguration create() {
      return new StateTransferConfiguration(attributes.protect());
   }

   @Override
   public StateTransferConfigurationBuilder read(StateTransferConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "StateTransferConfigurationBuilder [attributes=" + attributes + "]";
   }

   public AttributeSet attributes() {
      return attributes;
   }
}
