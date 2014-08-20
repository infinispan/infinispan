package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * Configures how state is transferred when a cache joins or leaves the cluster. Used in distributed and
 * replication clustered modes.
 *
 * @since 5.1
 */
public class StateTransferConfigurationBuilder extends
      AbstractClusteringConfigurationChildBuilder implements Builder<StateTransferConfiguration> {

   private static final Log log = LogFactory.getLog(StateTransferConfigurationBuilder.class);

   private Boolean fetchInMemoryState = null;
   private Boolean awaitInitialTransfer = null;
   private int chunkSize = 512;
   private long timeout = TimeUnit.MINUTES.toMillis(4);

   StateTransferConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * If {@code true}, the cache will fetch data from the neighboring caches when it starts up, so
    * the cache starts 'warm', although it will impact startup time.
    * <p/>
    * In distributed mode, state is transferred between running caches as well, as the ownership of
    * keys changes (e.g. because a cache left the cluster). Disabling this setting means a key will
    * sometimes have less than {@code numOwner} owners.
    */
   public StateTransferConfigurationBuilder fetchInMemoryState(boolean b) {
      this.fetchInMemoryState = b;
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
      this.awaitInitialTransfer = b;
      return this;
   }

   /**
    * The state will be transferred in batches of {@code chunkSize} cache entries.
    * If chunkSize is equal to Integer.MAX_VALUE, the state will be transferred in all at once. Not recommended.
    */
   public StateTransferConfigurationBuilder chunkSize(int i) {
      this.chunkSize = i;
      return this;
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    */
   public StateTransferConfigurationBuilder timeout(long l) {
      this.timeout = l;
      return this;
   }

   /**
    * This is the maximum amount of time - in milliseconds - to wait for state from neighboring
    * caches, before throwing an exception and aborting startup.
    */
   public StateTransferConfigurationBuilder timeout(long l, TimeUnit unit) {
      return timeout(unit.toMillis(l));
   }

   @Override
   public void validate() {
      if (chunkSize <= 0) {
         throw new CacheConfigurationException("chunkSize can not be <= 0");
      }

      if (awaitInitialTransfer != null && awaitInitialTransfer
            && !getClusteringBuilder().cacheMode().isReplicated() && !getClusteringBuilder().cacheMode().isDistributed())
         throw new CacheConfigurationException(
               "awaitInitialTransfer can be enabled only if cache mode is distributed or replicated.");
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public  StateTransferConfiguration create() {
      // If replicated or distributed and fetch state transfer was not explicitly
      // disabled, then force enabling of state transfer
      CacheMode cacheMode = getClusteringBuilder().cacheMode();
      boolean _fetchInMemoryState;
      if (fetchInMemoryState != null) {
         _fetchInMemoryState = fetchInMemoryState;
      } else if (cacheMode.isReplicated() || cacheMode.isDistributed()) {
         log.trace("Cache is distributed or replicated but state transfer was not defined, enabling it by default");
         _fetchInMemoryState = true;
      } else {
         _fetchInMemoryState = false;
      }

      // If replicated or distributed and awaitInitialTransfer was not explicitly disabled,
      // then enable it by default
      boolean _awaitInitialTransfer;
      if (awaitInitialTransfer != null) {
         _awaitInitialTransfer = awaitInitialTransfer;
      } else if (cacheMode.isClustered()) {
         log.trace("Cache is distributed or replicated but awaitInitialTransfer was not defined, enabling it by default");
         _awaitInitialTransfer = true;
      } else {
         _awaitInitialTransfer = false;
      }
      return new StateTransferConfiguration(_fetchInMemoryState, fetchInMemoryState,
            timeout, chunkSize, _awaitInitialTransfer, awaitInitialTransfer);
   }

   @Override
   public StateTransferConfigurationBuilder read(StateTransferConfiguration template) {
      this.fetchInMemoryState = template.originalFetchInMemoryState();
      this.awaitInitialTransfer = template.originalAwaitInitialTransfer();
      this.timeout = template.timeout();
      this.chunkSize = template.chunkSize();
      return this;
   }

   @Override
   public String toString() {
      return "StateTransferConfigurationBuilder{" +
            "chunkSize=" + chunkSize +
            ", fetchInMemoryState=" + fetchInMemoryState +
            ", awaitInitialTransfer=" + awaitInitialTransfer +
            ", timeout=" + timeout +
            '}';
   }
}
