package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A no-op implementation of {@link IracManager} for cache without asynchronous remote site backups.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public class NoOpIracManager implements IracManager {

   private static final NoOpIracManager INSTANCE = new NoOpIracManager();

   private NoOpIracManager() {
   }

   public static NoOpIracManager getInstance() {
      return INSTANCE;
   }

   @Override
   public void trackUpdatedKey(Object key, Object lockOwner) {
      //no-op
   }

   @Override
   public <K> void trackUpdatedKeys(Collection<K> keys, Object lockOwner) {
      //no-op
   }

   @Override
   public void trackKeysFromTransaction(Stream<WriteCommand> modifications, GlobalTransaction lockOwner) {
      //no-op
   }

   @Override
   public void trackClear() {
      //no-op
   }

   @Override
   public void cleanupKey(Object key, Object lockOwner, IracMetadata tombstone) {
      //no-op
   }

   @Override
   public void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology) {
      //no-op
   }

   @Override
   public void requestState(Address origin, IntSet segments) {
      //no-op
   }

   @Override
   public void receiveState(Object key, Object lockOwner, IracMetadata tombstone) {
      //no-op
   }

   @Override
   public CompletionStage<Boolean> checkAndTrackExpiration(Object key) {
      return CompletableFutures.completedTrue();
   }
}
