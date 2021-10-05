package org.infinispan.scattered.impl;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.manager.PreloadManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Scattered caches always need to preload all entries.
 *
 * @author Dan Berindei
 * @since 13.0
 */
@Scope(Scopes.NAMED_CACHE)
public class ScatteredPreloadManager extends PreloadManager {

   @Inject ScatteredVersionManager<?> scatteredVersionManager;
   @Inject ClusterTopologyManager clusterTopologyManager;

   @Override
   public void start() {
      super.start();

      initTopologyId();
   }

   private void initTopologyId() {
      int preloadedTopologyId = scatteredVersionManager.getPreloadedTopologyId();
      if (isFullyPreloaded()) {
         // We don't have to do the preload, already done
         if (preloadedTopologyId > 0) {
            clusterTopologyManager.setInitialCacheTopologyId(cache.wired().getName(), preloadedTopologyId + 1);
         }
         return;
      }
      // An alternative (not implemented) approach is storing the max versions as a part of the persisted global state
      // in ScatteredConsistentHash, but that works only for the orderly shutdown.
      // TODO: implement start after shutdown
      AtomicInteger maxTopologyId = new AtomicInteger(preloadedTopologyId);
      Publisher<MarshallableEntry<Object, Object>> publisher = persistenceManager.publishEntries(false, true);
      CompletionStage<Void> stage =
            Flowable.fromPublisher(publisher)
                    .doOnNext(me -> {
                       Metadata metadata = me.getMetadata();
                       EntryVersion entryVersion = metadata.version();
                       if (entryVersion instanceof SimpleClusteredVersion) {
                          int entryTopologyId = ((SimpleClusteredVersion) entryVersion).getTopologyId();
                          if (maxTopologyId.get() < entryTopologyId) {
                             maxTopologyId.updateAndGet(current -> Math.max(current, entryTopologyId));
                          }
                       }
                    })
                    .ignoreElements()
                    .toCompletionStage(null);
      CompletionStages.join(stage);
      if (maxTopologyId.get() > 0) {
         clusterTopologyManager.setInitialCacheTopologyId(cache.wired().getName(), maxTopologyId.get() + 1);
      }
   }
}
