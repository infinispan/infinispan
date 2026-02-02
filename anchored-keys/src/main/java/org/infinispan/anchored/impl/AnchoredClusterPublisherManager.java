package org.infinispan.anchored.impl;

import java.util.Map;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManagerImpl;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.remoting.transport.Address;
import org.reactivestreams.Publisher;

/**
 * ClusterPublisherManager for anchored keys that replaces entry values with node addresses during state transfer.
 *
 * @author William Burns
 * @since 16.2
 */
@Scope(Scopes.NAMED_CACHE)
public class AnchoredClusterPublisherManager<K, V> extends ClusterPublisherManagerImpl<K, V> {

   @Inject ComponentRegistry componentRegistry;

   @Override
   public Publisher<SegmentPublisherSupplier.Notification<CacheEntry<K, V>>> entryPublisherForTopology(int topologyId,
                                                                                                        int batchSize,
                                                                                                        Map<Address, IntSet> targets) {
      // Create and wire the transformation function that replaces values with location metadata
      ReplaceWithLocationFunction<K, V> transformer = new ReplaceWithLocationFunction<>();
      componentRegistry.wireDependencies(transformer);

      // Use the protected method with our wired transformation function
      return super.entryPublisherForTopology(topologyId, batchSize, targets, transformer);
   }
}
