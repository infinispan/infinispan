package org.infinispan.notifications.cachelistener.cluster.impl;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@SurvivesRestarts
public class ClusterEventManagerStub<K, V> implements ClusterEventManager<K, V> {
   @Override
   public void addEvents(Address target, UUID identifier, Collection<ClusterEvent<K, V>> clusterEvents, boolean sync) {
   }

   @Override
   public CompletionStage<Void> sendEvents() throws CacheException {
      return CompletableFutures.completedNull();
   }

   @Override
   public void dropEvents() {
   }
}
