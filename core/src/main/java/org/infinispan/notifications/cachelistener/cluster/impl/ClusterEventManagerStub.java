package org.infinispan.notifications.cachelistener.cluster.impl;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public class ClusterEventManagerStub<K, V> implements ClusterEventManager<K, V> {
   @Override
   public void addEvents(Object batchIdentifier, Address target, UUID identifier, Collection<ClusterEvent<K, V>> clusterEvents, boolean sync) {

   }

   @Override
   public CompletionStage<Void> sendEvents(Object batchIdentifier) {
      return CompletableFutures.completedNull();
   }

   @Override
   public void dropEvents(Object batchIdentifier) {

   }
}
