package org.infinispan.notifications.cachelistener.cluster.impl;

import java.util.Collection;
import java.util.UUID;

import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.remoting.transport.Address;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@SurvivesRestarts
public class ClusterEventManagerStub<K, V> implements ClusterEventManager<K, V> {
   @Override
   public void addEvents(Address target, UUID identifier, Collection<ClusterEvent<K, V>> clusterEvents, boolean sync) {
   }

   @Override
   public void sendEvents() throws CacheException {
   }

   @Override
   public void dropEvents() {
   }
}
