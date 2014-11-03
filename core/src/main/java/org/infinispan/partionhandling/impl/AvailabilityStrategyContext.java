package org.infinispan.partionhandling.impl;

import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

import java.util.List;


/**
 * Contains information about the current state of the cache.
 *
 * Also allows {@link AvailabilityStrategy} to proceed with a rebalance, a membership update, or an availability mode change.
 *
 * @author Mircea Markus
 * @author Dan Berindei
 */
public interface AvailabilityStrategyContext {
   String getCacheName();

   /**
    * @return The current cache topology.
    */
   CacheTopology getCurrentTopology();

   /**
    * Whenever a new cache topology without a {@code pendingCH} and with at least {@code numOwners} owners for each
    * segment is installed, and the cache is {@link AvailabilityMode#AVAILABLE}, the current cache topology is marked
    * as the stable topology.
    *
    * The same happens when a rebalance is scheduled to start, but it doesn't do anything because the current
    * topology is already balanced.
    *
    * @return The last stable cache topology. May be {@code null}.
    */
   CacheTopology getStableTopology();

   /**
    * @return The current availability mode.
    */
   AvailabilityMode getAvailabilityMode();

   /**
    * The members of the cache.
    *
    * Includes nodes which have tried to join the cache but are not yet part of the current {@code CacheTopology}.
    * Does not include nodes which have left the cluster (either gracefully or abruptly) but are still in the
    * current topology.
    */
   List<Address> getExpectedMembers();


   /**
    * Queue (or start) a rebalance.
    *
    * Use the configured {@link ConsistentHashFactory} to create a new balanced consistent hash
    * with the given members.
    *
    * If there is no rebalance in progress, start a rebalance right away.
    * If there is a rebalance in progress, queue another rebalance.
    * If there is a rebalance in the queue as well, it will be replaced with the new one.
    * If {@code newConsistentHash == null}, remove any queued rebalance.
    */
   void queueRebalance(List<Address> newMembers);

   /**
    * Use the configured {@link ConsistentHashFactory} to create a new CH
    * with the given {@code members}, but do not start a rebalance.
    * Members missing from the current topology are ignored.
    */
   void updateCurrentTopology(List<Address> newMembers);

   /**
    * Enter a new availability mode.
    */
   void updateAvailabilityMode(AvailabilityMode mode, boolean cancelRebalance);

   /**
    * Updates both the stable and the current topologies.
    *
    * Does not install the current topology on the cache members.
    */
   void updateTopologiesAfterMerge(CacheTopology currentTopology, CacheTopology stableTopology, AvailabilityMode availabilityMode);
}
