package org.infinispan.partitionhandling.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;


/**
 * Contains information about the current state of the cache.
 *
 * Also allows {@link AvailabilityStrategy} to proceed with a rebalance, a membership update, or an availability mode change.
 *
 * Implementations should not use blocking calls.
 *
 * @author Mircea Markus
 * @author Dan Berindei
 */
public interface AvailabilityStrategyContext {
   String getCacheName();

   CacheJoinInfo getJoinInfo();

   Map<Address, Float> getCapacityFactors();

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
   void updateAvailabilityMode(List<Address> actualMembers, AvailabilityMode mode, boolean cancelRebalance);

   /**
    * Enter a new availability mode manually.
    */
   void manuallyUpdateAvailabilityMode(List<Address> actualMembers, AvailabilityMode mode, boolean cancelRebalance);

   /**
    * Updates both the stable and the current topologies.
    *
    * Does not install the current topology on the cache members.
    */
   void updateTopologiesAfterMerge(CacheTopology currentTopology, CacheTopology stableTopology,
         AvailabilityMode availabilityMode);

   /**
    * @return true if {@link PartitionHandlingConfiguration#mergePolicy()} != null
    */
   boolean resolveConflictsOnMerge();

   /**
    * @param preferredHash the base consistent hash
    * @param distinctHashes a set of all hashes to be utilised as part of the conflict resolution hash
    * @param actualMembers a set of all valid addresses
    * @return the hash to be utilised as a pending CH during Phase.CONFLICT_RESOLUTION
    */
   ConsistentHash calculateConflictHash(ConsistentHash preferredHash, Set<ConsistentHash> distinctHashes, List<Address> actualMembers);

   /**
    * Initiates conflict resolution using the conflictTopology, which should have already been broadcast via
    * {@link AvailabilityStrategyContext#updateTopologiesAfterMerge(CacheTopology, CacheTopology, AvailabilityMode)}
    *
    * @param conflictTopology the topology to use during conflict resolution
    * @param preferredNodes the addresses that belong to the preferred partition as determined by the {@link AvailabilityStrategy}
    */
   void queueConflictResolution(CacheTopology conflictTopology, Set<Address> preferredNodes);

   /**
    * If CR is in progress, then this method cancels the current CR and starts a new CR phase with an updated topology
    * based upon newMembers and the previously queued CR topology
    *
    * @param newMembers the latest members of the current view
    * @return true if conflict resolution was restarted due to the newMembers
    */
   boolean restartConflictResolution(List<Address> newMembers);

   /**
    * @return true if manually set to {@link AvailabilityMode#DEGRADED_MODE}, and false, otherwise.
    */
   boolean isManuallyDegraded();
}
