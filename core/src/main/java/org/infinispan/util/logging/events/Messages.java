package org.infinispan.util.logging.events;

import static org.jboss.logging.Messages.getBundle;

import java.util.Collection;

import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MESSAGES = getBundle(Messages.class);

   @Message(value = "[Context=%s]")
   String eventLogContext(String ctx);

   @Message(value = "[User=%s]")
   String eventLogWho(String who);

   @Message(value = "[Scope=%s]")
   String eventLogScope(String scope);

   @Message(value = "Node %s joined the cluster", id = 100000)
   String nodeJoined(Address joiner);

   @Message(value = "Node %s left the cluster", id = 100001)
   String nodeLeft(Address leaver);

   @Message(value = "Starting rebalance with members %s, phase %s, topology id %d", id = 100002)
   String cacheRebalanceStart(Collection<Address> members, CacheTopology.Phase phase, int topologyId);

//   @Message(value = "Node %s finished rebalance phase %s with topology id %d", id = 100003)
//   String rebalancePhaseConfirmedOnNode(Address node, CacheTopology.Phase phase, int topologyId);

   @Message(value = "Lost data because of graceful leaver %s", id = 312)
   String lostDataBecauseOfGracefulLeaver(Address leaver);

   @Message(value = "Lost data because of abrupt leavers %s", id = 313)
   String lostDataBecauseOfAbruptLeavers(Collection<Address> leavers);

   @Message(value = "Lost at least half of the stable members, possible split brain causing data inconsistency. Current members are %s, lost members are %s, stable members are %s", id = 314)
   String minorityPartition(Collection<Address> currentMembers, Collection<Address> lostMembers, Collection<Address> stableMembers);

   @Message(value = "Unexpected availability mode %s, partition %s", id = 315)
   String unexpectedAvailabilityMode(AvailabilityMode availabilityMode, CacheTopology cacheTopology);

   @Message(value = "Lost data because of graceful leaver %s, entering degraded mode", id = 316)
   String enteringDegradedModeGracefulLeaver(Address leaver);

   @Message(value = "Lost data because of abrupt leavers %s, assuming a network split and entering degraded mode", id = 317)
   String enteringDegradedModeLostData(Collection<Address> leavers);

   @Message(value = "Lost at least half of the stable members, assuming a network split and entering degraded mode. Current members are %s, lost members are %s, stable members are %s", id = 318)
   String enteringDegradedModeMinorityPartition(Collection<Address> currentMembers, Collection<Address> lostMembers, Collection<Address> stableMembers);

   @Message(value = "After merge (or coordinator change), cache still hasn't recovered all its data and must stay in degraded mode. Current members are %s, lost members are %s, stable members are %s", id = 319)
   String keepingDegradedModeAfterMergeDataLost(Collection<Address> currentMembers, Collection<Address> lostMembers, Collection<Address> stableMembers);

   @Message(value = "After merge (or coordinator change), cache still hasn't recovered a majority of members and must stay in degraded mode. Current members are %s, lost members are %s, stable members are %s", id = 320)
   String keepingDegradedModeAfterMergeMinorityPartition(Collection<Address> currentMembers, Collection<Address> lostMembers, Collection<Address> stableMembers);

   @Message(value = "After merge (or coordinator change), the coordinator failed to recover cluster. Cluster members are %s.", id = 100004)
   String clusterRecoveryFailed(Collection<Address> members);

   @Message(value = "Site '%s' is online.", id = 100005)
   String siteOnline(String siteName);

   @Message(value = "Site '%s' is offline.", id = 100006)
   String siteOffline(String siteName);

   @Message(value = "After merge (or coordinator change), recovered members %s with topology id %d", id = 100007)
   String cacheRecoveredAfterMerge(Collection<Address> members, int topologyId);

   @Message(value = "Updating cache members list %s, topology id %d", id = 100008)
   String cacheMembersUpdated(Collection<Address> members, int topologyId);

   @Message(value = "Advancing to rebalance phase %s, topology id %d", id = 100009)
   String cacheRebalancePhaseChange(CacheTopology.Phase phase, int topologyId);

   @Message(value = "Finished rebalance with members %s, topology id %s", id = 100010)
   String rebalanceFinished(Collection<Address> members, int topologyId);

   @Message(value = "Entering availability mode %s, topology id %s", id = 100011)
   String cacheAvailabilityModeChange(AvailabilityMode availabilityMode, int topologyId);

   @Message(value = "Starting conflict resolution with members %s, topology id %d", id = 100012)
   String conflictResolutionStarting(Collection<Address> members, int topologyId);

   @Message(value = "Finished conflict resolution with members %s, topology id %d", id = 100013)
   String conflictResolutionFinished(Collection<Address> members, int topologyId);

   @Message(value = "Failed conflict resolution with members %s, topology id %d: %s", id = 100014)
   String conflictResolutionFailed(Collection<Address> members, int topologyId, String errorMessage);

   @Message(value = "Cancelled conflict resolution with members %s, topology id %s", id = 100015)
   String conflictResolutionCancelled(Collection<Address> members, int topologyId);
}
