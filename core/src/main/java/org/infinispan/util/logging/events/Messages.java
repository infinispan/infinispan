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

   @Message(value = "Node %s joined the cluster", id = 100000)
   String nodeJoined(Address joiner);

   @Message(value = "Node %s left the cluster", id = 100001)
   String nodeLeft(Address leaver);

   @Message(value = "Started local rebalance", id = 100002)
   String rebalanceStarted();

   @Message(value = "Finished local rebalance phase", id = 100003)
   String rebalancePhaseConfirmed();

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

   @Message(value = "[Context=%s]")
   String eventLogContext(String ctx);

   @Message(value = "[User=%s]")
   String eventLogWho(String who);

   @Message(value = "[Scope=%s]")
   String eventLogScope(String scope);

   @Message(value = "After merge (or coordinator change), the coordinator failed to recover cluster. Cluster members are %s.", id = 100004)
   String clusterRecoveryFailed(Collection<Address> members);

   @Message(value = "Site '%s' is online.", id = 100005)
   String siteOnline(String siteName);

   @Message(value = "Site '%s' is offline.", id = 100006)
   String siteOffline(String siteName);
}
