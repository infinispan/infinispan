package org.infinispan.lock.logging;

import static org.jboss.logging.Logger.Level.INFO;

import org.infinispan.lock.exception.ClusteredLockException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Range: 29001 - 30000
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   String LOCK_DELETE_MSG = "The lock was deleted.";
   String UNLOCK_FAILED_MSG = "LOCK[%s] Unlock failed from node %s";

   @Message(value = LOCK_DELETE_MSG, id = 29001)
   ClusteredLockException lockDeleted();

//   @Message(value = "The node has left the cluster.", id = 29002)
//   ClusteredLockException nodeShutdown();

   @Message(value = UNLOCK_FAILED_MSG, id = 29003)
   ClusteredLockException unlockFailed(String lockName, Object originator);

   @Message(value = "Missing name for the clustered lock", id = 29004)
   ClusteredLockException missingName();

   @Message(value = "Invalid number of owner. It must be higher than zero or -1 but it was %s", id = 29005)
   ClusteredLockException invalidNumOwners(Integer value);

   @Message(value = "Invalid reliability mode. Modes are AVAILABLE or CONSISTENT", id = 29006)
   ClusteredLockException invalidReliabilityMode();

   @Message(value = "Invalid scope for tag <clustered-lock>. Expected CACHE_CONTAINER but was %s", id = 29007)
   ClusteredLockException invalidScope(String scope);

   @Message(value = "Cannot create clustered locks when clustering is not enabled", id = 29008)
   ClusteredLockException requireClustered();

   @LogMessage(level = INFO)
   @Message(value = "Configuration is not clustered, clustered locks are disabled", id = 29009)
   void configurationNotClustered();

   @Message(value = "MBean registration failed", id = 29010)
   ClusteredLockException jmxRegistrationFailed(@Cause Throwable cause);
}
