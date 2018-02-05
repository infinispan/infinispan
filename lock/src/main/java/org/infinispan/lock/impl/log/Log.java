package org.infinispan.lock.impl.log;

import org.infinispan.lock.exception.ClusteredLockException;
import org.jboss.logging.BasicLogger;
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
   String NODE_LEFT_MSG = "The node has left the cluster.";
   String UNLOCK_FAILED_MSG = "LOCK[%s] Unlock failed from node %s";

   @Message(value = LOCK_DELETE_MSG, id = 29001)
   ClusteredLockException lockDeleted();

   @Message(value = NODE_LEFT_MSG, id = 29002)
   ClusteredLockException nodeShutdown();

   @Message(value = UNLOCK_FAILED_MSG, id = 29003)
   ClusteredLockException unlockFailed(String lockName, Object originator);

}
