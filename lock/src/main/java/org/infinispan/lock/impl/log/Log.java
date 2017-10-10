package org.infinispan.lock.impl.log;

import org.infinispan.lock.exception.ClusteredLockException;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Range: 29001 - 30000
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {
   String LOCK_DELETE_MSG = "The lock was deleted.";

   @Message(value = LOCK_DELETE_MSG, id = 29001)
   ClusteredLockException lockDeleted();
}
