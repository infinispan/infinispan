package org.infinispan.transaction.totalOrder;

import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.transaction.xa.CacheTransaction;

import java.util.Set;

/**
 * The interface needed for Remote Transaction in Total Order protocol. This manage the state of a transaction, using the
 * {@link TotalOrderState}
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface TotalOrderRemoteTransaction extends CacheTransaction {

   /**
    * check if the transaction is marked for rollback (by the Rollback Command)
    * @return true if it is marked for rollback, false otherwise
    */
   boolean isMarkedForRollback();

   /**
    * check if the transaction is marked for commit (by the Commit Command)
    * @return true if it is marked for commit, false otherwise
    */
   boolean isMarkedForCommit();

   /**
    * mark the transaction as prepared (the validation was finished) and notify a possible pending commit or rollback
    * command
    */
   void markPreparedAndNotify();

   /**
    * mark the transaction as preparing, blocking the commit and rollback commands until the
    * {@link #markPreparedAndNotify()} is invoked
    */
   void markForPreparing();

   /**
    * Commit and rollback commands invokes this method and they are blocked here if the state is preparing
    *
    * @param commit true if it is a commit command, false otherwise
    * @param newVersions the new versions in commit command (null for rollback)
    * @return true if the command needs to be processed, false otherwise
    * @throws InterruptedException when it is interrupted while waiting
    */
   boolean waitPrepared(boolean commit, EntryVersionsMap newVersions) throws InterruptedException;

   /**
    * Gets a set of the modified keys of this transaction
    * @return a set of keys
    */
   Set<Object> getModifiedKeys();

   /**
    * Gets the latch associated to this remote transaction
    * @return the latch associated to this transaction
    */
   TxDependencyLatch getLatch();
}
