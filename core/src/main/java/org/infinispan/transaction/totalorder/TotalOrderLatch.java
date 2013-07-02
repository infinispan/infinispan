package org.infinispan.transaction.totalorder;

/**
 * Behaves as a latch between {@code org.infinispan.commands.tx.PrepareCommand} delivered in total order to coordinate
 * conflicting transactions and between {@code org.infinispan.commands.tx.PrepareCommand} and state transfer (blocking
 * the prepare until the state transfer is finished and blocking the state transfer until all the prepared transactions
 * has finished)
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public interface TotalOrderLatch {

   /**
    * @return true if this synchronization block is blocked
    */
   boolean isBlocked();

   /**
    * Unblocks this synchronization block
    */
   void unBlock();

   /**
    * It waits for this synchronization block to be unblocked.
    *
    * @throws InterruptedException if interrupted while waiting.
    */
   void awaitUntilUnBlock() throws InterruptedException;

}
